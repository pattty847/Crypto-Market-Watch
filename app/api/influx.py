import json
from typing import Dict
from influxdb_client import InfluxDBClient, Point, WritePrecision
from influxdb_client.client.write_api import SYNCHRONOUS
import pandas as pd
import logging

class InfluxDB:
    def __init__(self, local) -> None:
        # Create a config.json file and store your INFLUX token as a key value pair
        with open('config.json', 'r') as f:
            self.config = json.load(f)
        self.client = self.get_influxdb_client(local)
        self.write_api = self.client.write_api(write_options=SYNCHRONOUS)
        self.query_api = self.client.query_api()
        self.delete_api = self.client.delete_api()
        
    def get_influxdb_client(self, local=False):
        return InfluxDBClient(
            url="http://localhost:8086" if local else "https://us-east-1-1.aws.cloud2.influxdata.com",
            token=self.config['INFLUXDB_TOKEN_LOCAL'] if local else self.config['INFLUXDB'],
            org="pepe"
        )
        
    def write_candles_to_influxdb(
        self,
        exchange,
        symbol: str,
        timeframe: str,
        candles: pd.DataFrame,
        bucket: str = "candles",
    ) -> None:
        if candles.empty:
            logging.warning(f"Skipping write to InfluxDB for {exchange} {symbol} {timeframe} as the DataFrame is empty.")
            return
        
        symbol = symbol.replace("/", "_")
        points = []

        for record in candles.to_records():
            point = Point("candle") \
                .tag("exchange", exchange) \
                .tag("symbol", symbol) \
                .tag("timeframe", timeframe) \
                .field("opens", record.opens) \
                .field("highs", record.highs) \
                .field("lows", record.lows) \
                .field("closes", record.closes) \
                .field("volumes", record.volumes) \
                .time(record.dates, WritePrecision.MS)

            points.append(point)
            
        logging.info(f"Writing {len(candles['dates'])} candles to bucket: {bucket}, organization: 'pepe'")
        
        self.write_api.write(bucket, 'pepe', points)

    def read_candles_from_influxdb(
        self, exchange: str, symbol: str, timeframe: str, bucket="candles") -> Dict:

        symbol = symbol.replace("/", "_")
        
        query = f"""
        from(bucket: "{bucket}")
        |> range(start: -30d)
        |> filter(fn: (r) => r["_measurement"] == "candle")
        |> filter(fn: (r) => r["exchange"] == "{exchange}")
        |> filter(fn: (r) => r["symbol"] == "{symbol}")
        |> filter(fn: (r) => r["timeframe"] == "{timeframe}")
        |> filter(fn: (r) => r["_field"] == "closes" or r["_field"] == "highs" or r["_field"] == "lows" or r["_field"] == "opens" or r["_field"] == "volumes")
        |> pivot(rowKey:["_time"], columnKey: ["_field"], valueColumn: "_value")
        |> drop(columns: ["_start", "_stop"])
        """

        logging.info(f"Fetching from bucket: {bucket}, organization: 'pepe', {exchange}, {symbol}, {timeframe}:")
        result = self.query_api.query_data_frame(query, 'pepe')

        if result.empty:
            return pd.DataFrame(columns=["dates", "opens", "highs", "lows", "closes", "volumes"])
        else:
            result = result.rename(columns={"_time": "dates"})
            result = result.reindex(columns=["dates", "opens", "highs", "lows", "closes", "volumes"])
            return result