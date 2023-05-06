import json
import os
import ccxt
import ccxt.pro as ccxtpro
import logging
import asyncio
import pandas as pd
from datetime import datetime, timedelta
from api.influx import InfluxDB
from influxdb_client import InfluxDBClient, Point, WritePrecision
from influxdb_client.client.write_api import SYNCHRONOUS
from api.ta import TechnicalAnalysis
from typing import Dict, List, Tuple
from asyncio import Queue
from api.market_aggregator import MarketAggregator


logging.basicConfig(level=logging.INFO)

class CCXTManager:
    """
    This class manages connections to CCXT exchanges.
    """
    def __init__(self, exchanges) -> None:
        self.watched_exchanges = exchanges
        self.exchanges = self.get_exchanges()
        self.influx = InfluxDB()
        self.agg = MarketAggregator(self.influx)
        self.ta = TechnicalAnalysis()
        self.trade_queue = Queue()
        
    def get_exchanges(self):
        supported_exchanges = {}
        for exchange_id in self.watched_exchanges:
            try:
                exchange_class = getattr(ccxtpro, exchange_id)({'newUpdates': False})
                if exchange_class.has['watchTrades']:
                    supported_exchanges[exchange_id] = exchange_class
            except Exception as e:
                logging.error(f"Error creating exchange object for {exchange_id}: {e}")
        return supported_exchanges
    
    async def close_all_exchanges(self):
        async def close_exchange(exchange_id):
            exchange = self.exchanges[exchange_id]
            try:
                await exchange.close()
                logging.info(f"{exchange_id} closed successfully.")
            except Exception as e:
                logging.error(f"Error closing {exchange_id}: {e}")

        tasks = [close_exchange(exchange_id) for exchange_id in self.exchanges.keys()]
        await asyncio.gather(*tasks)

    async def watch_exchanges(self, symbols):
        async def watch_exchange(exchange_id, symbols):
            exchange = self.exchanges[exchange_id]
            try:
                while True:
                    for symbol in symbols:
                        trades = await exchange.watch_trades(symbol)
                        await self.trade_queue.put((exchange_id, trades))
            except (Exception, KeyboardInterrupt) as e:
                logging.info(f"Error watching trades on {exchange_id}: {e}")
            finally:
                try:
                    await exchange.close()
                    logging.info(f"{exchange_id} closed successfully.")
                except Exception as e:
                    logging.info(f"Error closing {exchange_id}: {e}")

        async def process_trades():
            while True:
                exchange_id, trades = await self.trade_queue.get()
                self.handle_trades(exchange_id, trades, None)
                self.trade_queue.task_done()

        watch_tasks = [watch_exchange(exchange_id, symbols) for exchange_id in self.exchanges]
        worker_tasks = [process_trades() for _ in range(len(self.exchanges))]
        all_tasks = watch_tasks + worker_tasks

        try:
            await asyncio.gather(*all_tasks)
        except KeyboardInterrupt:
            for t in all_tasks:
                t.cancel()
            await asyncio.gather(*all_tasks, return_exceptions=True)
            
    def handle_trades(self, exchange, trades, orderbook):
        [self.agg.calculate_stats(exchange, trade['symbol'], trade) for trade in trades]
        self.agg.report()
        
    async def fetch_all_candles(
        self,
        symbols: List[str],
        timeframe: str,
        since: str,
        limit: int,
        return_dataframe=True,
        max_retries=3,
        resample_timeframe=None,
    ) -> List[Tuple]:
        tasks = [
            self.fetch_candles(
                exchange_id,
                symbol,
                timeframe,
                since,
                limit,
                return_dataframe,
                max_retries,
                resample_timeframe,
            )
            for exchange_id in self.exchanges
            for symbol in symbols
        ]
        dataframes = await asyncio.gather(*tasks)
        return [df for df in dataframes if df is not None]

    async def fetch_candles(
        self,
        exchange_id: str,
        symbol: str,
        timeframe: str,
        since: str,
        limit: int,
        return_dataframe: bool = True,
        max_retries: int = 3,
        resample_timeframe: str = None,
    ) -> Tuple[str, str, str, pd.DataFrame]:
        exchange = self.exchanges[exchange_id]
        available_timeframes = list(exchange.timeframes.keys())

        loaded_candles = self.load_candles_from_influxdb(exchange_id, symbol, timeframe or resample_timeframe)
        
        print(loaded_candles)

        if resample_timeframe:
            timeframe = self.find_highest_resample_timeframe(resample_timeframe, available_timeframes)
            
        timeframe_duration_in_seconds = exchange.parse_timeframe(timeframe)
        timedelta = limit * timeframe_duration_in_seconds * 1000
        now = exchange.milliseconds()

        fetch_since = (
            exchange.parse8601(since) if not len(loaded_candles["dates"]) else int(loaded_candles["dates"][-1] * 1000)
        )

        new_candles = await self.fetch_new_candles(exchange, symbol, timeframe, fetch_since, limit, max_retries, now, timedelta)

        loaded_candles = self.append_new_candles(loaded_candles, new_candles)
        
        if not loaded_candles['dates']:
            return

        self.save_candles_to_influxdb(exchange_id, symbol, timeframe, loaded_candles)

        await exchange.close()

        df = pd.DataFrame(loaded_candles)

        if resample_timeframe:
            df = self.ta.resample_dataframe(df, timeframe, resample_timeframe)
            timeframe = resample_timeframe

        return exchange_id, symbol, timeframe, df if return_dataframe else loaded_candles

    async def fetch_new_candles(
        self,
        exchange,
        symbol: str,
        timeframe: str,
        fetch_since: int,
        limit: int,
        max_retries: int,
        now: int,
        timedelta: int,
    ) -> List[List]:
        new_candles = []
        done_fetching = False
        while not done_fetching:
            new_candle_batch = await self.retry_request(exchange, symbol, timeframe, fetch_since, limit, max_retries)

            if new_candle_batch is None:
                break

            new_candles += new_candle_batch

            if len(new_candle_batch):
                last_time = new_candle_batch[-1][0] + exchange.parse_timeframe(timeframe) * 1000
            else:
                last_time = fetch_since + timedelta

            if last_time >= now:
                done_fetching = True
            else:
                fetch_since = last_time

        await exchange.close()
        return new_candles

    async def retry_request(
        self,
        exchange,
        symbol: str,
        timeframe: str,
        fetch_since: int,
        limit: int,
        max_retries: int,
    ) -> List:
        for _ in range(max_retries):
            try:
                new_candle_batch = await exchange.fetch_ohlcv(symbol, timeframe, since=fetch_since, limit=limit)
                print(len(new_candle_batch), "candles from", exchange.iso8601(new_candle_batch[0][0]), "to", exchange.iso8601(new_candle_batch[-1][0]))
            except (ccxt.ExchangeNotAvailable, ccxt.ExchangeError, ccxt.RequestTimeout, ccxt.BadSymbol) as e:
                print(f"Error fetching data from {exchange.id} for {symbol}: {e}")
                if isinstance(e, ccxt.BadSymbol):
                    break
                continue
            return new_candle_batch
        return

    def append_new_candles(self, loaded_candles: Dict, new_candles: List) -> Dict:
        if new_candles is None:
            return loaded_candles
        for row in new_candles[1:]:
            loaded_candles["dates"].append(row[0] / 1000)
            loaded_candles["opens"].append(float(row[1]))
            loaded_candles["highs"].append(float(row[2]))
            loaded_candles["lows"].append(float(row[3]))
            loaded_candles["closes"].append(float(row[4]))
            loaded_candles["volumes"].append(float(row[5]))
        return loaded_candles

    def convert_to_minutes(self, timeframe_str: str) -> int:
        units = timeframe_str[-1]
        value = int(timeframe_str[:-1])

        if units == 's':
            return value
        elif units == 'm' or units == 'T':
            return value
        elif units == 'h':
            return value * 60
        elif units == 'd':
            return value * 60 * 24
        elif units == 'w':
            return value * 60 * 24 * 7
        else:
            raise ValueError(f'Unknown timeframe unit: {units}')

    def find_highest_resample_timeframe(self, resample_timeframe, available_timeframes):
        available_timeframes_minutes = [self.convert_to_minutes(tf) for tf in available_timeframes]
        resample_timeframe_minutes = self.convert_to_minutes(resample_timeframe)
        best_timeframe = max((tf for tf in available_timeframes_minutes if resample_timeframe_minutes % tf == 0), default=None)

        if best_timeframe is None:
            raise ValueError(f"No suitable timeframe found for resampling to {resample_timeframe}")

        return [tf for tf in available_timeframes if self.convert_to_minutes(tf) == best_timeframe][0]

    def save_candles_to_influxdb(
        self,
        exchange,
        symbol: str,
        timeframe: str,
        candles: Dict,
        bucket: str = "candles",
    ) -> None:
        if not candles:
            print(f"Skipping write to InfluxDB for {exchange} {symbol} {timeframe} as the DataFrame is empty.")
            return

        influx_client = self.influx.get_influxdb_client()
        write_api = influx_client.write_api(write_options=SYNCHRONOUS)

        symbol = symbol.replace("/", "_")
        points = []

        for i in range(len(candles["dates"])):
            point = Point("candle") \
                .tag("exchange", exchange) \
                .tag("symbol", symbol) \
                .tag("timeframe", timeframe) \
                .field("open", candles["opens"][i]) \
                .field("high", candles["highs"][i]) \
                .field("low", candles["lows"][i]) \
                .field("close", candles["closes"][i]) \
                .field("volume", candles["volumes"][i]) \
                .time(int(candles["dates"][i] * 1e9), WritePrecision.NS)

            points.append(point)
            
        print(f"Writing to bucket: {bucket}, organization: 'pepe'")
        
        write_api.write(bucket, 'pepe', points)
        write_api.close()
        influx_client.close()

    def load_candles_from_influxdb(
        self, exchange: str, symbol: str, timeframe: str, bucket="candles") -> Dict:
        
        influx_client = self.influx.get_influxdb_client()
        query_api = influx_client.query_api()

        symbol = symbol.replace("/", "_")
        
        query = f"""
        from(bucket: "{bucket}")
        |> range(start: -30d)
        |> filter(fn: (r) => r["_measurement"] == "candle")
        |> filter(fn: (r) => r["exchange"] == "{exchange}")
        |> filter(fn: (r) => r["symbol"] == "{symbol}")
        |> filter(fn: (r) => r["timeframe"] == "{timeframe}")
        |> filter(fn: (r) => r["_field"] == "close" or r["_field"] == "high" or r["_field"] == "low" or r["_field"] == "open" or r["_field"] == "volume")
        |> pivot(rowKey:["_time"], columnKey: ["_field"], valueColumn: "_value")
        """

        print(f"Fetching from bucket: {bucket}, organization: 'pepe', {exchange}, {symbol}, {timeframe}:")
        result = query_api.query_data_frame(query, 'pepe')

        influx_client.close()

        if result.empty:
            return {"dates": [], "opens": [], "highs": [], "lows": [], "closes": [], "volumes": []}
        else:
            return {
                "dates": (result["_time"].astype('int64') / 1e9).tolist(),
                "opens": result["open"].tolist(),
                "highs": result["high"].tolist(),
                "lows": result["low"].tolist(),
                "closes": result["close"].tolist(),
                "volumes": result["volume"].tolist()
            }