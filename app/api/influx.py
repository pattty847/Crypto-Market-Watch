import os
from influxdb_client import InfluxDBClient
from datetime import datetime
from influxdb_client.client.write_api import SYNCHRONOUS


class InfluxDB:
    def __init__(self) -> None:
        self.client = self.get_influxdb_client()
        self.write_api = self.client.write_api(write_options=SYNCHRONOUS)
        self.query_api = self.client.query_api()
        
    def get_influxdb_client(self):
        return InfluxDBClient(
            url="http://localhost:8086",
            token=os.environ.get('INFLUXDB_TOKEN'),
            org="pepe"
        )