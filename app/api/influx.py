import json
import os
from influxdb_client import InfluxDBClient
from datetime import datetime
from influxdb_client.client.write_api import SYNCHRONOUS


class InfluxDB:
    def __init__(self) -> None:
        with open('config.json', 'r') as f:
            self.config = json.load(f)
        
    def get_influxdb_client(self):
        return InfluxDBClient(
            url="http://localhost:8086",
            token=self.config['INFLUXDB_TOKEN'],
            org="pepe"
        )