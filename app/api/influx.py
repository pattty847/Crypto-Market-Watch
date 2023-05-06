import json
from influxdb_client import InfluxDBClient
from influxdb_client.client.write_api import SYNCHRONOUS

class InfluxDB:
    def __init__(self) -> None:
        with open('config.json', 'r') as f:
            self.config = json.load(f)
        self.client = self.get_influxdb_client()
        self.write_api = self.client.write_api(write_options=SYNCHRONOUS)
        self.query_api = self.client.query_api()
        self.delete_api = self.client.delete_api()
        
    def get_influxdb_client(self):
        return InfluxDBClient(
            url="http://localhost:8086",
            token=self.config['INFLUXDB_TOKEN'],
            org="pepe"
        )