from api.services import get_influxdb_client
from datetime import datetime
from influxdb_client.client.write_api import SYNCHRONOUS
from api.services import get_influxdb_client


class InfluxDB:
    def __init__(self, trades_bucket) -> None:
        self.client = get_influxdb_client()
        self.bucket = trades_bucket
        self.write_api = self.client.write_api(write_options=SYNCHRONOUS)
        self.query_api = self.client.query_api()