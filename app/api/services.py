import os
from influxdb_client import InfluxDBClient
from influxdb_client.client.write_api import SYNCHRONOUS


def get_influxdb_client():
    return InfluxDBClient(
        url="http://localhost:8086",
        token=os.environ.get('INFLUXDB'),
        org="pepe"
    )
    
    
# Testing purposes
# ----------------------------------------------------------
# client = get_influxdb_client()
# bucket = 'trades'
# write_api = client.write_api(write_options=SYNCHRONOUS)
# query_api = client.query_api()

# query = f'''
# from(bucket: "{bucket}")
#     |> range(start: -1000y)
#     |> filter(fn: (r) => r["_measurement"] == "trades")
# '''

# result = query_api.query(org="pepe", query=query)

# for table in result:
#     for record in table.records:
#         print(record.values)