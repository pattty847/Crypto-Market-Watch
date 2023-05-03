from collections import defaultdict
from api.services import get_influxdb_client
from datetime import datetime
from influxdb_client.client.write_api import SYNCHRONOUS
from tabulate import tabulate
from api.influx import InfluxDB

class MarketAggregator:
    
    def __init__(self, trades_bucket="trades"):
        self.data = defaultdict(lambda: defaultdict(lambda: defaultdict(float)))
        self.order_size_categories = ['0-10k', '10k-100k', '100k-1m', '1m-10m', '10m-100m']
        
        self.influx = InfluxDB(trades_bucket)


    def calculate_stats(self, exchange, symbol, trade, write_to_db=False):
        order_cost = float(trade["price"]) * float(trade["amount"])
        order_size_category = self.get_order_size_category(order_cost)

        # Total volume for an exchange and symbol pair
        self.data[(exchange, symbol)]['volume']['total'] += float(trade["amount"])

        # CVD for an exchange and symbol pair
        self.data[(exchange, symbol)]['CVD']['total'] += float(trade["amount"]) if trade["side"] == "buy" else -float(trade["amount"])

        # CVD and Volume for an order size separated into categories based on size for an exchange and symbol pair
        self.data[(exchange, symbol)]['CVD'][order_size_category] += float(trade["amount"]) if trade["side"] == "buy" else -float(trade["amount"])
        self.data[(exchange, symbol)]['volume'][order_size_category] += float(trade["amount"])

        if write_to_db:
            self.write_to_db(exchange, symbol, trade, order_cost, order_size_category)


    def get_order_size_category(self, order_cost):
        if order_cost < 1e4:
            return '0-10k'
        elif order_cost < 1e5:
            return '10k-100k'
        elif order_cost < 1e6:
            return '100k-1m'
        elif order_cost < 1e7:
            return '1m-10m'
        elif order_cost < 1e8:
            return '10m-100m'


    def report(self):
        header = ['Exchange/Symbol', 'Volume', 'CVD', '0-10k', '0-10kΔ', '10k-100k', '10k-100kΔ', '100k-1m', '100k-1mΔ', '1m-10m', '1m-10mΔ', '10m-100m', '10m-100mΔ']

        rows = []
        for (exchange, symbol), values in self.data.items():
            volume = values['volume']['total']
            cvd = values['CVD']['total']
            row = [
                f"{exchange}/{symbol}",
                f"{volume:.4f}",
                f"{cvd:.4f}",
                f"{values['volume']['0-10k']:.4f}",
                f"{values['CVD']['0-10k']:.4f}",
                f"{values['volume']['10k-100k']:.4f}",
                f"{values['CVD']['10k-100k']:.4f}",
                f"{values['volume']['100k-1m']:.4f}",
                f"{values['CVD']['100k-1m']:.4f}",
                f"{values['volume']['1m-10m']:.4f}",
                f"{values['CVD']['1m-10m']:.4f}",
                f"{values['volume']['10m-100m']:.4f}",
                f"{values['CVD']['10m-100m']:.4f}",
            ]
            rows.append(row)

        print(tabulate(rows, headers=header, tablefmt='grid'))
        
        
    def write_to_db(self, exchange, symbol, trade, order_cost, order_size_category):
        point = {
            "measurement": "trades",
            "tags": {
                "exchange": exchange,
                "symbol": symbol,
                "side": trade["side"],
                "order_size_category": order_size_category
            },
            "fields": {
                "price": float(trade["price"]),
                "amount": float(trade["amount"]),
                "cost": order_cost
            },
            "time": datetime.fromtimestamp(trade["timestamp"] / 1000).strftime('%Y-%m-%dT%H:%M:%SZ')
        }
    
        try:
            print(point)
            self.influx.write_api.write(bucket=self.influx.bucket, org='pepe', record=point)
        except Exception as e:
            print(e)