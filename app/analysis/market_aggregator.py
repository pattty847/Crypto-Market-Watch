import asyncio
from collections import defaultdict
from datetime import datetime
from typing import Dict, List
from influxdb_client import Point, WritePrecision
from influxdb_client.client.write_api import SYNCHRONOUS
import pandas as pd
from pydantic import ValidationError
from tabulate import tabulate
from api.influx import InfluxDB
from collections import defaultdict
from typing import List
from pydantic import parse_obj_as
from api.trade_model import Trade

class MarketAggregator:
    
    def __init__(self):
        self.data = defaultdict(lambda: defaultdict(lambda: defaultdict(float)))
        self.order_size_categories = ['0-10k', '10k-100k', '100k-1m', '1m-10m', '10m-100m']
        self.combined_trades = {}
        self.aggregated_trades = {}
        self.last_taker_order_id = None
        self.timeframes = ["1m", "5m", "15m", "1h", "4h", "1d"]
            
    async def aggregate_trades(self, exchange_id, trades):
        for trade in trades:
            trade_info = trade['info']
            taker_order_id = trade_info['takerOrderId']
            if taker_order_id not in self.aggregated_trades:
                if self.last_taker_order_id is not None:
                    await self.emit_trade(self.last_taker_order_id)
                self.last_taker_order_id = taker_order_id
                self.aggregated_trades[taker_order_id] = {
                    'exchange_id': exchange_id,
                    'takerOrderId': taker_order_id,
                    'symbol': trade['symbol'],
                    'side': trade_info['side'],
                    'datetime': trade['datetime'],
                    'quantity': 0,
                    'total_value': 0,
                    'total_cost': 0
                }
            self.aggregated_trades[taker_order_id]['quantity'] += trade_info['size']
            self.aggregated_trades[taker_order_id]['total_value'] += trade_info['size'] * trade_info['price']
            self.aggregated_trades[taker_order_id]['total_cost'] += trade_info['size'] * trade_info['price'] * 0.1

    async def emit_trade(self, taker_order_id):
        agg_trade = self.aggregated_trades[taker_order_id]
        agg_trade['price'] = agg_trade['total_value'] / agg_trade['quantity'] if agg_trade['quantity'] > 0 else 0
        agg_trade['cost'] = agg_trade['total_cost'] / (agg_trade['quantity'] * 0.1) if agg_trade['quantity'] > 0 else 0
        del agg_trade['total_value']
        del agg_trade['total_cost']
        print(f"Emitting aggregated trade: {agg_trade}")  # Replace with your emitting code
        del self.aggregated_trades[taker_order_id]


    def calculate_stats(self, exchange_id, trade):
        # Calculate stats here
        pass

    def report_statistics(self):
        # Report stats here
        pass          
    
    def calculate_stats(self, exchange: str, trades: List[str], write_to_db=False) -> None:

        try:
            symbol = trades['symbol']
            # Check if necessary fields are in the trade data
            if not all(key in trades for key in ("price", "amount", "side")):
                print(f"Trade data is missing necessary fields: {trades}")
                return

            # Convert amount to float once and store the result
            try:
                amount = float(trades["amount"]) # base currency
            except ValueError:
                print(f"Amount is not a number: {trades['amount']}")
                return

            # Check if side is either "buy" or "sell"
            if trades["side"] not in ("buy", "sell"):
                print(f"Invalid trade side: {trades['side']}")
                return

            order_cost = float(trades["price"]) * amount # quote currency
            order_size_category = self.get_order_size_category(order_cost)

            # Total volume for an exchange and symbol pair
            self.data[(exchange, symbol)]['volume']['total'] += amount

            # CVD for an exchange and symbol pair
            self.data[(exchange, symbol)]['CVD']['total'] += amount if trades["side"] == "buy" else -amount

            # CVD and Volume for an order size separated into categories based on size for an exchange and symbol pair
            self.data[(exchange, symbol)]['CVD'][order_size_category] += amount if trades["side"] == "buy" else -amount
            self.data[(exchange, symbol)]['volume'][order_size_category] += amount

            if write_to_db:
                self.write_to_db(exchange, symbol, trades, order_cost, order_size_category)
        except Exception as e:
            print(f"Error processing trade data: {e}")

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

    def report_statistics(self):
        header = ['Exchange/Symbol', 'Volume', 'CVDΔ', '0-10k', '0-10kΔ', '10k-100k', '10k-100kΔ', '100k-1m', '100k-1mΔ', '1m-10m', '1m-10mΔ', '10m-100m', '10m-100mΔ']

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