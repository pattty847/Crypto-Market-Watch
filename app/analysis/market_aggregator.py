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
        self.prev_taker_id = None
        self.prev_timestamp = None
        self.timeframes = ["1m", "5m", "15m", "1h", "4h", "1d"]
            
    async def aggregate_trades(self, exchange_id: str, trades_data: List[dict]):
        # Use Pydantic to parse trades data into a list of Trade objects
        trades = parse_obj_as(List[Trade], trades_data)
        
        print(trades)

        for trade in trades:
            taker_id = trade.info.taker_order_id
            timestamp = trade.timestamp

            # If the taker_id or timestamp has changed, emit the previous combined trade
            if self.prev_taker_id is not None and (taker_id != self.prev_taker_id or timestamp != self.prev_timestamp):
                self.emit_combined_trade(exchange_id, self.prev_taker_id)
                # Reset the combined trade dictionary for the new taker_id
                self.combined_trades = {}

            # Add the new trade to the combined trades
            if taker_id in self.combined_trades:
                self.combined_trades[taker_id]['total_amount'] += trade.amount
                self.combined_trades[taker_id]['total_price'] += trade.price * trade.amount
            else:
                self.combined_trades[taker_id] = {'total_amount': trade.amount, 'total_price': trade.price * trade.amount}

            self.prev_taker_id = taker_id
            self.prev_timestamp = timestamp

        # Emit the last combined trade
        if self.prev_taker_id is not None:
            self.emit_combined_trade(exchange_id, self.prev_taker_id)

    def emit_combined_trade(self, exchange_id, taker_id):
        # Emit the combined trade
        total_amount = self.combined_trades[taker_id]['total_amount']
        total_price = self.combined_trades[taker_id]['total_price']
        average_price = total_price / total_amount
        print(f'Exchange ID: {exchange_id}, Taker ID: {taker_id}, Total amount: {total_amount}, Average price: {average_price}')
                    
    
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