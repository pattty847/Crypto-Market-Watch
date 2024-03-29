import asyncio
import json
from typing import List
import ccxt
import ccxt.pro as ccxtpro
import logging

from .influx import InfluxDB

class CCXTInterface:
    """
    This class manages connections to CCXT exchanges.
    """
    def __init__(self, local_database, exchanges):
        self.exchanges = exchanges
        self.exchange_list = None
        self.influx = InfluxDB(local_database)
        with open('config.json', 'r') as f:
            self.config = json.load(f)

    async def load_exchanges(self):
        supported_exchanges = {}
        for exchange_id in self.exchanges:
            try:
                exchange_class = getattr(ccxtpro, exchange_id)({
                    'apiKey': self.config[exchange_id]['KEY'],
                    'secret': self.config[exchange_id]['SECRET'],
                    'password': self.config[exchange_id]['PASS']
                } if exchange_id == 'kucoinfutures' else {})

                await exchange_class.load_markets()
                if exchange_class.has['watchTrades'] and exchange_class.has['fetchOHLCV']:
                    supported_exchanges[exchange_id] = {
                        "ccxt":exchange_class,
                        "symbols": list(exchange_class.markets),
                        "timeframes": list(exchange_class.timeframes.keys())
                    }
            except Exception as e:
                logging.error(f"Error creating exchange object for {exchange_id}: {e}")
        self.exchange_list = supported_exchanges

    async def __aenter__(self):
        await self.load_exchanges()
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        await self.close_all_exchanges()

    async def close_all_exchanges(self):
        async def close_exchange(exchange_id):
            exchange = self.exchange_list[exchange_id]["ccxt"]
            try:
                await exchange.close()
                logging.info(f"{exchange_id} closed successfully.")
            except Exception as e:
                logging.error(f"Error closing {exchange_id}: {e}")

        tasks = [close_exchange(exchange_id) for exchange_id in self.exchange_list.keys()]
        await asyncio.gather(*tasks)
