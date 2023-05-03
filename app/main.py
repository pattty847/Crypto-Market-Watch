import asyncio
import logging
from api.ccxt_manager import CCXTManager


symbols = ['BTC/USD', 'BTC/USDT', 'ETH/USD', 'ETH/USDT']
exchanges = ['coinbasepro', 'bitfinex', 'kraken', 'bitstamp', 'kucoin', 'binanceus', 'cryptocom']
manager = CCXTManager(exchanges)
try:
    asyncio.run(manager.watch_exchanges(symbols))
except KeyboardInterrupt:
    logging.info("Program stopped by user. All tasks cancelled and exchanges closed.")