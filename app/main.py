import asyncio
import logging
from api.ccxt_manager import CCXTManager


# symbols = ['BTC/USD', 'BTC/USDT', 'ETH/USD', 'ETH/USDT']
# exchanges = ['coinbasepro', 'bitfinex', 'kraken', 'bitstamp', 'kucoin', 'binanceus']

symbols = ['ETH/USDT']
exchanges = ['binanceus']

manager = CCXTManager(exchanges)
try:
    # asyncio.run(manager.watch_exchanges(symbols))
    print(asyncio.run(manager.fetch_all_candles(symbols=symbols, timeframe='1m', since=None, limit=1000, resample_timeframe=None)))
except KeyboardInterrupt:
    logging.info("Program stopped by user. All tasks cancelled and exchanges closed.")
finally:
    asyncio.run(manager.close_all_exchanges())