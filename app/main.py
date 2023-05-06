import asyncio
import logging
from api.ccxt_manager import CCXTManager


# symbols = ['BTC/USD', 'BTC/USDT', 'ETH/USD', 'ETH/USDT']
# exchanges = ['coinbasepro', 'bitfinex', 'kraken', 'bitstamp', 'kucoin', 'binanceus']

symbols = ['BTC/USD']
exchanges = ['coinbasepro', 'kucoin']

manager = CCXTManager(exchanges)
try:
    # asyncio.run(manager.watch_exchanges(symbols))
    print(asyncio.run(manager.fetch_all_candles(symbols=symbols, timeframe='5m', since=None, limit=1000, return_dataframe=True, resample_timeframe='4h')))
except KeyboardInterrupt:
    logging.info("Program stopped by user. All tasks cancelled and exchanges closed.")
    asyncio.run(manager.close_all_exchanges())
finally:
    asyncio.run(manager.close_all_exchanges())