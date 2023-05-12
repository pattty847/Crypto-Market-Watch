import asyncio
import pandas as pd
from api.influx import InfluxDB
from ui.notifications import NotificationManager
from ui.chart import Chart
from ui.main_menu import MainMenu
from api.ccxt_manager import CCXTManager
from ui.viewport import View_Port

# symbols = ['BTC/USD', 'BTC/USDT', 'ETH/USD', 'ETH/USDT']
# exchanges = ['coinbasepro', 'bitfinex', 'kraken', 'bitstamp', 'kucoin', 'binanceus']

async def main():
    symbols = ['BTC/USD']
    exchanges = ['coinbasepro', 'bybit']

    with View_Port('MarketWatch') as viewport:
        influx = InfluxDB(local=False)
        loop = asyncio.new_event_loop()
        notifications = NotificationManager()
        manager = await CCXTManager.create(exchanges, loop, influx)
        main_menu = MainMenu(viewport.tag)
        chart = await Chart.create(manager, viewport.tag, notifications)
        
        viewport.run()

        await manager.close_all_exchanges()

if __name__ == "__main__":
    asyncio.run(main())
    



# manager.start_watch_trades(symbols)
# print('Sleeping for 15s')
# time.sleep(5)

# manager.pause_trades('coinbasepro', 'BTC/USD')
# print('Sleeping for 5s')
# time.sleep(5)

# manager.resume_trades('coinbasepro', 'BTC/USD')

# with View_Port('MarketWatch') as viewport:
    
#     main_menu = MainMenu(viewport.tag)
#     chart = Chart(manager, viewport.tag)
    
#     try:
#         # asyncio.run(manager.watch_exchanges(symbols))
#         df = asyncio.run(manager.fetch_all_candles(symbols=symbols, timeframe='1h', since=None, limit=1000, resample_timeframe=None))
#     except KeyboardInterrupt:
#         logging.info("Program stopped by user. All tasks cancelled and exchanges closed.")
#     finally:
#         asyncio.run(manager.close_all_exchanges())
    
#     df = df[0]
#     chart.draw_chart(df[0], df[1], df[2], df[3])
    
#     viewport.run()