import asyncio
from api.influx import InfluxDB
from ui.chart import Chart
from ui.main_menu import MainMenu
from api.ccxt_manager import CCXTManager
from ui.viewport import View_Port
from api.server import Server

async def main():
    
    exchanges = ['coinbasepro']
    influx = InfluxDB(local=False)
    # server = Server(influx)
    # server.run()
    
    manager = await CCXTManager.create(exchanges, influx)
    print(manager.calculate_volatility())
    #await manager.watch_trades(['BTC/USD'])
    
    await manager.close_all_exchanges()

    # with View_Port('MarketWatch') as viewport:
    #     exchanges = ['coinbasepro']
    #     influx = InfluxDB(local=False)
    #     # server = Server(influx)
    #     # server.run()
        
    #     manager = await CCXTManager.create(exchanges, influx)
    #     await manager.watch_trades(['BTC/USD'])
    #     # loop.create_task(manager.watch_trades("BTC/USD"))

    #     main_menu = MainMenu(viewport.tag)
    #     chart = await Chart.create(manager, viewport.tag)
        
    #     viewport.run()

    # await manager.close_all_exchanges()

if __name__ == "__main__":
    asyncio.run(main())