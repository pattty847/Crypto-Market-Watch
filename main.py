import asyncio
import logging
import pandas as pd
import numpy as np

from matplotlib import pyplot as plt
from sklearn.preprocessing import StandardScaler
from app.api.watch_trades import Trades
from app.api.fetch_candles import Candles
from app.machine_learning.trading import TradingBot

logging.basicConfig(level=logging.INFO)

exchanges = ['coinbasepro']
charts = [{"exchange":"coinbasepro", "symbol":"BTC/USD", "timeframe": "1d"}]

async def main():
    async with Candles(local_database=True, exchanges=exchanges) as manager:
        
        # await manager.watch_trades(['1INCH/USD'])
        
        candles = await manager.fetch_candles(
            charts=charts,
            from_date='2021-04-09T00:00:00.000Z',
            limit=1000
        )
        
        for a, b, c, df in candles:
            print(df)
            

if __name__ == "__main__":
    asyncio.run(main())