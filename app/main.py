import asyncio
import logging
import pandas as pd

from sklearn.preprocessing import StandardScaler
from api.watch_trades import Trades
from api.fetch_candles import Candles
from ai.trading import Trading

logging.basicConfig(level=logging.INFO)

async def main():
    async with Candles(local_database=True) as manager:
        await manager.load_exchanges(['coinbasepro'])
        
        # await manager.watch_trades(['BTC/USDT:USDT'])
        
        candles = await manager.fetch_candles([{"exchange":"coinbasepro", "symbol":"BTC/USD", "timeframe": "1d"}], '2016-01-01T00:00:00.000Z', 1000)
        
        for exchange_id, symbol, timeframe, dataframe in candles:
            df = dataframe.dropna()
            
            # scaler = StandardScaler()
            
            # cols = df.columns[1:]

            # df.loc[:, cols] = scaler.fit_transform(df[cols])
            
            print(f"Exchange ID: {exchange_id}, Symbol: {symbol}, Timeframe: {timeframe}")
            print(df)

if __name__ == "__main__":
    asyncio.run(main())