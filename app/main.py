import asyncio
import logging
import pandas as pd

from sklearn.preprocessing import StandardScaler
from api.watch_trades import Trades
from api.fetch_candles import Candles
from ai.trading import Trading

logging.basicConfig(level=logging.INFO)

async def main():
    async with Trades(local_database=True) as manager:
        await manager.load_exchanges(['kucoinfutures'])
        
        await manager.watch_trades(['BTC/USDT:USDT'])
        
        # candles = await manager.fetch_candles([{"exchange":"kucoinfutures", "symbol":"BTC/USDT:USDT", "timeframe":"1h"}], None, 1000)
        
        # for exchange_id, symbol, timeframe, dataframe in candles:
        #     print(f"Exchange ID: {exchange_id}, Symbol: {symbol}, Timeframe: {timeframe}")
        #     dataframe = dataframe.dropna()
            
        #     print(dataframe.head(10))
            
        #     trading = Trading()
        #     trading.run(dataframe)

if __name__ == "__main__":
    asyncio.run(main())