import asyncio
import ccxt.pro as ccxtpro
import logging
import logging
import asyncio

from asyncio import Queue

from api.market_aggregator import MarketAggregator


logging.basicConfig(level=logging.INFO)

class CCXTManager:
    """
    This class manages connections to CCXT exchanges.
    """
    def __init__(self, exchanges) -> None:
        self.watched_exchanges = exchanges
        self.exchanges = self.get_exchanges()
        self.agg = MarketAggregator()
        self.trade_queue = Queue()
        
    def get_exchanges(self):
        supported_exchanges = {}
        for exchange_id in self.watched_exchanges:
            exchange_class = getattr(ccxtpro, exchange_id)({'newUpdates': False})
            if exchange_class.has['watchTrades']:
                supported_exchanges[exchange_id] = exchange_class
        return supported_exchanges
    
    async def close_all_exchanges(self):
        async def close_exchange(exchange_id):
            exchange = self.exchanges[exchange_id]
            try:
                await exchange.close()
                logging.info(f"{exchange_id} closed successfully.")
            except Exception as e:
                logging.error(f"Error closing {exchange_id}: {e}")

        tasks = [close_exchange(exchange_id) for exchange_id in self.exchanges.values()]
        await asyncio.gather(*tasks)

    async def watch_exchanges(self, symbols):
        async def watch_exchange(exchange_id, symbols):
            exchange = self.exchanges[exchange_id]
            try:
                while True:
                    for symbol in symbols:
                        trades = await exchange.watch_trades(symbol)
                        await self.trade_queue.put((exchange_id, trades))
            except (Exception, KeyboardInterrupt) as e:
                logging.info(f"Error watching trades on {exchange_id}: {e}")
            finally:
                try:
                    await exchange.close()
                    logging.info(f"{exchange_id} closed successfully.")
                except Exception as e:
                    logging.info(f"Error closing {exchange_id}: {e}")

        async def process_trades():
            while True:
                exchange_id, trades = await self.trade_queue.get()
                self.handle_trades(exchange_id, trades, None)
                self.trade_queue.task_done()

        watch_tasks = [watch_exchange(exchange_id, symbols) for exchange_id in self.exchanges]
        worker_tasks = [process_trades() for _ in range(len(self.exchanges))]
        all_tasks = watch_tasks + worker_tasks

        try:
            await asyncio.gather(*all_tasks)
        except KeyboardInterrupt:
            for t in all_tasks:
                t.cancel()
            await asyncio.gather(*all_tasks, return_exceptions=True)
            
    def handle_trades(self, exchange, trades, orderbook):
        [self.agg.calculate_stats(exchange, trade['symbol'], trade) for trade in trades]
        self.agg.report()