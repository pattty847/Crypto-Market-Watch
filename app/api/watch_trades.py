import asyncio
import logging
import threading

from asyncio import Queue
from typing import Dict, List, Optional, Tuple
from ..analysis.market_aggregator import MarketAggregator
from .ccxt_interface import CCXTInterface


class Trades(CCXTInterface):
    def __init__(self, local_database, exchanges):
        super().__init__(local_database, exchanges)
        self.agg = MarketAggregator()
        self.trade_queue = Queue()
        self.thread = threading.Thread()
        self.pause_events = {} 
    
    #########################################################################################################
    # Watch Trades

    async def watch_trades(self, symbols: List[str]):
        async def watch_trades_(exchange_id, symbols):
            exchange = self.exchange_list[exchange_id]["ccxt"]
            try:
                while True:
                    for symbol in symbols:
                        if (exchange_id, symbol) not in self.pause_events:
                            self.pause_events[(exchange_id, symbol)] = asyncio.Event()
                            self.pause_events[(exchange_id, symbol)].set()
                            
                        await self.pause_events[(exchange_id, symbol)].wait()
                        
                        trades = await exchange.watch_trades(symbol)
                        await self.trade_queue.put((exchange_id, trades))
            except (Exception, KeyboardInterrupt) as e:
                logging.warning(f"Error watching trades on {exchange_id}: {e}")

        async def process_trades():
            while True:
                exchange_id, trades = await self.trade_queue.get()
                for trade in trades:
                    self.agg.calculate_stats(exchange_id, trade)
                    self.agg.report_statistics()
                
                # await self.agg.aggregate_trades(exchange_id, trades)
                
                self.trade_queue.task_done()
                

        watch_tasks = [watch_trades_(exchange_id, symbols) for exchange_id in self.exchange_list.keys()]
        worker_tasks = [process_trades() for _ in range(len(self.exchange_list))]
        all_tasks = watch_tasks + worker_tasks

        try:
            await asyncio.gather(*all_tasks)
        except KeyboardInterrupt:
            for t in all_tasks:
                t.cancel()
            await asyncio.gather(*all_tasks, return_exceptions=True)
        
    def start_watch_trades_thread(self, symbols):
        self.thread = threading.Thread(target=self._run_watch_trades, args=(symbols,))
        self.thread.start()

    def _run_watch_trades(self, symbols):
        asyncio.run(self.watch_trades(symbols))
        
    async def pause_trades(self, exchange_id, symbol):
        self.pause_events[(exchange_id, symbol)].clear()

    async def resume_trades(self, exchange_id, symbol):
        self.pause_events[(exchange_id, symbol)].set()
    
    # End Watch Trades
    #########################################################################################################