import json
import threading
import ccxt
import ccxt.pro as ccxtpro
import logging
import asyncio
import pandas as pd

from api.influx import InfluxDB
from influxdb_client import Point, WritePrecision
from api.ta import TechnicalAnalysis
from typing import Dict, List, Optional, Tuple
from asyncio import Queue
from api.market_aggregator import MarketAggregator

logging.basicConfig(level=logging.INFO)

class CCXTManager:
    """
    This class manages connections to CCXT exchanges.
    """
    async def __init__(self, exchanges, loop, influx) -> None:
        self.watched_exchanges = exchanges
        self.exchanges = await self.load_exchanges()
        self.influx = influx
        self.agg = MarketAggregator(self.influx)
        self.ta = TechnicalAnalysis()
        self.trade_queue = Queue()
        self.thread = threading.Thread()
        self.loop = loop
        self.pause_events = {}  
        
    @classmethod
    async def create(cls, exchanges, loop, influx):
        self = cls.__new__(cls)
        await self.__init__(exchanges, loop, influx)
        return self
        
    async def load_exchanges(self):
        supported_exchanges = {}
        for exchange_id in self.watched_exchanges:
            try:
                exchange_class = getattr(ccxtpro, exchange_id)({'newUpdates': False})
                await exchange_class.load_markets()
                if exchange_class.has['watchTrades'] and exchange_class.has['fetchOHLCV']:
                    supported_exchanges[exchange_id] = {
                        "ccxt":exchange_class,
                        "symbols": list(exchange_class.markets),
                        "timeframes": list(exchange_class.timeframes.keys())
                    }
            except Exception as e:
                logging.error(f"Error creating exchange object for {exchange_id}: {e}")
        return supported_exchanges
    
    async def add_exchange(self, exchange_id):
        if exchange_id in self.exchanges.keys():
            raise KeyError
        try:
            exchange_class = getattr(ccxtpro, exchange_id)({'newUpdates': False})
            await exchange_class.load_markets()
            if exchange_class.has['watchTrades'] and exchange_class.has['fetchOHLCV']:
                self.exchanges[exchange_id] = {
                    "ccxt":exchange_class,
                    "symbols": list(exchange_class.markets),
                    "timeframes": list(exchange_class.timeframes.keys())
                }
                self.watched_exchanges.append(exchange_id)
        except Exception as e:
            logging.error(f"Error adding exchange {exchange_id}: {e}")

    async def remove_exchange(self, exchange_id):
        if exchange_id in self.exchanges:
            del self.exchanges[exchange_id]
            self.watched_exchanges.remove(exchange_id)
        else:
            logging.error(f"Exchange {exchange_id} is not in the list of exchanges")
    
    async def close_all_exchanges(self):
        async def close_exchange(exchange_id):
            exchange = self.exchanges[exchange_id]["ccxt"]
            try:
                await exchange.close()
                logging.info(f"{exchange_id} closed successfully.")
            except Exception as e:
                logging.error(f"Error closing {exchange_id}: {e}")

        tasks = [close_exchange(exchange_id) for exchange_id in self.exchanges.keys()]
        await asyncio.gather(*tasks)

    async def watch_trades(self, symbols):
        async def watch_trades_(exchange_id, symbols):
            exchange = self.exchanges[exchange_id]["ccxt"]
            try:
                while True:
                    for symbol in symbols:
                        if (exchange_id, symbol) not in self.pause_events:
                            self.pause_events[(exchange_id, symbol)] = threading.Event()
                            self.pause_events[(exchange_id, symbol)].set()
                            
                        self.pause_events[(exchange_id, symbol)].wait()
                        
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

        watch_tasks = [watch_trades_(exchange_id, symbols) for exchange_id in self.exchanges.keys()]
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
        
    def start_watch_trades(self, symbols):
        self.thread = threading.Thread(target=self._run_watch_trades, args=(symbols,))
        self.thread.start()

    def _run_watch_trades(self, symbols):
        asyncio.run(self.watch_trades(symbols))
        
    def pause_trades(self, exchange_id, symbol):
        self.pause_events[(exchange_id, symbol)].clear()

    def resume_trades(self, exchange_id, symbol):
        self.pause_events[(exchange_id, symbol)].set()
        
    async def fetch_all_candles(
        self,
        charts: List[Dict[str, str]],
        since: str,
        limit: int,
        max_retries: int = 3,
        resample_timeframe: Optional[str] = None,
    ) -> List[Tuple[str, str, str, pd.DataFrame]]:
        fetch_tasks = []
        for chart in charts:
            exchange_id = chart['exchange']
            symbol = chart['symbol']
            timeframe = chart['timeframe']
            task = asyncio.create_task(
                self.fetch_candles_for_symbol(
                    exchange_id, symbol, timeframe, since, limit, max_retries, resample_timeframe)
            )
            fetch_tasks.append(task)

        return await asyncio.gather(*fetch_tasks)

        
    async def fetch_candles_for_symbol(
        self, exchange_id, symbol, timeframe, since, limit, max_retries, resample_timeframe) -> Tuple[str, str, str, pd.DataFrame]:
        
        exchange = self.exchanges[exchange_id]["ccxt"]
        
        if symbol not in self.exchanges[exchange_id]["symbols"]:
            print(f"Symbol {symbol} does not exist for exchange {exchange_id}. Skipping fetch.")
            return (exchange_id, symbol, timeframe, pd.DataFrame())

        if resample_timeframe:
            timeframe = self.find_highest_resample_timeframe(resample_timeframe, exchange.timeframes)

        timeframe_duration_in_seconds = exchange.parse_timeframe(timeframe)
        timedelta = limit * timeframe_duration_in_seconds * 1000
        now = exchange.milliseconds()

        try:
            loaded_candles = self.read_candles_from_influxdb(exchange_id, symbol, timeframe or resample_timeframe)
        except Exception as e:
            print(f"Error reading data from InfluxDB for {symbol} on {exchange_id}: {str(e)}")
            return (exchange_id, symbol, timeframe, pd.DataFrame())

        fetch_since = (
            exchange.parse8601(since) if loaded_candles.empty else int(loaded_candles["dates"].iloc[-1].timestamp() * 1000)
        )

        try:
            new_candles = await self.fetch_new_candles(
                exchange,
                symbol,
                timeframe,
                fetch_since,
                limit,
                max_retries,
                now,
                timedelta
            )
        except Exception as e:
            print(f"Error fetching data from {exchange_id} for {symbol}: {str(e)}")
            return (exchange_id, symbol, timeframe, pd.DataFrame())

        loaded_candles = self.concat_new_candles(loaded_candles, new_candles)

        # Write the updated candles DataFrame to InfluxDB
        new_candles_ = pd.DataFrame(new_candles, columns=['dates', 'opens', 'highs', 'lows', 'closes', 'volumes'])
        self.write_candles_to_influxdb(exchange_id, symbol, timeframe, new_candles_)

        if resample_timeframe:
            loaded_candles = self.ta.resample_dataframe(loaded_candles, timeframe, resample_timeframe)
            timeframe = resample_timeframe

        return (exchange_id, symbol, timeframe, loaded_candles)


    async def fetch_new_candles(
        self,
        exchange,
        symbol: str,
        timeframe: str,
        fetch_since: int,
        limit: int,
        max_retries: int,
        now: int,
        timedelta: int,
    ) -> List[List]:
        new_candles = []
        done_fetching = False
        while not done_fetching:
            new_candle_batch = await self.retry_request(exchange, symbol, timeframe, fetch_since, limit, max_retries)

            if new_candle_batch is None:
                break

            print(len(new_candle_batch), "new candle(s) from", exchange.iso8601(new_candle_batch[0][0]), "to", exchange.iso8601(new_candle_batch[-1][0]))

            new_candles += new_candle_batch

            if len(new_candle_batch):
                last_time = new_candle_batch[-1][0] + exchange.parse_timeframe(timeframe) * 1000
            else:
                last_time = fetch_since + timedelta

            if last_time >= now:
                done_fetching = True
            else:
                fetch_since = last_time

        return new_candles

    async def retry_request(
        self,
        exchange,
        symbol: str,
        timeframe: str,
        fetch_since: int,
        limit: int,
        max_retries: int,
    ) -> List:
        for _ in range(max_retries):
            try:
                new_candle_batch = await exchange.fetch_ohlcv(symbol, timeframe, since=fetch_since, limit=limit)
            except (ccxt.ExchangeNotAvailable, ccxt.ExchangeError, ccxt.RequestTimeout, ccxt.BadSymbol) as e:
                print(f"Error fetching data from {exchange.id} for {symbol}: {e}")
                if isinstance(e, ccxt.BadSymbol):
                    break
                continue
            return new_candle_batch
        return

    def concat_new_candles(self, loaded_candles: pd.DataFrame, new_candles: List) -> pd.DataFrame:
        if new_candles is None:
            return loaded_candles
        
        new_candles_df = pd.DataFrame(new_candles, columns=['dates', 'opens', 'highs', 'lows', 'closes', 'volumes'])
        new_candles_df['dates'] = pd.to_datetime(new_candles_df['dates'], unit='ms', utc=True)
        loaded_candles = pd.concat([loaded_candles, new_candles_df], ignore_index=True)

        return loaded_candles


    def write_candles_to_influxdb(
        self,
        exchange,
        symbol: str,
        timeframe: str,
        candles: pd.DataFrame,
        bucket: str = "candles",
    ) -> None:
        if candles.empty:
            print(f"Skipping write to InfluxDB for {exchange} {symbol} {timeframe} as the DataFrame is empty.")
            return
        
        symbol = symbol.replace("/", "_")
        points = []

        for record in candles.to_records():
            point = Point("candle") \
                .tag("exchange", exchange) \
                .tag("symbol", symbol) \
                .tag("timeframe", timeframe) \
                .field("opens", record.opens) \
                .field("highs", record.highs) \
                .field("lows", record.lows) \
                .field("closes", record.closes) \
                .field("volumes", record.volumes) \
                .time(record.dates, WritePrecision.MS)

            points.append(point)
            
        print(f"Writing {len(candles['dates'])} candles to bucket: {bucket}, organization: 'pepe'")
        
        self.influx.write_api.write(bucket, 'pepe', points)

    def read_candles_from_influxdb(
        self, exchange: str, symbol: str, timeframe: str, bucket="candles") -> Dict:

        symbol = symbol.replace("/", "_")
        
        query = f"""
        from(bucket: "{bucket}")
        |> range(start: -30d)
        |> filter(fn: (r) => r["_measurement"] == "candle")
        |> filter(fn: (r) => r["exchange"] == "{exchange}")
        |> filter(fn: (r) => r["symbol"] == "{symbol}")
        |> filter(fn: (r) => r["timeframe"] == "{timeframe}")
        |> filter(fn: (r) => r["_field"] == "closes" or r["_field"] == "highs" or r["_field"] == "lows" or r["_field"] == "opens" or r["_field"] == "volumes")
        |> pivot(rowKey:["_time"], columnKey: ["_field"], valueColumn: "_value")
        |> drop(columns: ["_start", "_stop"])
        """

        print(f"Fetching from bucket: {bucket}, organization: 'pepe', {exchange}, {symbol}, {timeframe}:")
        result = self.influx.query_api.query_data_frame(query, 'pepe')

        if result.empty:
            return pd.DataFrame(columns=["dates", "opens", "highs", "lows", "closes", "volumes"])
        else:
            result = result.rename(columns={"_time": "dates"})
            result = result.reindex(columns=["dates", "opens", "highs", "lows", "closes", "volumes"])
            return result
        
    def convert_to_minutes(self, timeframe_str: str) -> int:
        units = timeframe_str[-1]
        value = int(timeframe_str[:-1])

        if units == 's':
            return value
        elif units == 'm' or units == 'T':
            return value
        elif units == 'h':
            return value * 60
        elif units == 'd':
            return value * 60 * 24
        elif units == 'w':
            return value * 60 * 24 * 7
        else:
            raise ValueError(f'Unknown timeframe unit: {units}')

    def find_highest_resample_timeframe(self, resample_timeframe, available_timeframes):
        available_timeframes_minutes = [self.convert_to_minutes(tf) for tf in available_timeframes]
        resample_timeframe_minutes = self.convert_to_minutes(resample_timeframe)
        best_timeframe = max((tf for tf in available_timeframes_minutes if resample_timeframe_minutes % tf == 0), default=None)

        if best_timeframe is None:
            raise ValueError(f"No suitable timeframe found for resampling to {resample_timeframe}")

        return [tf for tf in available_timeframes if self.convert_to_minutes(tf) == best_timeframe][0]