import asyncio
from typing import Dict, List, Optional, Tuple
import ccxt
import logging
import pandas as pd

from analysis.technical_analysis import TA
from .ccxt_interface import CCXTInterface

class Candles(CCXTInterface):
    def __init__(self, local_database):
        super().__init__(local_database)
        
        self.ta = TA()
        
        
    #########################################################################################################
    # Fetch Candles
    
    
    #TODO: FIX THESE ISSUES DURING LOADING/SAVING OF CANDLES
    

    async def fetch_candles(
        self,
        charts: List[Dict[str, str]],
        from_date: str,
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
                self._fetch_candles(
                    exchange_id, symbol, timeframe, from_date, limit, max_retries, resample_timeframe)
            )
            fetch_tasks.append(task)

        return await asyncio.gather(*fetch_tasks)

        
    async def _fetch_candles(
        self, exchange_id, symbol, timeframe, from_date, limit, max_retries, resample_timeframe) -> Tuple[str, str, str, pd.DataFrame]:
        
        exchange = self.exchanges[exchange_id]["ccxt"]
        
        if symbol not in self.exchanges[exchange_id]["symbols"]:
            logging.error(f"Symbol {symbol} does not exist for exchange {exchange_id}. Skipping fetch.")
            return (exchange_id, symbol, timeframe, pd.DataFrame())

        if resample_timeframe:
            timeframe = self.find_highest_resample_timeframe(resample_timeframe, exchange.timeframes)   

        timeframe_duration_in_seconds = exchange.parse_timeframe(timeframe)
        timedelta = limit * timeframe_duration_in_seconds * 1000
        now = exchange.milliseconds()

        try:
            saved_candles = self.influx.read_candles_from_influxdb(exchange_id, symbol, timeframe or resample_timeframe)
        except Exception as e:
            logging.error(f"Error reading data from InfluxDB for {symbol} on {exchange_id}: {str(e)}")
            return (exchange_id, symbol, timeframe, pd.DataFrame())

        fetch_from_date = (
            exchange.parse8601(from_date) if saved_candles.empty else int(saved_candles["dates"].iloc[-1].timestamp() * 1000)
        )
        
        if not saved_candles.empty:
            cached_candles = saved_candles.drop(saved_candles.index[-1])
        else:
            cached_candles = saved_candles

        try:
            new_candles = await self.fetch_new_candles(
                exchange,
                symbol,
                timeframe,
                fetch_from_date,
                limit,
                max_retries,
                now,
                timedelta
            )
        except Exception as e:
            logging.error(f"Error fetching data from {exchange_id} for {symbol}: {str(e)}")
            return (exchange_id, symbol, timeframe, pd.DataFrame())

        # This is the entire data set of candles. The loaded (if any, and the freshly fetched 'new_candles' list)
        concatted_candles = self.concat_candles(cached_candles, new_candles)
        
        # Calculate a bunch of techincal indicators
        concatted_candles = self.ta.calculate_indicators(concatted_candles)

        # Turn the new candles into a dataframe
        new_candles_ = pd.DataFrame(new_candles, columns=['dates', 'opens', 'highs', 'lows', 'closes', 'volumes'])
        
        # Write the unsaved candles DataFrame to InfluxDB
        self.influx.write_candles_to_influxdb(exchange_id, symbol, timeframe, new_candles_)

        if resample_timeframe:
            concatted_candles = self.ta.resample_dataframe(concatted_candles, timeframe, resample_timeframe)
            timeframe = resample_timeframe

        return (exchange_id, symbol, timeframe, concatted_candles)


    async def fetch_new_candles(
        self,
        exchange,
        symbol: str,
        timeframe: str,
        fetch_date: int,
        limit: int,
        max_retries: int,
        now: int,
        timedelta: int,
    ) -> List[List]:
        new_candles = []
        done_fetching = False
        while not done_fetching:
            new_candle_batch = await self.retry_fetch_candles(exchange, symbol, timeframe, fetch_date, limit, max_retries)

            if new_candle_batch is None:
                break

            logging.info(f'Fetching new candles for {symbol}, {timeframe} on {exchange.id}')
            logging.info(f'Found ({len(new_candle_batch)}) new candle(s) from {exchange.iso8601(new_candle_batch[0][0])} to {exchange.iso8601(new_candle_batch[-1][0])}')

            new_candles += new_candle_batch

            if len(new_candle_batch):
                last_time = new_candle_batch[-1][0] + exchange.parse_timeframe(timeframe) * 1000
            else:
                last_time = fetch_date + timedelta

            if last_time >= now:
                done_fetching = True
            else:
                fetch_date = last_time

        return new_candles

    async def retry_fetch_candles(
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
                logging.error(f"Error fetching data from {exchange.id} for {symbol}: Reason - {e}")
                if isinstance(e, ccxt.BadSymbol):
                    break
                continue
            return new_candle_batch
        return

    def concat_candles(self, loaded_candles: pd.DataFrame, new_candles: List) -> pd.DataFrame:
        if new_candles is None:
            return loaded_candles
        
        new_candles_df = pd.DataFrame(new_candles, columns=['dates', 'opens', 'highs', 'lows', 'closes', 'volumes'])
        new_candles_df['dates'] = pd.to_datetime(new_candles_df['dates'], unit='ms', utc=True)
        loaded_candles = pd.concat([loaded_candles, new_candles_df], ignore_index=True)
        # loaded_candles = loaded_candles.groupby('dates').agg({
        #     'opens': 'first',
        #     'highs': 'max',
        #     'lows': 'min',
        #     'closes': 'last',
        #     'volumes': 'sum'
        # }).reset_index()

        return loaded_candles
    
    def find_highest_resample_timeframe(self, resample_timeframe, available_timeframes):
        available_timeframes_minutes = [self.convert_to_minutes(tf) for tf in available_timeframes]
        resample_timeframe_minutes = self.convert_to_minutes(resample_timeframe)
        best_timeframe = max((tf for tf in available_timeframes_minutes if resample_timeframe_minutes % tf == 0), default=None)

        if best_timeframe is None:
            raise ValueError(f"No suitable timeframe found for resampling to {resample_timeframe}")

        return [tf for tf in available_timeframes if self.convert_to_minutes(tf) == best_timeframe][0]
    
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
        
    def resample_dataframe(self, df, original_timeframe, resample_timeframe):
        logging.info(f'Resampling from {original_timeframe} to {resample_timeframe}.')
        df = df.set_index(pd.to_datetime(df['dates'], unit='s'))
        ohlc = df.resample(resample_timeframe).agg({'opens': 'first', 'highs': 'max', 'lows': 'min', 'closes': 'last', 'volumes': 'sum'})
        ohlc = ohlc.dropna()
        return ohlc.reset_index()

    # End Fetch Candles
    #########################################################################################################