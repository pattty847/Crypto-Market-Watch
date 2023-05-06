import pandas as pd
import pandas_ta as ta

class TechnicalAnalysis:
    def __init__(self):
        pass

    def HMA(self, length):
        return self.dataframe.ta.hma(length)

    def EHMA(self, length):
        return self.dataframe.ta.ema(length).apply(lambda x: 2 * x - self.dataframe.ta.ema(2*length))

    def THMA(self, length):
        return self.dataframe.ta.wma(length).apply(lambda x: 3 * x - self.dataframe.ta.wma(2*length) - self.dataframe.ta.wma(length))

    def Mode(self, modeSwitch, length):
        if modeSwitch == "Hma":
            return self.HMA(length)
        elif modeSwitch == "Ehma":
            return self.EHMA(length)
        elif modeSwitch == "Thma":
            return self.THMA(length)
        else:
            return None
        
    def resample_dataframe(self, df, original_timeframe, resample_timeframe):
        print(f'Resampling from {original_timeframe} to {resample_timeframe}.')
        df = df.set_index(pd.to_datetime(df['dates'], unit='s'))
        ohlc = df.resample(resample_timeframe).agg({'opens': 'first', 'highs': 'max', 'lows': 'min', 'closes': 'last', 'volumes': 'sum'})
        ohlc = ohlc.dropna()
        return ohlc.reset_index()
        
    def set_dataframes(self, dataframe):
        self.dataframe = dataframe