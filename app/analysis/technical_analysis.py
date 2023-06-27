import pandas as pd
import pandas_ta as pta
import numpy as np

class TA:
    def __init__(self) -> None:
        pass
    
    def calculate_indicators(self, df: pd.DataFrame):
        pct_change_series = df['closes'].pct_change() * 100
        close_idx = df.columns.get_loc('closes')
        df.insert(close_idx+1, 'CLOSE_PCT_CHANGE', pct_change_series)
        
        df['RSI'] = pta.rsi(df['closes'], length=14)
        
        df['EMA_10'] = pta.ema(df['closes'], length=10)
        df['EMA_100'] = pta.ema(df['closes'], length=100)
        df['EMA_200'] = pta.ema(df['closes'], length=200)

        # Stochastic Oscillator
        stoch = pta.stoch(df['highs'], df['lows'], df['closes'])
        df = pd.concat([df, stoch], axis=1) # Concatenate the stoch DataFrame to the original DataFrame

        # Volume indicators
        df['OBV'] = pta.obv(df['closes'], df['volumes'])
        
        # Calculate ADL
        clv = ((df['closes'] - df['lows']) - (df['highs'] - df['closes'])) / (df['highs'] - df['lows'])
        clv = clv.replace([np.inf, -np.inf], 0)  # replace inf values with 0
        df['ADL'] = (clv * df['volumes']).cumsum()

        return df