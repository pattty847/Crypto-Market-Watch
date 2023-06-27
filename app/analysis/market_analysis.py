import logging
from typing import List
import ccxt
import pandas as pd
import seaborn as sns
import matplotlib.pyplot as plt

def get_highest_volume_symbols(exchange_id: str, top_x: int):
    exchange = getattr(ccxt, exchange_id)()
    exchange.load_markets()

    ticker_info = exchange.fetch_tickers()

    # Prepare a list of tuples where each tuple is (symbol, 24h volume)
    # Only add to the list if quoteVolume is not None
    volume_list = [(symbol, ticker['quoteVolume']) for symbol, ticker in ticker_info.items() if ticker['quoteVolume'] is not None]

    # Sort the list by volume in descending order
    volume_list.sort(key=lambda x: x[1], reverse=True)

    # Return the symbols of the top X markets by volume
    return [symbol for symbol, volume in volume_list[:top_x]]

def create_correlation_heatmap(exchange_id: str):
    # Initialize a dictionary to hold our data.
    data = {}

    exchange = getattr(ccxt, exchange_id)()
    exchange.load_markets()

    symbols = ['BTC/USDT', 'ETH/USDT', 'XRP/USDT', 'PEPE/USDT', 'SOL/USDT', 'XMR/USDT', 'BCH/USDT', 'TOMI/USDT', 'BNB/USDT', 'AAVE/USDT', 'SHIB/USDT', 'LINK/USDT', 'DOGE/USDT']

    # Fetch the closing prices for each symbol.
    for symbol in symbols:
        logging.info(f'Fetching {symbol}')
        ohlcv = exchange.fetch_ohlcv(symbol, '1h')  # Get daily data
        df = pd.DataFrame(ohlcv, columns=['Timestamp', 'Open', 'High', 'Low', 'Close', 'Volume'])
        df['Timestamp'] = pd.to_datetime(df['Timestamp'], unit='ms')  # Convert timestamp to datetime
        data[symbol] = df['Close']

    # Create a DataFrame from our dictionary.
    df = pd.DataFrame(data)

    # Calculate the correlations.
    corr = df.corr()

    # Plot the correlations as a heatmap.
    plt.figure(figsize=(10,10))
    sns.heatmap(corr, annot=True, cmap='coolwarm')
    plt.show()


def create_dataframe(exchanges: List, symbols: List):
    all_dataframes = {}
    for exchange_id in exchanges:
        for symbol in symbols:
            exchange = getattr(ccxt, exchange_id)()

            base, quote = symbol.split('/')

            base_volume_title = f'Base Volume {base}'
            quote_volume_title = f'Quote Volume {quote}'

            # Fetch daily OHLCV data. 1D denotes 1 day. You can adjust this as needed.
            data = exchange.fetch_ohlcv(symbol, '1d')

            df = pd.DataFrame(data, columns=['Timestamp', 'Open', 'High', 'Low', 'Close', base_volume_title])
            df['Timestamp'] = pd.to_datetime(df['Timestamp'], unit='ms') # Convert timestamp to datetime.

            # Quote volume
            df[quote_volume_title] = (df[base_volume_title] * df['Close']).apply(lambda x: '{:,.2f}'.format(x))

            pct_change_series = df['Close'].pct_change() * 100
            close_idx = df.columns.get_loc('Close')
            df.insert(close_idx+1, 'CLOSE_PCT_CHANGE', pct_change_series)

            # Calculate the moving averages.
            df['SMA_20'] = df['Close'].rolling(window=20).mean() # 20-day SMA
            df['SMA_50'] = df['Close'].rolling(window=50).mean() # 50-day SMA
            df['SMA_100'] = df['Close'].rolling(window=100).mean() # 50-day SMA
            df['SMA_200'] = df['Close'].rolling(window=200).mean() # 200-day SMA

            # Determine the trend.
            df['SMA_Short_Term_Trend'] = df['SMA_20'] > df['SMA_50']
            df['SMA_Long_Term_Trend'] = df['SMA_50'] > df['SMA_200']

            df['SMA_Short_Term_Trend'] = df['SMA_Short_Term_Trend'].apply(lambda x: 'Bull' if x else 'Bear')
            df['SMA_Long_Term_Trend'] = df['SMA_Long_Term_Trend'].apply(lambda x: 'Bull' if x else 'Bear')

            df['SMA_Trend_Strength'] = abs(df['SMA_20'] - df['SMA_50'])
            df['SMA_Trend_Strength'] = pd.cut(df['SMA_Trend_Strength'], bins=5, labels=['Neutral', 'Weak', 'Moderate', 'Strong', 'Very Strong'])


            # Calculate the moving averages.
            df['EMA_20'] = df['Close'].ewm(span=20, adjust=False).mean()
            df['EMA_50'] = df['Close'].ewm(span=50, adjust=False).mean()
            df['EMA_100'] = df['Close'].ewm(span=100, adjust=False).mean()
            df['EMA_200'] = df['Close'].ewm(span=200, adjust=False).mean()

            # Determine the trend.
            df['EMA_Short_Term_Trend'] = df['EMA_20'] > df['EMA_50']
            df['EMA_Long_Term_Trend'] = df['EMA_50'] > df['EMA_200']

            df['EMA_Short_Term_Trend'] = df['EMA_Short_Term_Trend'].apply(lambda x: 'Bull' if x else 'Bear')
            df['EMA_Long_Term_Trend'] = df['EMA_Long_Term_Trend'].apply(lambda x: 'Bull' if x else 'Bear')

            df['EMA_Trend_Strength'] = abs(df['EMA_20'] - df['EMA_50'])
            df['EMA_Trend_Strength'] = pd.cut(df['EMA_Trend_Strength'], bins=5, labels=['Neutral', 'Weak', 'Moderate', 'Strong', 'Very Strong'])

            symbol = symbol.replace('/', '-')
            df.to_csv(f'{exchange_id}:{symbol}.csv')

            all_dataframes[(exchange_id, symbol)] = df

    return all_dataframes


create_correlation_heatmap('kucoin')