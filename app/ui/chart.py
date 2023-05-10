import asyncio
import json
import os
import dearpygui.dearpygui as dpg
import utils.do_stuff as do
import pandas as pd


class Chart:
    
    async def __init__(self, ccxt_manager, parent) -> None:
        self.parent = parent
        self.manager = ccxt_manager
        self.last_chart = None
        self.last_saved_chart = [{"exchange":"coinbasepro", "symbol":"BTC/USD", "timeframe":"1h"}]

        
        self.indicators = [
            'RSI', 'MACD', 'Bollinger Bands', 'SMA', 'EMA', 'HMA'
        ]

        # self.load_favorites()
            
        # self.draw_default_chart()
        
        with dpg.child_window(menubar=True, parent=self.parent) as self.chart_window:
            with dpg.menu_bar():
                with dpg.menu(label="Chart"):
                    dpg.add_menu_item(label='Select Chart', callback=self.chart_and_favorites_window)
                    dpg.add_slider_float(label="Candle Width", default_value=1.0, max_value=5.0, format='width = %.2f', callback=lambda s, a, u: dpg.configure_item('chart', weight=round(a, 2)))
            
    def draw_default_chart(self):
        exchange = next(iter(self.favorites["last_saved"]))
        symbol, timeframe = self.favorites["last_saved"][exchange]
        self.draw_chart(exchange, symbol, timeframe)
        
    def load_favorites(self):
        if os.path.exists('favorites.json'):
            with open('favorites.json', 'r') as f:
                self.favorites = json.load(f)
        else:
            self.favorites = {"last_saved": {}, "favorites": []}
        
    def save_favorites(self):
        with open('favorites.json', 'w') as f:
            json.dump(self.favorites, f)

    def add_favorite(self, exchange, symbol, timeframe):
        favorite = {"exchange": exchange, "symbol": symbol, "timeframe": timeframe}
        
        print(favorite)
        return
        
        if favorite not in self.favorites["favorites"]:
            self.favorites["favorites"].append(favorite)
            self.save_favorites()
        favorite_str = f"{favorite['exchange']} - {favorite['symbol']} ({favorite['timeframe']}) added to favorites."
        dpg.set_value('added_favorite', favorite_str)

    def remove_favorite(self, sender, app_data, user_data):
        del self.favorites["favorites"][user_data]
        self.save_favorites()
        self.draw_favorites_list()

    def search_symbol(self, searcher, result, search_list):
        do.searcher(searcher, result, search_list)

    def draw_chart(self, exchange, symbol, timeframe, candles):

        self.candles = candles
            
        if self.last_chart is None:
            self.last_chart = dpg.generate_uuid()
        else:
            dpg.delete_item(self.last_chart)
            
        with dpg.subplots(
            rows=2,
            columns=1,
            label=f"{exchange.upper()} | {symbol.upper()} | {timeframe}",
            row_ratios=[80, 20],
            link_all_x=True,
            width=-1,
            height=-1,
            parent=self.chart_window,
            tag=self.last_chart):
            
            dates = [pd.Timestamp(date).timestamp() for date in candles["dates"]]
            
            with dpg.plot(use_local_time=True):
                dpg.add_plot_legend()
                xaxis = dpg.add_plot_axis(dpg.mvXAxis, time=True)
                with dpg.plot_axis(dpg.mvYAxis, label="USD"):
                    dpg.add_candle_series(dates, candles['opens'].values, candles['closes'].values, candles['lows'].values, candles['highs'].values, time_unit=do.convert_timeframe(timeframe), tag='chart')
                    dpg.fit_axis_data(dpg.top_container_stack())
                dpg.fit_axis_data(xaxis)

            with dpg.plot(use_local_time=True):
                dpg.add_plot_legend()
                xaxis_vol = dpg.add_plot_axis(dpg.mvXAxis, label="Time [UTC]", time=True)
                with dpg.plot_axis(dpg.mvYAxis, label="USD"):
                    self.bar_series = dpg.add_line_series(dates, candles['volumes'].values)
                    dpg.fit_axis_data(dpg.top_container_stack())
                dpg.fit_axis_data(xaxis_vol)
        
    def update_chart(self, trades, exchange_object):
        # store the timeframe in milliseconds

        # Get the last candle in the chart
        current_candle = candles.iloc[-1].copy()

        # Loop through trades
        for trade in trades:
            
            self.timeframe_ms = trade['timestamp']
            self.close_time = candles.loc[candles.index[-1], 'dates'] * 1000 + self.timeframe_ms

            # If the trade is in a new candle, add a new candle to candles
            if trade['timestamp'] >= self.close_time:
                new_candle = pd.DataFrame({
                    'dates': [self.close_time / 1000],
                    'opens': [trade['price']],
                    'highs': [trade['price']],
                    'lows': [trade['price']],
                    'volumes': [trade['amount']],
                    'closes': [trade['price']]
                })

                candles = candles.append(new_candle, ignore_index=True)

                # Set current_candle to the new candle
                current_candle = new_candle.iloc[0]

            else:
                # If the trade is in the current candle, update the last candle in candles
                current_candle['highs'] = max(current_candle['highs'], trade['price'])
                current_candle['lows'] = min(current_candle['lows'], trade['price'])
                current_candle['volumes'] += trade['amount']
                current_candle['closes'] = trade['price']

                candles.iloc[-1] = current_candle

            # if trade['amount'] >= 1:
            #     x = candles.iloc[-1]['dates']
            #     y = trade['price']
            #     size = trade['amount'] # adjust the size to your preference
            #     # dpg.draw_circle(center=[x, y], radius=size, color=[255, 255, 255, 255], thickness=1, parent='plot')

            
            dpg.configure_item(
                self.candle_series,
                dates=candles['dates'].values,
                opens=candles['opens'].values,
                highs=candles['highs'].values,
                lows=candles['lows'].values,
                closes=candles['closes'].values
            )

            dpg.configure_item(
                self.bar_series,
                x=candles['dates'].values,
                y=candles['volumes'].values
            )