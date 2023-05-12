import asyncio
import json
import os
import dearpygui.dearpygui as dpg
from ui.favorites import FavoritesManager
import utils.do_stuff as do
import pandas as pd


class Chart:
    
    def __init__(self, ccxt_manager, parent, notifications) -> None:
        self.parent = parent
        self.manager = ccxt_manager
        self.last_chart = None
        self.last_saved_charts = [
            {"exchange":"coinbasepro", "symbol":"BTC/USD", "timeframe":"1h"},
            {"exchange":"bybit", "symbol":"BTC/USDT", "timeframe":"1h"}
        ]
        self.indicators = [
            'RSI', 'MACD', 'Bollinger Bands', 'SMA', 'EMA', 'HMA'
        ]
        self.notifications = notifications
        first_chart = self.last_saved_charts[0]
        self.current_tab = f"{first_chart['exchange']}_{first_chart['symbol']}_{first_chart['timeframe']}_chart"
        self.favorites = FavoritesManager()
            
        # self.draw_default_chart()
        
        with dpg.child_window(menubar=True, parent=self.parent) as self.chart_window:
            with dpg.menu_bar():
                with dpg.menu(label="Chart"):
                    dpg.add_menu_item(label='Select Chart', callback=self.chart_and_favorites_window)
                    dpg.add_menu_item(label='print', callback=lambda: print(dpg.get_item_children('tab_bar')))
                    dpg.add_slider_float(label="Candle Width", default_value=1.0, max_value=5.0, format='width = %.2f', callback=lambda s, a, u: dpg.configure_item(self.current_tab, weight=round(a, 2)))
                
                with dpg.menu(label='Indicators'):
                    for indicator in self.indicators:
                        dpg.add_menu_item(label=indicator)         

            with dpg.tab_bar(tag='tab_bar', reorderable=True, callback=self.tab_callback):
                pass

    @classmethod
    async def create(cls, ccxt_manager, parent, notifications):
        self = cls(ccxt_manager, parent, notifications)
        await self.initialize()
        return self

    async def initialize(self):
        # Fetch all candles at once
        all_candles = await self.manager.fetch_all_candles(
            charts=self.last_saved_charts,
            since=None, 
            limit=1000, 
            resample_timeframe=None
        )

        # Iterate over the returned candles and draw each chart
        for chart, candles in zip(self.last_saved_charts, all_candles):
            exchange = chart['exchange']
            symbol = chart['symbol']
            timeframe = chart['timeframe']
            parent = f'{exchange}_{symbol}_{timeframe}'
            tag = f'{parent}_chart'
            with dpg.tab(label=f"{symbol}:{timeframe}", parent='tab_bar', tag=parent):
                self.draw_chart(exchange, symbol, timeframe, candles[3], parent, tag)


    def chart_and_favorites_window(self, sender, app_data, user_data):
        with dpg.window(label="Chart Selection and Favorites", tag='charts_favorites', width=466, height=444, on_close=dpg.delete_item('charts_favorites')):
            with dpg.tab_bar() as favorites_tab_bar:
                with dpg.tab(label="Chart Selection"):
                    with dpg.child_window(width=-1, height=-1):
                        for exchange in self.manager.watched_exchanges:
                            symbols = self.manager.exchanges[exchange]['symbols']
                            timeframes = self.manager.exchanges[exchange]['timeframes']
                            with dpg.child_window(width=-1, height=320):
                                dpg.add_text(f"{exchange.upper()}")
                                with dpg.child_window(width=-1):
                                    
                                    dpg.add_input_text(hint='Search', callback=lambda s, a, u: do.searcher(s, f'{exchange}_symbol_listbox', sorted(symbols)))
                                    dpg.add_listbox(sorted(symbols), width=-1, num_items=7, tag=f'{exchange}_symbol_listbox')
                                    
                                    dpg.add_input_text(hint='Custom TF (13m, 33m, 80m, 3h, 8h, etc)', width=-1, tag=f'{exchange}_custom_tf_text')
                                    dpg.add_listbox(timeframes, width=-1, tag=f'{exchange}_timeframe_listbox')
                                    
                                    dpg.add_button(label="Push to Chart", width=-1)
                                    dpg.add_button(label="Add to Favorites", width=-1, callback=lambda s, a, u: self.add_favorite(
                                        exchange, 
                                        dpg.get_value(f'{exchange}_symbol_listbox'), 
                                        dpg.get_value(f'{exchange}_timeframe_listbox')))
                                        
                self.favorites.draw_favorites_tab(favorites_tab_bar)

        self.center_window('charts_favorites', 466, 444)

    def draw_chart(self, exchange, symbol, timeframe, candles, parent, tag):
            
        # if self.last_chart is None:
        #     self.last_chart = dpg.generate_uuid()
        # else:
        #     dpg.delete_item(self.last_chart)
            
        with dpg.subplots(
            rows=2,
            columns=1,
            label=f"{exchange.upper()} | {symbol.upper()} | {timeframe}",
            row_ratios=[80, 20],
            link_all_x=True,
            width=-1,
            height=-1,
            parent=parent):
            
            dates = [pd.Timestamp(date).timestamp() for date in candles["dates"]]
            
            with dpg.plot(use_local_time=True):
                dpg.add_plot_legend()
                xaxis = dpg.add_plot_axis(dpg.mvXAxis, time=True)
                with dpg.plot_axis(dpg.mvYAxis, label="USD"):
                    dpg.add_candle_series(dates, candles['opens'].values, candles['closes'].values, candles['lows'].values, candles['highs'].values, time_unit=do.convert_timeframe(timeframe), tag=tag)
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
            
    def center_window(self, window_tag, window_width, window_height):
        # Get the size of the viewport
        viewport_width, viewport_height = dpg.get_viewport_width(), dpg.get_viewport_height()

        # Calculate the position for the window to be centered
        pos_x = (viewport_width - window_width) // 2
        pos_y = (viewport_height - window_height) // 2

        # Set the position of the window
        dpg.set_item_pos(window_tag, (pos_x, pos_y))
        
    def tab_callback(self, s, chart, u):
        self.current_tab = f'{dpg.get_item_alias(chart)}_chart'