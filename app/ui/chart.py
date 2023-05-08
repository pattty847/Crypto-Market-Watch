import dearpygui.dearpygui as dpg
import utils.do_stuff as do
import pandas as pd


class Chart:
    
    def __init__(self, ccxt_manager, parent) -> None:
        self.parent = parent
        self.manager = ccxt_manager
        
        with dpg.child_window(menubar=True, parent=self.parent) as self.chart_window:
            with dpg.menu_bar():
                with dpg.menu(label="Chart"):
                    dpg.add_menu_item(label='Select Chart', callback=self.chart_selection_window)
                    dpg.add_slider_float(label="Candle Width", default_value=1.0, max_value=5.0, format='width = %.2f', callback=lambda s, a, u: dpg.configure_item('chart', weight=round(a, 2)))
            
    def draw_chart(self, exchange, symbol, timeframe, candles):
                
            with dpg.subplots(
                rows=2,
                columns=1,
                label=f"{exchange.upper()} | {symbol.upper()} | {timeframe}",
                row_ratios=[80, 20],
                link_all_x=True,
                width=-1,
                height=-1,
                parent=self.chart_window):
                
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
            
    def chart_selection_window(self, sender, app_data, user_data):
        with dpg.window(popup=True, modal=True, width=500, height=500):
            pass