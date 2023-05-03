import dearpygui.dearpygui as dpg
import utils.do_stuff as do
import pandas as pd


class Chart:
    
    def __init__(self, parent) -> None:
        with dpg.child_window(parent=parent, menubar=True) as self.parent:
            with dpg.menu_bar():
                pass
            
            self.candles = pd.DataFrame()
        
        # self.draw_chart('coinbasepro', 'BTC/USD', '1h')
            
    def draw_chart(self, exchange, symbol, timeframe):
        with dpg.subplots(
            rows=2,
            columns=1,
            label=f"{exchange.upper()} | {symbol.upper()} | {timeframe.upper()}",
            row_ratios=[80, 20],
            link_all_x=True,
            width=-1,
            height=-1,
            parent=self.parent):
            
            with dpg.plot(use_local_time=True):
                dpg.add_plot_legend()

                xaxis_candles = dpg.add_plot_axis(dpg.mvXAxis, time=True)

                with dpg.plot_axis(dpg.mvYAxis, label="USD"):

                    self.candle_series = dpg.add_candle_series(
                        self.candles["dates"].values,
                        self.candles["opens"].values,
                        self.candles["closes"].values,
                        self.candles["lows"].values,
                        self.candles["highs"].values,
                        time_unit=do.convert_timeframe(self.timeframe),
                    )
                    
                    dpg.fit_axis_data(dpg.top_container_stack())
                    dpg.fit_axis_data(xaxis_candles)

            with dpg.plot(use_local_time=True):
                dpg.add_plot_legend()
                xaxis_vol = dpg.add_plot_axis(dpg.mvXAxis, label="Time [UTC]", time=True)

                with dpg.plot_axis(dpg.mvYAxis, label="USD"):
                    self.bar_series = dpg.add_line_series(
                        self.candles['dates'].values,
                        self.candles['volumes'].values
                    )

                    dpg.fit_axis_data(dpg.top_container_stack())
                    dpg.fit_axis_data(xaxis_vol)
        
    def update_chart(self, trades, exchange_object):
        # store the timeframe in milliseconds

        # Get the last candle in the chart
        current_candle = self.candles.iloc[-1].copy()

        # Loop through trades
        for trade in trades:
            
            self.timeframe_ms = trade['timestamp']
            self.close_time = self.candles.loc[self.candles.index[-1], 'dates'] * 1000 + self.timeframe_ms

            # If the trade is in a new candle, add a new candle to self.candles
            if trade['timestamp'] >= self.close_time:
                new_candle = pd.DataFrame({
                    'dates': [self.close_time / 1000],
                    'opens': [trade['price']],
                    'highs': [trade['price']],
                    'lows': [trade['price']],
                    'volumes': [trade['amount']],
                    'closes': [trade['price']]
                })

                self.candles = self.candles.append(new_candle, ignore_index=True)

                # Set current_candle to the new candle
                current_candle = new_candle.iloc[0]

            else:
                # If the trade is in the current candle, update the last candle in self.candles
                current_candle['highs'] = max(current_candle['highs'], trade['price'])
                current_candle['lows'] = min(current_candle['lows'], trade['price'])
                current_candle['volumes'] += trade['amount']
                current_candle['closes'] = trade['price']

                self.candles.iloc[-1] = current_candle

            # if trade['amount'] >= 1:
            #     x = self.candles.iloc[-1]['dates']
            #     y = trade['price']
            #     size = trade['amount'] # adjust the size to your preference
            #     # dpg.draw_circle(center=[x, y], radius=size, color=[255, 255, 255, 255], thickness=1, parent='plot')

            
            dpg.configure_item(
                self.candle_series,
                dates=self.candles['dates'].values,
                opens=self.candles['opens'].values,
                highs=self.candles['highs'].values,
                lows=self.candles['lows'].values,
                closes=self.candles['closes'].values
            )

            dpg.configure_item(
                self.bar_series,
                x=self.candles['dates'].values,
                y=self.candles['volumes'].values
            )