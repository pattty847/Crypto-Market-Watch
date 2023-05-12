import json
import os
import dearpygui.dearpygui as dpg

class FavoritesManager:
    def __init__(self):
        self.favorites = self.load_favorites()

    def load_favorites(self):
        if os.path.exists('favorites.json'):
            with open('favorites.json', 'r') as f:
                return json.load(f)
        else:
            return {"last_saved": {}, "favorites": []}

    def save_favorites(self):
        with open('favorites.json', 'w') as f:
            json.dump(self.favorites, f)
            
    def draw_favorites_list(self):
        if dpg.does_item_exist("favorites_list"):
            dpg.delete_item("favorites_list")

        with dpg.child_window(id="favorites_list", parent="favorites", autosize_x=True, autosize_y=False, horizontal_scrollbar=True):
            favorites_by_exchange = self.group_favorites_by_exchange()
            for exchange, favorites in favorites_by_exchange.items():
                with dpg.tree_node(label=exchange.upper(), default_open=True):
                    for index, favorite in favorites:
                        dpg.add_text(f"{favorite['symbol']}:{favorite['timeframe']}", bullet=True, indent=10)
                        with dpg.group(horizontal=True):
                            dpg.add_spacer()
                            dpg.add_button(label="Go")
                            dpg.add_button(label="Remove", callback=self.remove_favorite, user_data=index)
                        dpg.add_separator()
                        dpg.add_spacer()

    def group_favorites_by_exchange(self):
        favorites_by_exchange = {}
        for index, favorite in enumerate(self.favorites["favorites"]):
            exchange = favorite['exchange']
            if exchange not in favorites_by_exchange:
                favorites_by_exchange[exchange] = []
            favorites_by_exchange[exchange].append((index, favorite))
        return favorites_by_exchange

    def draw_favorites_tab(self, tab_bar):
        with dpg.tab(label="Favorites", parent=tab_bar):
            # Favorites
            with dpg.child_window(label="Favorites", width=-1, height=-1, tag='favorites'):
                self.draw_favorites_list()
            
    def draw_default_chart(self):
        exchange = next(iter(self.favorites["last_saved"]))
        symbol, timeframe = self.favorites["last_saved"][exchange]
        self.draw_chart(exchange, symbol, timeframe)

    def add_favorite(self, exchange, symbol, timeframe):
        favorite = dict(exchange=exchange, symbol=symbol, timeframe=timeframe)

        if favorite not in self.favorites["favorites"]:
            self.favorites["favorites"].append(favorite)
            self.save_favorites()
            self.draw_favorites_list()

    def remove_favorite(self, sender, app_data, user_data):
        del self.favorites["favorites"][user_data]
        self.save_favorites()
        self.draw_favorites_list()