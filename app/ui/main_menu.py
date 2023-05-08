import dearpygui.dearpygui as dpg
import dearpygui.demo as demo

# Name can be changed to the official name later.
class MainMenu:
    def __init__(self, parent) -> None:
        self.parent = parent
        
        self.build_nav_bar()
        
    def build_nav_bar(self):
        with dpg.menu_bar(parent=self.parent):
            dpg.add_menu_item(label="Demo", callback=demo.show_demo)