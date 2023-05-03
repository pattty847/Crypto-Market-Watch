import dearpygui.dearpygui as dpg
import dearpygui.demo as demo

from ui.chart import Chart

# Name can be changed to the official name later.
class MainMenu:
    def __init__(self, viewport) -> None:
        self.viewport = viewport
        self.parent = self.viewport.tag
        
        self.build_nav_bar()
        self.push_tab()
        
    def build_nav_bar(self):
        with dpg.menu_bar(parent=self.parent) as self.menu_bar:
            dpg.add_menu_item(label="Demo", callback=demo.show_demo)
            
        with dpg.tab_bar(parent=self.parent) as self.tab_bar:
            
            dpg.add_tab_button(label="+", trailing=True, callback=self.push_tab)
    
    def push_tab(self):
        with dpg.tab(parent=self.tab_bar, label="Test"):
            chart = Chart(dpg.last_item())