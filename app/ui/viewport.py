import logging
import dearpygui.dearpygui as dpg

# from .aggregate import CryptoData
# from .aggregate_window import Window
from screeninfo import get_monitors

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s | File: %(name)s | Log Type: %(levelname)s | Message: %(message)s',
    handlers=[
        logging.StreamHandler()
    ]
)

class Viewport:
    """
        This is a context manager to abstract the dearpygui setup and the main program setup.
    """
    def __init__(self, title):
        self.title = title
        self.tag = "root"
        self.logger = logging.getLogger(__name__)
        self.monitor = get_monitors(is_primary=True)[0]
        self.logger.info(f"Primary Monitor: {self.monitor}")
        self.window_x, self.window_y, self.window_width, self.window_height = self.get_centered_window_dimensions_(self.monitor)
        
    def __enter__(self):
        self.logger.info("Setting up DearPyGUI.")
        dpg.create_context()
        dpg.configure_app(init_file="dpg.ini")  # default file is 'dpg.ini'
        dpg.add_window(tag=self.tag) # primary window
        dpg.create_viewport(title=self.title, width=self.window_width, height=self.window_height, x_pos=self.window_x, y_pos=self.window_y)
        dpg.show_viewport()
        dpg.set_primary_window(self.tag, True)
        dpg.start_dearpygui()
    
    def get_centered_window_dimensions_(self, monitor):
        # Calculate 70% of the monitor's width and height
        window_width = int(monitor.width * 0.7)
        window_height = int(monitor.height * 0.7)

        # Calculate the position for the centered window
        window_x = monitor.x + (monitor.width - window_width) // 2
        window_y = monitor.y + (monitor.height - window_height) // 2

        return window_x, window_y, window_width, window_height
            
    def __exit__(self, exc_type, exc_val, exc_tb):
        dpg.destroy_context()
        dpg.save_init_file("dpg.ini")