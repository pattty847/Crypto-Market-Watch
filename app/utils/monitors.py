from screeninfo import get_monitors

def get_window_position():
    primary_monitor = get_monitors()[0]
    width = int(primary_monitor.width * 0.8)
    height = int(primary_monitor.height * 0.8)
    x_pos = int(primary_monitor.x + (primary_monitor.width - width) / 2)
    y_pos = int(primary_monitor.y + (primary_monitor.height - height) / 2)
    return x_pos, y_pos, width, height

def get_centered_window_dimensions_(monitor):
    # Calculate 70% of the monitor's width and height
    window_width = int(monitor.width * 0.7)
    window_height = int(monitor.height * 0.7)

    # Calculate the position for the centered window
    window_x = monitor.x + (monitor.width - window_width) // 2
    window_y = monitor.y + (monitor.height - window_height) // 2

    return window_x, window_y, window_width, window_height