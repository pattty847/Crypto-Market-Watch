import dearpygui.dearpygui as dpg
import time

class NotificationManager:
    def __init__(self):
        self.notifications = []

    def add_notification(self, message):
        # Create a new notification window
        with dpg.handler_registry():
            with dpg.window(label="Notification", no_title_bar=True, on_close=self.remove_notification) as handler:
                dpg.add_text(message)
        
        notification = {
            'handler': handler,
            'start_time': time.time(),
            'message': message,
            'state': 'enter'  # can be 'enter', 'stay', 'exit'
        }
        self.notifications.append(notification)

    def remove_notification(self, sender, app_data):
        # Find the notification that corresponds to the sender
        for i, notification in enumerate(self.notifications):
            if notification['handler'] == sender:
                del self.notifications[i]
                break

    def update(self):
        for notification in self.notifications:
            elapsed = time.time() - notification['start_time']
            
            if notification['state'] == 'enter':
                if elapsed < 1:  # slide in for 1 second
                    x = int(1920 - elapsed * 200)  # adjust these values to suit your needs
                    y = 1000  # adjust this value to suit your needs
                    dpg.configure_item(notification['handler'], pos=(x, y))
                else:
                    notification['state'] = 'stay'
                    notification['start_time'] = time.time()

            elif notification['state'] == 'stay':
                if elapsed > 5:  # stay for 5 seconds
                    notification['state'] = 'exit'
                    notification['start_time'] = time.time()

            elif notification['state'] == 'exit':
                if elapsed < 1:  # slide out for 1 second
                    x = int(1720 + elapsed * 200)  # adjust these values to suit your needs
                    y = 1000  # adjust this value to suit your needs
                    dpg.configure_item(notification['handler'], pos=(x, y))
                else:
                    dpg.delete_item(notification['handler'])
