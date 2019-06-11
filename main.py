import paho.mqtt.client as mqtt
import paho.mqtt.subscribe as subscribe
import urllib
import requests
import sys
import threading
import time
import queue
from collections import deque

import settings

class Mqtt2Telegram:
    def __init__(self):
        self.msg_history = deque(list())
        self.queue = queue.Queue(maxsize = 120)
        self.worker_thread = threading.Thread(name = "TelegrammerThread", target=self.telegram_worker)
        self.worker_thread.start()

        self.mqtt_client = mqtt.Client()
        self.mqtt_client.connect_async(settings.MQTT_HOST, settings.MQTT_PORT, 60)
        self.mqtt_client.on_connect = self.on_mqtt_connect
        self.mqtt_client.on_message = self.on_mqtt_received
        self.mqtt_client.loop_start()

    def telegram_worker(self):
        while(True):
            for i, e in reversed(list(enumerate(self.msg_history))):
                if e < time.time() - 60:
                    del self.msg_history[i]

            if len(self.msg_history) > 19:
                print("Max messages per minute limit reached (20), waiting...")
                time.sleep(1)
                continue

            item = self.queue.get()
            print("Message queue: {} of {}".format(self.queue.qsize(),self.queue.maxsize))
            self._send_to_telegram(item)
            self.msg_history.append(time.time())
            time.sleep(1)
            self.queue.task_done()
            
    def on_mqtt_connect(self, client, userdata, flags, rc):
        print("Connected to MQTT server")
        self.mqtt_client.subscribe(settings.MQTT_TOPICS)

    def on_mqtt_received(self, client, userdata, msg):
        if isinstance(msg.payload, bytes):
            msg.payload = msg.payload.decode("utf-8")
        if msg.retain:
            msg.payload = msg.payload + " (retain)"
        print("%s %s" % (msg.topic, msg.payload))
        self.queue.put("{} - {} {}".format(time.strftime("%H:%M:%S"), msg.topic, msg.payload))
        

    def _send_to_telegram(self, text):
        try:
            text = urllib.parse.quote(text)
            url = ("https://api.telegram.org/bot{}/sendMessage?chat_id={}&text={}&disable_notification={}".format(settings.TELEGRAM_BOT_TOKEN, settings.TELEGRAM_CHAT_ID, text, settings.TELEGRAM_DISABLE_NOTIFICATION))
            requests.get(url, timeout=5)
        except:
            print("Failed to send Telegram message '{}' Error: {}".format(text,sys.exc_info()[0]))

    
if __name__ == "__main__":
    app = Mqtt2Telegram()
