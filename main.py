import paho.mqtt.client as mqtt
import paho.mqtt.subscribe as subscribe
import asyncio
import aiohttp
import urllib
import sys

import settings

class Mqtt2Telegram:
    def __init__(self):
        self.mqtt_client = mqtt.Client()
        self.mqtt_client.connect_async(settings.MQTT_HOST, settings.MQTT_PORT, 60)
        self.mqtt_client.on_connect = self.on_mqtt_connect
        self.mqtt_client.on_message = self.on_mqtt_received
        self.mqtt_client.loop_start()

        self.loop = asyncio.get_event_loop()
        self.loop.run_forever()

    def on_mqtt_connect(self, client, userdata, flags, rc):
        print("Connected to MQTT server")
        self.mqtt_client.subscribe(settings.MQTT_TOPICS)

    def on_mqtt_received(self, client, userdata, msg):
        if msg.retain:
            return
        if isinstance(msg.payload, bytes):
            msg.payload = msg.payload.decode("utf-8")
        
        print("%s %s" % (msg.topic, msg.payload))
        asyncio.run_coroutine_threadsafe(self._send_to_telegram("{} {}".format(msg.topic, msg.payload)), loop=self.loop)

    async def _send_to_telegram(self, text):
        try:
            text = urllib.parse.quote(text)
            url = ("https://api.telegram.org/bot{}/sendMessage?chat_id={}&text={}&disable_notification={}".format(settings.TELEGRAM_BOT_TOKEN, settings.TELEGRAM_CHAT_ID, text, settings.TELEGRAM_DISABLE_NOTIFICATION))
            async with aiohttp.ClientSession() as session:
                async with session.get(url, timeout=10) as resp:
                    print("Telegram message sent, response from server: {}".format(await resp.text()))
        except:
            print("Failed to send Telegram message '{}' Error: {}".format(text,sys.exc_info()[0]))

    
if __name__ == "__main__":
    app = Mqtt2Telegram()
