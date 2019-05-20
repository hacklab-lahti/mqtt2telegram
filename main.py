import paho.mqtt.subscribe as subscribe
import asyncio
import aiohttp
import urllib

import settings

def send_to_telegram(text, silent=settings.TELEGRAM_DISABLE_NOTIFICATION):
    asyncio.ensure_future(_send_to_telegram(**args, **kwargs))

async def _send_to_telegram(text, silent=settings.TELEGRAM_DISABLE_NOTIFICATION):
    text = urllib.parse.quote(text)
    url = ("https://api.telegram.org/bot{}/sendMessage?chat_id={}&text={}&disable_notification={}".format(settings.TELEGRAM_BOT_TOKEN, settings.TELEGRAM_CHAT_ID, text, silent))
    async with aiohttp.ClientSession() as session:
                async with session.get(url, timeout=10) as resp:
                    print("Telegram message sent, response from server: {}".format(await resp.text()))

def on_mqtt_received(client, userdata, message):
    print("%s %s" % (message.topic, message.payload))
    send_to_telegram("{} {}".format(message.topic, message.payload.decode('utf-8')))

subscribe.callback(on_mqtt_received, settings.MQTT_TOPICS, qos=0, userdata=None, hostname=settings.HOST,
    port=settings.MQTT_PORT, client_id="", keepalive=60, will=None, auth=None, tls=None,
    protocol=mqtt.MQTTv311)