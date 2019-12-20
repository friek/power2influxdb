import os

TOPIC = os.getenv("ENERGY_TOPIC", '')
MQTT_HOST = os.getenv("MQTT_HOST", '')
MQTT_PORT = int(os.getenv("MQTT_PORT", 1883))
MQTT_KEEPALIVE = int(os.getenv("MQTT_KEEPALIVE", 60))
