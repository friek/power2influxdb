import os

TOPIC = os.getenv("ENERGY_TOPIC", '')
MQTT_HOST = os.getenv("MQTT_HOST", '')
MQTT_PORT = int(os.getenv("MQTT_PORT", 1883))
MQTT_KEEPALIVE = int(os.getenv("MQTT_KEEPALIVE", 60))
INFLUXDB_HOST = os.getenv("INFLUXDB_HOST")
INFLUXDB_DB = os.getenv("INFLUXDB_DB", "energy")
INFLUXDB_PORT = int(os.getenv("INFLUXDB_PORT", 8086))
INFLUXDB_URL = f"http://{INFLUXDB_HOST}:{INFLUXDB_PORT}/write?db={INFLUXDB_DB}"