#!/usr/bin/env python3

import json
from datetime import datetime

import paho.mqtt.client as mqtt
import requests

from settings import MQTT_HOST, MQTT_PORT, MQTT_KEEPALIVE, TOPIC, INFLUXDB_URL


# The callback for when the client receives a CONNACK response from the server.
# def on_connect(client, userdata, flags, rc):
def on_connect(*args):
    print("Connected")
    if len(args) >= 3:
        print(f"Connected with result code {args[3]}")

    # Subscribing in on_connect() means that if we lose the connection and
    # reconnect then subscriptions will be renewed.
    result, message_id = client.subscribe(TOPIC)
    if result != mqtt.MQTT_ERR_SUCCESS:
        raise Exception("Subscribe failed")
    else:
        print(f"Subscribed to {TOPIC} with message id {message_id}")


def send_influx_data(meter, value, table, timestamp=None):
    if timestamp is None:
        t = datetime.now()
        # Round to 10 seconds
        timestamp = t.timestamp()
        timestamp = timestamp - (timestamp % 10)
    else:
        t = datetime.fromisoformat(timestamp)
        timestamp = t.timestamp()

    # Convert to string
    timestamp = '%s%09d' % (str(int(timestamp)), 0)

    post_data = '{0},meter={1} value={2} {3}'.format(table, meter, value, timestamp)
    print(f"table={table}, meter={meter}, value={value}, timestamp={timestamp}, url={INFLUXDB_URL}")
    requests.post(INFLUXDB_URL, data=post_data)


def on_message(*args):
    if len(args) < 3:
        return
    msg: mqtt.MQTTMessage = args[2]
    energy_info = json.loads(s=msg.payload)

    power_meter_time = energy_info.get('meter_time', None)
    gas_meter_time = energy_info.get('gas_last_measurement', None)

    # Power usage
    current_power = energy_info.get('instantaneous_active_power_draw_l1', None)
    if current_power is not None:
        send_influx_data('current', value=current_power, timestamp=power_meter_time, table='current_usage')
    # Solar panels
    current_power_delivery = energy_info.get('instantaneous_active_power_delivery_l1', None)
    if current_power_delivery is not None:
        send_influx_data('current', value=current_power_delivery, timestamp=power_meter_time, table='current_delivery')

    # Consumed power meters. meter1 is low (night) tariff, meter2 is high (day) tariff.
    send_influx_data('meter1', value=energy_info.get('total_usage_night'), timestamp=power_meter_time, table='usage')
    send_influx_data('meter2', value=energy_info.get('total_usage_day'), timestamp=power_meter_time, table='usage')
    # Generated power meters
    send_influx_data('meter1', value=energy_info.get('total_energy_delivered_night'),
                     timestamp=power_meter_time, table='generated')
    send_influx_data('meter2', value=energy_info.get('total_energy_delivered_day'),
                     timestamp=power_meter_time, table='generated')
    # Gas usage.
    send_influx_data('gas', value=energy_info.get('gas_usage_total'), timestamp=gas_meter_time, table='usage')


def validate_env(env_key, value, expected_type):
    if value is None or not isinstance(value, expected_type):
        raise ValueError(f"Expected environment {env_key} to have a value of type {expected_type}. Value is '{value}'")


if __name__ == '__main__':
    validate_env('ENERGY_TOPIC', TOPIC, str)
    validate_env('MQTT_HOST', MQTT_HOST, str)
    validate_env('MQTT_PORT', MQTT_PORT, int)
    validate_env('MQTT_KEEPALIVE', MQTT_KEEPALIVE, int)

    client = mqtt.Client()
    client.on_connect = on_connect
    client.on_message = on_message

    client.connect(host=MQTT_HOST, port=MQTT_PORT, keepalive=MQTT_KEEPALIVE)

    # Blocking call that processes network traffic, dispatches callbacks and handles reconnecting.
    # Other loop*() functions are available that give a threaded interface and a manual interface.
    client.loop_forever()
