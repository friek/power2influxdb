#!/usr/bin/env python3

import json
from datetime import datetime
import logging

import paho.mqtt.client as mqtt
import requests
from influxdb import InfluxDBClient
import pendulum

import settings

LOGGER = logging.getLogger(__name__)
INFLUX_CLIENT = InfluxDBClient(host=settings.INFLUXDB_HOST, port=settings.INFLUXDB_PORT, database=settings.INFLUXDB_DB)
prev_totals = {
    'used': None,  # type: int
    'exported': None,  # type: int
    'timestamp': None,  # type: pendulum.DateTime
}


# The callback for when the client receives a CONNACK response from the server.
# def on_connect(client, userdata, flags, rc):
def on_connect(*args):
    LOGGER.info("Connected")
    if len(args) >= 3:
        LOGGER.info(f"Connected with result code {args[3]}")

    # Subscribing in on_connect() means that if we lose the connection and
    # reconnect then subscriptions will be renewed.
    result, message_id = client.subscribe(settings.TOPIC)
    if result != mqtt.MQTT_ERR_SUCCESS:
        raise Exception("Subscribe failed")
    else:
        LOGGER.info(f"Subscribed to {settings.TOPIC} with message id {message_id}")


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
    LOGGER.info(f"table={table}, meter={meter}, value={value}, timestamp={timestamp}, url={settings.INFLUXDB_URL}")
    requests.post(settings.INFLUXDB_URL, data=post_data)


def on_message(*args):
    if len(args) < 3:
        return
    msg: mqtt.MQTTMessage = args[2]
    energy_info = json.loads(s=msg.payload)

    power_meter_time = energy_info.get('meter_time', None)
    pmt_p = pendulum.parse(power_meter_time)
    gas_meter_time = energy_info.get('gas_last_measurement', None)

    fields = {
        'current_power': None,
        'current_export': None,
        'meter1_total_usage': energy_info.get('total_usage_night'),
        'meter2_total_usage': energy_info.get('total_usage_day'),
        'meter1_total_exported': energy_info.get('total_energy_delivered_night'),
        'meter2_total_exported': energy_info.get('total_energy_delivered_day'),
        'gas_usage_total': energy_info.get('gas_usage_total'),
        'power_exported': 0,
        'power_used': 0,
    }
    points = [
        {
            "measurement": "readings",
            "time": power_meter_time,
            "fields": fields
        }
    ]

    # Power usage
    current_power = energy_info.get('instantaneous_active_power_draw_l1', None)
    if current_power is not None:
        send_influx_data('current', value=current_power, timestamp=power_meter_time, table='current_usage')
        fields['current_power'] = int(current_power * 1000)
    # Solar panels
    current_power_delivery = energy_info.get('instantaneous_active_power_delivery_l1', None)
    if current_power_delivery is not None:
        send_influx_data('current', value=current_power_delivery, timestamp=power_meter_time, table='current_delivery')
        fields['current_export'] = int(current_power_delivery * 1000)

    # Consumed power meters. meter1 is low (night) tariff, meter2 is high (day) tariff.
    send_influx_data('meter1', value=fields['meter1_total_usage'], timestamp=power_meter_time, table='usage')
    send_influx_data('meter2', value=fields['meter2_total_usage'], timestamp=power_meter_time, table='usage')
    # Generated power meters
    send_influx_data('meter1', value=fields['meter1_total_exported'], timestamp=power_meter_time, table='generated')
    send_influx_data('meter2', value=fields['meter2_total_exported'], timestamp=power_meter_time, table='generated')
    # Gas usage.
    send_influx_data('gas', value=fields['gas_usage_total'], timestamp=gas_meter_time, table='usage')

    # This is a workaround for a meter reading failure. Sometimes, one of the meter reports 0 all of
    # a sudden, while the meter can never decrease in value. Only calculate totals when both meters are
    # above 0.
    if fields['meter1_total_usage'] > 0 and fields['meter2_total_usage'] > 0:
        total_used = int(fields['meter1_total_usage'] * 1000) + int(fields['meter2_total_usage'] * 1000)
    else:
        total_used = prev_totals['used']
        LOGGER.error(f"Not calculating usage totals as one of the meters is 0 "
                     f"(meter1={fields['meter1_total_usage']}, meter2={fields['meter2_total_usage']})")
    if fields['meter1_total_exported'] > 0 and fields['meter2_total_exported'] > 0:
        total_exported = int(fields['meter1_total_exported'] * 1000) + int(fields['meter2_total_exported'] * 1000)
    else:
        total_exported = prev_totals['exported']
        LOGGER.error(f"Not calculating export totals as one of the meters is 0 "
                     f"(meter1={fields['meter1_total_exported']}, meter2={fields['meter2_total_exported']})")

    # In the first run, prev_totals['used'] and 'exported' are both None. In that case, it is not possible to
    # calculate the totals since the last reading. It is assumed that there are no interruptions in the broadcasting
    # of the measurements. If there are, the totals calculation will be skewed.
    if prev_totals['used'] is not None and prev_totals['exported'] is not None and prev_totals['timestamp']:
        # noinspection PyUnresolvedReferences
        delay = (pmt_p - prev_totals['timestamp']).seconds
        if delay > settings.MAX_DELAY:
            fields['power_exported'] = fields['power_used'] = 0
            LOGGER.error(f"Not calculating used and exported, as there's a delay of more than {settings.MAX_DELAY} "
                         f"seconds since the last reading (delay={delay} seconds)")
        else:
            # noinspection PyTypeChecker
            fields['power_exported'] = total_exported - prev_totals['exported']
            # noinspection PyTypeChecker
            fields['power_used'] = total_used - prev_totals['used']

    INFLUX_CLIENT.write_points(points)

    prev_totals['used'] = total_used
    prev_totals['exported'] = total_exported
    # noinspection PyTypeChecker
    prev_totals['timestamp'] = pmt_p


def validate_env(env_key, value, expected_type):
    if value is None or not isinstance(value, expected_type):
        raise ValueError(f"Expected environment {env_key} to have a value of type {expected_type}. Value is '{value}'")


if __name__ == '__main__':
    validate_env('ENERGY_TOPIC', settings.TOPIC, str)
    validate_env('MQTT_HOST', settings.MQTT_HOST, str)
    validate_env('MQTT_PORT', settings.MQTT_PORT, int)
    validate_env('MQTT_KEEPALIVE', settings.MQTT_KEEPALIVE, int)

    if settings.DEBUG:
        min_level = logging.DEBUG
    else:
        min_level = logging.INFO

    logging.basicConfig(format='%(asctime)-15s %(name)s %(message)s', level=min_level)

    client = mqtt.Client()
    client.on_connect = on_connect
    client.on_message = on_message

    client.connect(host=settings.MQTT_HOST, port=settings.MQTT_PORT, keepalive=settings.MQTT_KEEPALIVE)

    # Blocking call that processes network traffic, dispatches callbacks and handles reconnecting.
    # Other loop*() functions are available that give a threaded interface and a manual interface.
    client.loop_forever()
