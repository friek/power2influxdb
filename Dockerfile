FROM python:3.8

COPY src/requirements.txt /tmp
RUN pip install -r /tmp/requirements.txt

ADD src/ /application/

ENV MQTT_HOST=localhost
ENV MQTT_PORT=1883
ENV ENERGY_TOPIC=localhost/energy
ENV INFLUXDB_HOST=localhost
ENV INFLUXDB_DB=energy
ENV INFLUXDB_PORT=8086

CMD ["/usr/local/bin/python", "/application/listener.py"]