FROM python:3.8

COPY src/requirements.txt /tmp
RUN pip install -r /tmp/requirements.txt

ADD src/ /application/

ENV MQTT_HOST=localhost
ENV MQTT_PORT=1883
ENV ENERGY_TOPIC=localhost/energy

CMD ["/usr/local/bin/python", "/application/listener.py"]