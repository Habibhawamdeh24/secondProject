import threading
import pika
from pysnmp.hlapi import *
from influxdb import InfluxDBClient

# RabbitMQ connection
connection = pika.BlockingConnection(pika.ConnectionParameters('localhost'))
channel = connection.channel()
channel.queue_declare(queue='tasks')

influx_client = InfluxDBClient(host='localhost', port=8086)
influx_client.switch_database('temperature_db')

def collect_temperature(switch_ip):
    iterator = getCmd(
        SnmpEngine(),
        CommunityData('public', mpModel=0),
        UdpTransportTarget((switch_ip, 161)),
        ContextData(),
        ObjectType(ObjectIdentity('CISCO-ENTITY-SENSOR-MIB', 'entSensorValue', 1))
    )

    errorIndication, errorStatus, errorIndex, varBinds = next(iterator)

    if errorIndication:
        print(errorIndication)
    elif errorStatus:
        print(f'{errorStatus.prettyPrint()} at {errorIndex and varBinds[int(errorIndex) - 1][0] or "?"}')
    else:
        for varBind in varBinds:
            value = varBind[1]
            json_body = [
                {
                    "measurement": "temperature",
                    "tags": {
                        "host": switch_ip
                    },
                    "fields": {
                        "value": float(value)
                    }
                }
            ]
            influx_client.write_points(json_body)
            print(f"Temperature data written to InfluxDB for {switch_ip}")

def process_task(ch, method, properties, body):
    switch_ip = body.decode()
    collect_temperature(switch_ip)
    ch.basic_ack(delivery_tag=method.delivery_tag)

channel.basic_qos(prefetch_count=1)
channel.basic_consume(queue='tasks', on_message_callback=process_task)

print("Waiting for tasks. To exit press CTRL+C")
channel.start_consuming()
