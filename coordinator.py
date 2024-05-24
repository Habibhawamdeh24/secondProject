import pika
import sqlite3

connection = pika.BlockingConnection(pika.ConnectionParameters('localhost'))
channel = connection.channel()
channel.queue_declare(queue='tasks')

def get_switches():
    conn = sqlite3.connect('switches.db')
    cursor = conn.cursor()
    cursor.execute("SELECT ip FROM switches")
    switches = cursor.fetchall()
    conn.close()
    return [switch[0] for switch in switches]

def create_tasks():
    switches = get_switches()
    for switch in switches:
        channel.basic_publish(exchange='', routing_key='tasks', body=switch)
    print("Tasks created")

create_tasks()
