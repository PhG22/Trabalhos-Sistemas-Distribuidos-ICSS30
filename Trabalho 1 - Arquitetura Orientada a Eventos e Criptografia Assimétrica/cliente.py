import pika
import sys

def callback(ch, method, properties, body):
    print(f" [x] {method.routing_key}:{body}")


class Client:
    def __init__(self):
        connection = pika.BlockingConnection(
            pika.ConnectionParameters(host='localhost'))
        channel = connection.channel()

        channel.exchange_declare(exchange='direct_logs', exchange_type='direct')

        result = channel.queue_declare(queue='leilao_iniciado', exclusive=True)
        queue_name = result.method.queue
        channel.queue_bind(
        exchange='direct_logs', queue=queue_name, routing_key="")

        channel.basic_consume(
            queue=queue_name, on_message_callback=callback, auto_ack=True)
        

    def make_bid(self):
        pass
