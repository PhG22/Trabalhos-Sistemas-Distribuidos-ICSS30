import pika
import sys
from uuid import uuid4, UUID

import json

from Crypto.Signature import pkcs1_15
from Crypto.Hash import SHA256
from Crypto.PublicKey import RSA


def callback(ch, method, properties, body):
    print(f" [x] {method.routing_key}:{body}")


class Client:
    def __init__(self, id: str):
        self.user_id = id

        connection = pika.BlockingConnection(
            pika.ConnectionParameters(host="localhost")
        )

        # fila leilao_iniciado
        self.canal_iniciado = connection.channel()

        self.canal_iniciado.exchange_declare(
            exchange="leilao_iniciado", exchange_type="direct"
        )

        result_iniciado = self.canal_iniciado.queue_declare(
            queue="leilao_iniciado", exclusive=True
        )
        queue_name_iniciado = result_iniciado.method.queue
        self.canal_iniciado.queue_bind(
            exchange="leilao_iniciado",
            queue=queue_name_iniciado,
            routing_key="iniciado",
        )

        self.canal_iniciado.basic_consume(
            queue=queue_name_iniciado, on_message_callback=callback, auto_ack=True
        )

        # fila lance_realizado
        self.canal_lances = connection.channel()

        self.canal_lances.exchange_declare(
            exchange="lance_realizado", exchange_type="direct"
        )

        result_lance = self.canal_lances.queue_declare(
            queue="lance_realizado", exclusive=True
        )
        queue_name_lance = result_lance.method.queue
        self.canal_lances.queue_bind(
            exchange="lance_realizado", queue=queue_name_lance, routing_key="lance"
        )

    def make_bid(
        self,
        auction_id: str,
        bid_value: int,
        user_id: UUID,
    ):
        bid_data = json.dumps(
            {"auction_id": auction_id, "user_id": user_id, "value": bid_value}
        ).encode("utf-8")

        user_private_key = RSA.import_key(
            open(f"private_keys/{user_id}_priv.pem").read()
        )

        bid_hash = SHA256.new(bid_data)

        signature = pkcs1_15.new(user_private_key).sign(bid_hash)

        final_message = {"message_body": bid_data, "message_signature": signature}

        self.canal_lances.basic_publish(
            exchange="lance realizado", routing_key="lance", body=json.dumps(final_message)
        )
