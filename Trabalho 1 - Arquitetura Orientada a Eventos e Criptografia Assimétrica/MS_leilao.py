import pika
import sys
from datetime import datetime
import json


auctions = [
    {
        "id": "auction_1",
        "descrição": "Leilão de uma cara de magic da coleção do final fantasy -> Buster Sword",
        "início": datetime(2025, 9, 4, 00, 00, 00),
        "fim": datetime(2025, 9, 5, 00, 00, 00),
        "lance_minimo": 100,
    },
    {
        "id": "auction_2",
        "descrição": "Leilão de uma cara de magic da coleção do final fantasy -> Tifa Lockhart",
        "início": datetime(2025, 9, 4, 00, 00, 00),
        "fim": datetime(2025, 9, 5, 00, 00, 00),
        "lance_minimo": 200,
    },
    {
        "id": "auction_3",
        "descrição": "Leilão de uma cara de magic da coleção do final fantasy -> Cloud Strife",
        "início": datetime(2025, 9, 4, 00, 00, 00),
        "fim": datetime(2025, 9, 5, 00, 00, 00),
        "lance_minimo": 300,
    },
]

auctions_started = []

auctions_finished = []


def callback(ch, method, properties, body):
    print(f" [x] {method.routing_key}:{body}")


class MSLeilao:
    def __init__(self):
        connection = pika.BlockingConnection(
            pika.ConnectionParameters(host="localhost")
        )

        # fila leilao_iniciado
        self.channel_iniciado = connection.channel()

        self.channel_iniciado.exchange_declare(
            exchange="leilao_iniciado", exchange_type="direct"
        )

        result_iniciado = self.channel_iniciado.queue_declare(
            queue="leilao_iniciado", exclusive=True
        )
        queue_name_iniciado = result_iniciado.method.queue
        self.channel_iniciado.queue_bind(
            exchange="leilao_iniciado", queue=queue_name_iniciado, routing_key="iniciado"
        )

        self.channel_iniciado.basic_publish(
            exchange="", routing_key="", body=json.dumps(auctions_started)
        )

        # fila leilao_finalizado
        self.channel_finalizado = connection.channel()

        self.channel_finalizado.exchange_declare(
            exchange="leilao_finalizado", exchange_type="direct"
        )

        result_finalizado = self.channel_finalizado.queue_declare(
            queue="leilao_finalizado", exclusive=True
        )
        queue_name_finalizado = result_finalizado.method.queue
        self.channel_finalizado.queue_bind(
            exchange="leilao_finalizado", queue=queue_name_finalizado, routing_key="finalizado"
        )

        self.channel_finalizado.basic_publish(
            exchange="", routing_key="", body=json.dumps(auctions_finished)
        )


    def start_auction(self):
        now = datetime.now()
        for auction in auctions:
            if auction not in auctions_started:
                if now > auction.get("inicio") and now < auction.get("fim"):
                    auctions_started.append(auction)

        self.channel_iniciado.basic_publish(
            exchange="leilao_iniciado",
            routing_key="iniciado",
            body=json.dumps(auctions_started),
        )

    def finish_auction(self):
        now = datetime.now()
        for auction in auctions:
            if auction not in auctions_finished:
                if now > auction.get("fim"):
                    auctions_finished.append(auction)

        self.channel_iniciado.basic_publish(
            exchange="leilao_iniciado",
            routing_key="finalizado",
            body=json.dumps(auctions_finished),
        )
