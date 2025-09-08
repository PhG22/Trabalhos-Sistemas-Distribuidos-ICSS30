import pika
import json
import time
from datetime import datetime, timedelta

# Lista de leilões pré-configurada
# Os horários são definidos dinamicamente com base no momento em que o script é iniciado.
LEILOES = [
    {
        "id": 1,
        "descricao": "1155 do ET",
        "inicio": datetime.now() + timedelta(seconds=30),
        "fim": datetime.now() + timedelta(minutes=2, seconds=30),
        "status": "pendente",
        "lance_minimo": 10.0,
    },
    {
        "id": 2,
        "descricao": "Carta MTG: Tifa, Martial Artist (Surge Foil)",
        "inicio": datetime.now() + timedelta(seconds=30.5),
        "fim": datetime.now() + timedelta(minutes=2.1, seconds=30.5),
        "status": "pendente",
        "lance_minimo": 100.0,
    },
]


class MS_Auctions:

    def __init__(self, host="localhost", exchange="leilao_iniciado_exchange"):
        try:
            self.host = host
            self.exchange = exchange

            self.connection = pika.BlockingConnection(
                pika.ConnectionParameters("localhost")
            )
            self.channel = self.connection.channel()

            # Declara um exchange do tipo fanout para transmitir os anúncios de início de leilão.
            # Isso garante que todos os clientes e serviços interessados recebam a notificação.
            self.channel.exchange_declare(
                exchange=self.exchange, exchange_type="fanout"
            )

            # A fila para finalizar leilões continua sendo ponto-a-ponto.
            self.channel.queue_declare(queue="leilao_finalizado")

        except pika.exceptions.AMQPConnectionError as e:
            print(
                f"--- [MS_Auctions] Erro: Não foi possível estabelecer conexão com o RabbitMQ em '{self.host}'. ---"
            )
            # Elimininando qualquer ponta solta que possa ter ficado da tentativa mal executada
            if self.connection and self.connection.is_open:
                self.connection.close()
                print("--- [MS_Auctions] Conexão com RabbitMQ encerrada. ---")

    def _start_auction(self, leilao):
        leilao["status"] = "ativo"

        message = {
            "id": leilao["id"],
            "descricao": leilao["descricao"],
            "inicio": leilao["inicio"].isoformat(),
            "fim": leilao["fim"].isoformat(),
            "lance_minimo": leilao["lance_minimo"],
        }

        # Publica a mensagem no exchange, não em uma fila específica.
        # O exchange se encarregará de distribuir para todos os consumidores (clientes e MS Lance).
        self.channel.basic_publish(
            exchange=self.exchange,
            routing_key="",  # routing_key é ignorada em exchanges do tipo fanout
            body=json.dumps(message),
        )
        print(
            f" [MS_Auctions] Leilão {leilao['id']} INICIADO: {leilao['descricao']} (Lance Mínimo: R${leilao['lance_minimo']:.2f})"
        )

    def _end_auction(self, leilao):
        leilao["status"] = "encerrado"

        message = {"id": leilao["id"]}

        self.channel.basic_publish(
            exchange="",
            routing_key="leilao_finalizado",
            body=json.dumps(message),
        )
        print(f" Leilão {leilao['id']} FINALIZADO.")

    def run(self):

        print(
            "--- [MS_Auctions] Monitorando horários... (Pressione Ctrl+C para sair) ---"
        )
        try:
            while True:
                now = datetime.now()
                for leilao in LEILOES:
                    # Inicia o leilão se o tempo for atingido e estiver pendente
                    if leilao["status"] == "pendente" and now >= leilao["inicio"]:
                        self._start_auction(leilao=leilao)
                    # Finaliza o leilão se o tempo expirar e estiver ativo
                    elif leilao["status"] == "ativo" and now >= leilao["fim"]:
                        self._end_auction(leilao=leilao)

                time.sleep(
                    1
                )  # Verifica a cada segundo. Ajuda a evitar sobrecarga do CPU

        except KeyboardInterrupt:
            print("\n--- [MS_Auctions] Encerrando processo. ---")
            if self.connection and self.connection.is_open:
                self.connection.close()
                print("--- [MS_Auctions] Conexão com RabbitMQ encerrada. ---")

        except Exception as e:
            print(
                f"\n--- [MS_Auctions] Processo encerrado inesperadamente. Um erro pode ter acontecido {e} ---"
            )
            if self.connection and self.connection.is_open:
                self.connection.close()
                print("--- [MS_Auctions] Conexão com RabbitMQ encerrada. ---")


if __name__ == "__main__":
    auction_service = MS_Auctions()
    auction_service.run()
