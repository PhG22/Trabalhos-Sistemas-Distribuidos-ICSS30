import pika
import json


class MS_Notifications:

    def __init__(self, host="localhost", exchange="notificacoes_exchange"):

        try:
            self.host = host
            self.exchange = exchange

            self.connection = pika.BlockingConnection(
                pika.ConnectionParameters("localhost")
            )
            self.channel = self.connection.channel()

            # Declara o exchange do tipo 'direct' que será usado para rotear as notificações
            self.channel.exchange_declare(
                exchange=self.exchange, exchange_type="direct"
            )

            # Filas que este serviço consome
            for queue in ["lance_validado", "leilao_vencedor"]:
                self.channel.queue_declare(queue=queue)

        except pika.exceptions.AMQPConnectionError as e:
            print(
                f"--- [MS_Notifications] Erro: Não foi possível estabelecer conexão com o RabbitMQ em '{self.host}'. ---"
            )
            # Elimininando qualquer ponta solta que possa ter ficado da tentativa mal executada
            if self.connection and self.connection.is_open:
                self.connection.close()
                print("--- [MS_Notifications] Conexão com RabbitMQ encerrada. ---")

    def callback_router(self, ch, method, properties, body):
        """Callback que consome eventos e os re-publica no exchange com a routing key correta."""
        try:
            data = json.loads(body)
            leilao_id = data.get("leilao_id")

            if leilao_id is None:
                print(f"\n[MS_Notifications] Mensagem recebida sem 'leilao_id': {body}")
                ch.basic_ack(delivery_tag=method.delivery_tag)
                return

            # A routing key é semanticamente significativa (ex: 'leilao_1')
            routing_key = f"leilao_{leilao_id}"

            # Publica a mensagem no exchange, delegando o roteamento ao broker
            self.channel.basic_publish(
                exchange=self.exchange, routing_key=routing_key, body=body
            )

            print(
                f"\n [MS_Notifications] Mensagem do tipo '{method.routing_key}' roteada para o exchange {self.exchange} com a chave '{routing_key}'"
            )

        except json.JSONDecodeError:
            print(f" Falha ao decodificar JSON: {body}")
        except Exception as e:
            print(f" Erro inesperado: {e}")

        ch.basic_ack(delivery_tag=method.delivery_tag)

    def run(self):

        try:

            # Usa o mesmo callback para ambas as filas de entrada
            self.channel.basic_consume(
                queue="lance_validado", on_message_callback=self.callback_router
            )
            self.channel.basic_consume(
                queue="leilao_vencedor", on_message_callback=self.callback_router
            )

            print(
                "--- [MS_Notifications] iniciado. Aguardando mensagens para rotear... ---"
            )
            self.channel.start_consuming()

        except KeyboardInterrupt:
            print("\n--- [MS_Notifications] Encerrando processo. ---")
            if self.connection and self.connection.is_open:
                self.connection.close()
                print("--- [MS_Notifications] Conexão com RabbitMQ encerrada. ---")

        except Exception as e:
            print(
                f"\n--- [MS_Notifications] Processo encerrado inesperadamente. Um erro pode ter acontecido {e} ---"
            )
            if self.connection and self.connection.is_open:
                self.connection.close()
                print("--- [MS_Notifications] Conexão com RabbitMQ encerrada. ---")


if __name__ == "__main__":
    notifications_service = MS_Notifications()
    notifications_service.run()
