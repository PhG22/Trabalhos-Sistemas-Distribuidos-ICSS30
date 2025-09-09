import pika
import json
from crypto_utils import verify_signature, get_public_key_path

leiloes_ativos = {}  # { leilao_id: True/False }
lances_mais_altos = {}  # { leilao_id: {"user_id": str, "valor": float} }


class MSBid:
    def __init__(self, host="localhost", exchange="leilao_iniciado_exchange"):
        try:
            self.host = host
            self.exchange = exchange

            self.connection = pika.BlockingConnection(
                pika.ConnectionParameters("localhost")
            )
            self.channel = self.connection.channel()

            # Inscrição nos anúncios de início de leilão
            self.channel.exchange_declare(
                exchange="leilao_iniciado_exchange", exchange_type="fanout"
            )
            # Cria uma fila exclusiva para o MS Lance receber os anúncios de início
            result = self.channel.queue_declare(queue="", exclusive=True)
            self.queue_name_iniciado = result.method.queue
            self.channel.queue_bind(
                exchange="leilao_iniciado_exchange", queue=self.queue_name_iniciado
            )

            consumidoras = [
                "lance realizado",
                "lance_finalizado",
            ]  # Filas a serem consumidas

            publicadoras = [
                "lance_validado",
                "leilao_vencedor",
            ]  # Flias a serem publicadas

            for queue in (
                consumidoras + publicadoras
            ):  # Concatenando ambas os tipos de serviços para fazer a declaração inicial
                self.channel.queue_declare(queue=queue)

        except pika.exceptions.AMQPConnectionError as e:
            print(
                f"--- [MS_Bid] Erro: Não foi possível estabelecer conexão com o RabbitMQ em '{self.host}'. ---"
            )
            # Elimininando qualquer ponta solta que possa ter ficado da tentativa mal executada
            if self.connection and self.connection.is_open:
                self.connection.close()
                print("--- [MS_Bid] Conexão com RabbitMQ encerrada. ---")

    def callback_leilao_iniciado(self, ch, method, properties, body):
        leilao = json.loads(body)
        leilao_id = leilao["id"]
        leiloes_ativos[leilao_id] = True
        # Armazena o lance mínimo junto com as informações do leilão
        lances_mais_altos[leilao_id] = {
            "user_id": None,
            "valor": 0.0,
            "lance_minimo": leilao.get("lance_minimo", 0.0),
        }
        print(
            f"[MS_Bid] [INFO] Leilão {leilao_id} agora está ATIVO com lance mínimo de R${lances_mais_altos[leilao_id]['lance_minimo']:.2f}."
        )
        ch.basic_ack(delivery_tag=method.delivery_tag)

    def callback_leilao_finalizado(self, ch, method, properties, body):
        data = json.loads(body)
        leilao_id = data["id"]
        if leilao_id in leiloes_ativos:
            leiloes_ativos[leilao_id] = False
            print(
                f"[MS_Bid] [INFO] Leilão {leilao_id} agora está ENCERRADO. Determinando vencedor..."
            )

            vencedor_info = lances_mais_altos.get(leilao_id)

            message = {
                "leilao_id": leilao_id,
                "vencedor_id": vencedor_info["user_id"] if vencedor_info else None,
                "valor": vencedor_info["valor"] if vencedor_info else 0.0,
            }

            self.channel.basic_publish(
                exchange="", routing_key="leilao_vencedor", body=json.dumps(message)
            )
            print(
                f" [MS_Bid] Vencedor do leilão {leilao_id} publicado: {message['vencedor_id']}"
            )
        ch.basic_ack(delivery_tag=method.delivery_tag)

    def callback_lance_realizado(self, ch, method, properties, body):
        reply_to = properties.reply_to

        def send_feedback(motivo):
            if reply_to:
                feedback_message = {"status": "recusado", "motivo": motivo}
                ch.basic_publish(
                    exchange="", routing_key=reply_to, body=json.dumps(feedback_message)
                )
                print(f" Enviado para '{reply_to}': {motivo}")

        lance = json.loads(body)
        leilao_id = lance["leilao_id"]
        user_id = lance["user_id"]
        valor = lance["valor"]

        print(
            f"[MS_Bid] [INFO] Recebido lance de {user_id} para leilão {leilao_id} no valor de R${valor:.2f}"
        )

        mensagem_original = {"leilao_id": leilao_id, "user_id": user_id, "valor": valor}

        public_key_path = get_public_key_path(user_id)
        if not verify_signature(
            public_key_path, mensagem_original, lance["assinatura"]
        ):
            send_feedback("Assinatura digital inválida.")
            ch.basic_ack(delivery_tag=method.delivery_tag)
            return

        if not leiloes_ativos.get(leilao_id, False):
            send_feedback(f"Leilão {leilao_id} não está ativo ou não existe.")
            ch.basic_ack(delivery_tag=method.delivery_tag)
            return

        ultimo_lance_info = lances_mais_altos.get(leilao_id)
        valor_atual = ultimo_lance_info["valor"]
        lance_minimo = ultimo_lance_info["lance_minimo"]

        if valor_atual == 0.0 and valor < lance_minimo:
            motivo = f"Seu lance (R${valor:.2f}) é menor que o lance mínimo inicial (R${lance_minimo:.2f})."
            send_feedback(motivo)
            ch.basic_ack(delivery_tag=method.delivery_tag)
            return

        if valor_atual > 0.0 and valor <= valor_atual:
            motivo = f"Seu lance (R${valor:.2f}) não é maior que o lance atual (R${valor_atual:.2f})."
            send_feedback(motivo)
            ch.basic_ack(delivery_tag=method.delivery_tag)
            return

        print(f" [MS_Bid] Lance de {user_id} para o leilão {leilao_id} é VÁLIDO.")

        lances_mais_altos[leilao_id]["user_id"] = user_id
        lances_mais_altos[leilao_id]["valor"] = valor

        message = {"leilao_id": leilao_id, "user_id": user_id, "valor": valor}
        self.channel.basic_publish(
            exchange="", routing_key="lance_validado", body=json.dumps(message)
        )
        print(f" [MS_Bid] Lance validado publicado para o leilão {leilao_id}.")
        ch.basic_ack(delivery_tag=method.delivery_tag)

    def run(self):

        try:
            self.channel.basic_consume(
                queue=self.queue_name_iniciado,
                on_message_callback=self.callback_leilao_iniciado,
            )
            self.channel.basic_consume(
                queue="leilao_finalizado",
                on_message_callback=self.callback_leilao_finalizado,
            )
            self.channel.basic_consume(
                queue="lance_realizado",
                on_message_callback=self.callback_lance_realizado,
            )

            print("--- [MS_Bid] iniciado. Aguardando eventos... ---")
            self.channel.start_consuming()

        except KeyboardInterrupt:
            print("\n--- [MS_Bid] Encerrando processo. ---")
            if self.connection and self.connection.is_open:
                self.connection.close()
                print("--- [MS_Bid] Conexão com RabbitMQ encerrada. ---")

        except Exception as e:
            print(
                f"\n--- [MS_Bid] Processo encerrado inesperadamente. Um erro pode ter acontecido {e}---"
            )
            if self.connection and self.connection.is_open:
                self.connection.close()
                print("--- [MS_Bid] Conexão com RabbitMQ encerrada. ---")


if __name__ == "__main__":
    bid_service = MSBid()
    bid_service.run()
