import pika
import json
import sys
import os
from crypto_utils import generate_keys, sign_message, get_private_key_path
from message_consumer import MessageConsumer

class AuctionClient:
    """
    A classe principal da aplicação para o cliente do leilão.
    Gerencia a interação com o usuário e a publicação de mensagens.
    """
    def __init__(self, user_id):
        self.user_id = user_id
        self.private_key_path = get_private_key_path(user_id)
        self.subscribed_auctions = set()
        self.host = 'localhost'
        self._ensure_keys()

        # Cria o consumidor de mensagens, mas ainda não o inicia
        self.consumer = MessageConsumer(self.user_id, self.subscribed_auctions)

    def _ensure_keys(self):
        """Verifica se as chaves criptográficas existem para o usuário, gerando-as se não existirem."""
        if not os.path.exists(self.private_key_path):
            print(f"Chaves para '{self.user_id}' não encontradas. Gerando novas chaves...")
            generate_keys(self.user_id)

    def place_bid(self, leilao_id, valor, channel, feedback_queue):
        """Cria, assina e publica uma mensagem de lance para o RabbitMQ."""
        lance_data = {"leilao_id": leilao_id, "user_id": self.user_id, "valor": valor}
        assinatura = sign_message(self.private_key_path, lance_data)
        payload = {**lance_data, "assinatura": assinatura}
        
        properties = pika.BasicProperties(reply_to=feedback_queue)
        channel.basic_publish(exchange='', routing_key='lance_realizado', body=json.dumps(payload), properties=properties)
        
        print(f"Lance de R${valor:.2f} enviado para o leilão {leilao_id}!\n")
        self.subscribed_auctions.add(leilao_id)

    def run(self):
        """Inicia a aplicação do cliente."""
        print(f"--- Cliente de Leilão: {self.user_id} ---")
        
        # Inicia a thread do consumidor
        self.consumer.start()
        # Espera até que o consumidor tenha criado com sucesso sua fila de feedback
        self.consumer.ready_event.wait()
        feedback_queue = self.consumer.feedback_queue_name

        if not feedback_queue:
            print("--- [CLIENTE] Não foi possível iniciar o consumidor de mensagens. Encerrando. ---")
            return
            
        connection = pika.BlockingConnection(pika.ConnectionParameters(self.host))
        channel = connection.channel()
        channel.queue_declare(queue='lance_realizado')

        try:
            while True:
                leilao_id_str = input("Digite o ID do leilão para dar um lance ou 'sair': ")
                if leilao_id_str.lower() == 'sair':
                    break
                
                valor_str = input(f"Digite o valor do lance para o leilão {leilao_id_str}: ")
                
                leilao_id = int(leilao_id_str)
                valor = float(valor_str)

                self.place_bid(leilao_id, valor, channel, feedback_queue)

        except (ValueError, TypeError):
            print("Entrada inválida. Por favor, insira números válidos.")
        except (KeyboardInterrupt, EOFError):
            pass
        finally:
            connection.close()
            print("\nCliente encerrado.")


if __name__ == '__main__':
    if len(sys.argv) < 2:
        print("Uso: python client.py <user_id>")
        sys.exit(1)
    
    client_user_id = sys.argv[1]
    client = AuctionClient(user_id=client_user_id)
    client.run()