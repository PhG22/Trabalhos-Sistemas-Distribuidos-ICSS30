import pika
import json
import sys
import threading
import os
import time
from crypto_utils import generate_keys, sign_message, get_private_key_path

def rabbitmq_consumer(user_id, subscribed_auctions):
    """Thread para consumir mensagens do RabbitMQ."""
    connection = pika.BlockingConnection(pika.ConnectionParameters('localhost'))
    channel = connection.channel()

    # Declara um exchange do tipo fanout para receber anúncios de leilão.
    # Este tipo de exchange envia uma cópia da mensagem para todas as filas ligadas a ele.
    channel.exchange_declare(exchange='leilao_iniciado_exchange', exchange_type='fanout')

    # Cria uma fila temporária e exclusiva para este cliente.
    # O nome da fila será gerado pelo RabbitMQ (queue='').
    # `exclusive=True` garante que a fila seja deletada quando a conexão for fechada.
    result = channel.queue_declare(queue='', exclusive=True)
    queue_name = result.method.queue

    # Liga a fila temporária ao exchange. Agora, esta fila receberá todas as mensagens
    # publicadas no 'leilao_iniciado_exchange'.
    channel.queue_bind(exchange='leilao_iniciado_exchange', queue=queue_name)

    def callback_leilao_iniciado(ch, method, properties, body):
        leilao = json.loads(body)
        print("\n\n--- NOVO LEILÃO INICIADO ---")
        print(f"  ID: {leilao['id']}")
        print(f"  Descrição: {leilao['descricao']}")
        print(f"  Início: {leilao['inicio']}")
        print(f"  Fim: {leilao['fim']}")
        print("--------------------------\n")
        print("Digite o ID do leilão para dar um lance ou 'sair':")

    # Começa a consumir da fila temporária e exclusiva.
    channel.basic_consume(queue=queue_name, on_message_callback=callback_leilao_iniciado, auto_ack=True)

    # Função para consumir de filas específicas de leilão
    def consume_auction_queue(auction_id):
        auction_queue = f'leilao_{auction_id}'
        channel.queue_declare(queue=auction_queue)
        
        def callback_auction_update(ch, method, properties, body):
            data = json.loads(body)
            print(f"\n\n--- ATUALIZAÇÃO LEILÃO {data['leilao_id']} ---")
            if 'vencedor_id' in data:
                if data['vencedor_id']:
                    print(f"  LEILÃO ENCERRADO! Vencedor: {data['vencedor_id']} com lance de R${data['valor']:.2f}")
                else:
                    print("  LEILÃO ENCERRADO! Sem vencedor.")
            else:
                print(f"  Novo lance de {data['user_id']}: R${data['valor']:.2f}")
            print("----------------------------------\n")
            print("Digite o ID do leilão para dar um lance ou 'sair':")

        channel.basic_consume(queue=auction_queue, on_message_callback=callback_auction_update, auto_ack=True)
        print(f"[INFO] Cliente '{user_id}' inscrito para receber notificações do leilão {auction_id}.")

    # Mantém o controle das filas de leilão que já estão sendo consumidas
    consuming_for = set()

    # Loop para verificar novas assinaturas e consumir
    while True:
        # Cria uma cópia da lista de leilões inscritos para evitar problemas de concorrência
        auctions_to_subscribe = list(subscribed_auctions)
        
        for auction_id in auctions_to_subscribe:
            if auction_id not in consuming_for:
                try:
                    consume_auction_queue(auction_id)
                    # Adiciona o ID do leilão ao conjunto para não se inscrever novamente
                    consuming_for.add(auction_id)
                except Exception as e:
                    print(f"Erro ao se inscrever no leilão {auction_id}: {e}")
        
        connection.process_data_events(time_limit=1) # Processa eventos por 1 segundo
        time.sleep(0.1) # Pequena pausa

def main():
    if len(sys.argv) < 2:
        print("Uso: python client.py <user_id>")
        sys.exit(1)
    
    user_id = sys.argv[1]
    private_key_path = get_private_key_path(user_id)

    if not os.path.exists(private_key_path):
        print(f"Chaves para '{user_id}' não encontradas. Gerando novas chaves...")
        generate_keys(user_id)
        print(f"Chaves geradas. Execute o programa novamente")
        exit()
    
    print(f"--- Cliente de Leilão: {user_id} ---")

    # Lista de leilões que o cliente deu lance
    subscribed_auctions = set()

    # Inicia o consumidor RabbitMQ em um thread separado
    consumer_thread = threading.Thread(target=rabbitmq_consumer, args=(user_id, subscribed_auctions), daemon=True)
    consumer_thread.start()

    # Conexão para publicação
    connection = pika.BlockingConnection(pika.ConnectionParameters('localhost'))
    channel = connection.channel()
    channel.queue_declare(queue='lance_realizado')

    while True:
        try:
            leilao_id_str = input("Digite o ID do leilão para dar um lance ou 'sair': ")
            if leilao_id_str.lower() == 'sair':
                break
            
            leilao_id = int(leilao_id_str)
            valor_str = input(f"Digite o valor do lance para o leilão {leilao_id}: ")
            valor = float(valor_str)

            # Constrói a mensagem a ser assinada
            lance_data = {
                "leilao_id": leilao_id,
                "user_id": user_id,
                "valor": valor
            }
            
            # Assina a mensagem
            assinatura = sign_message(private_key_path, lance_data)
            
            # Constrói o payload final
            payload = {
                **lance_data,
                "assinatura": assinatura
            }
            
            # Publica o lance
            channel.basic_publish(exchange='',
                                  routing_key='lance_realizado',
                                  body=json.dumps(payload))
            
            print(f"Lance de R${valor:.2f} enviado para o leilão {leilao_id}!\n")

            # Registra interesse no leilão para receber notificações
            if leilao_id not in subscribed_auctions:
                subscribed_auctions.add(leilao_id)

        except ValueError:
            print("Entrada inválida. Por favor, insira números válidos.")
        except Exception as e:
            print(f"Ocorreu um erro: {e}")

    connection.close()
    print("Cliente encerrado.")

if __name__ == '__main__':
    main()