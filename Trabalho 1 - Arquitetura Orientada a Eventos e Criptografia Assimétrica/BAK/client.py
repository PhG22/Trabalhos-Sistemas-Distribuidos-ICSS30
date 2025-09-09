import pika
import json
import sys
import threading
import os
import time
import uuid
from crypto_utils import generate_keys, sign_message, get_private_key_path

def rabbitmq_consumer(user_id, subscribed_auctions, feedback_queue_holder):
    """Thread para consumir mensagens do RabbitMQ."""
    connection = pika.BlockingConnection(pika.ConnectionParameters('localhost'))
    channel = connection.channel()

    # Configuração para anúncios de início de leilão (Broadcast)
    channel.exchange_declare(exchange='leilao_iniciado_exchange', exchange_type='fanout')
    result_fanout = channel.queue_declare(queue='', exclusive=True)
    queue_name_fanout = result_fanout.method.queue
    channel.queue_bind(exchange='leilao_iniciado_exchange', queue=queue_name_fanout)

    def callback_leilao_iniciado(ch, method, properties, body):
        leilao = json.loads(body)
        print("\n\n--- NOVO LEILÃO INICIADO ---")
        print(f"  ID: {leilao['id']} | Descrição: {leilao['descricao']}")
        print(f"  Lance Mínimo: R${leilao.get('lance_minimo', 0.0):.2f}")
        print(f"  Início: {leilao['inicio']} | Fim: {leilao['fim']}")
        print("--------------------------\n")
        print("Digite o ID do leilão para dar um lance ou 'sair':", end=' ', flush=True)

    channel.basic_consume(queue=queue_name_fanout, on_message_callback=callback_leilao_iniciado, auto_ack=True)

    # Configuração para notificações específicas de leilão (Direcionado)
    channel.exchange_declare(exchange='notificacoes_exchange', exchange_type='direct')
    result_direct = channel.queue_declare(queue='', exclusive=True)
    queue_name_direct = result_direct.method.queue

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
        print("Digite o ID do leilão para dar um lance ou 'sair':", end=' ', flush=True)

    channel.basic_consume(queue=queue_name_direct, on_message_callback=callback_auction_update, auto_ack=True)

    # Configuração da fila de feedback (RPC Reply-to)
    feedback_queue_name = f'feedback_{user_id}_{uuid.uuid4().hex}'
    channel.queue_declare(queue=feedback_queue_name, exclusive=True)
    feedback_queue_holder.append(feedback_queue_name)

    def callback_feedback(ch, method, properties, body):
        feedback = json.loads(body)
        print(f"\n\n--- FEEDBACK DO LANCE ---")
        print(f"  Status: {feedback['status'].upper()}")
        print(f"  Motivo: {feedback['motivo']}")
        print("-------------------------\n")
        print("Digite o ID do leilão para dar um lance ou 'sair':", end=' ', flush=True)
    
    channel.basic_consume(queue=feedback_queue_name, on_message_callback=callback_feedback, auto_ack=True)

    consuming_for = set()
    while True:
        for auction_id in list(subscribed_auctions):
            if auction_id not in consuming_for:
                routing_key = f'leilao_{auction_id}'
                channel.queue_bind(exchange='notificacoes_exchange', queue=queue_name_direct, routing_key=routing_key)
                consuming_for.add(auction_id)
                print(f"\n[INFO] Inscrito para receber notificações do leilão {auction_id}.")
                print("Digite o ID do leilão para dar um lance ou 'sair':", end=' ', flush=True)
        
        connection.process_data_events(time_limit=1)
        time.sleep(0.1)

def main():
    if len(sys.argv) < 2:
        print("Uso: python client.py <user_id>")
        sys.exit(1)
    
    user_id = sys.argv[1]
    private_key_path = get_private_key_path(user_id)

    if not os.path.exists(private_key_path):
        print(f"Chaves para '{user_id}' não encontradas. Gerando novas chaves...")
        generate_keys(user_id)
    
    print(f"--- Cliente de Leilão: {user_id} ---")

    subscribed_auctions = set()
    feedback_queue_holder = []
    consumer_thread = threading.Thread(target=rabbitmq_consumer, args=(user_id, subscribed_auctions, feedback_queue_holder), daemon=True)
    consumer_thread.start()

    # Espera o thread consumidor criar a fila de feedback e passar o nome
    while not feedback_queue_holder:
        time.sleep(0.1)
    feedback_queue_name = feedback_queue_holder[0]
    
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

            lance_data = {"leilao_id": leilao_id, "user_id": user_id, "valor": valor}
            assinatura = sign_message(private_key_path, lance_data)
            payload = {**lance_data, "assinatura": assinatura}
            
            # Envia o lance com a propriedade 'reply_to' para receber feedback
            properties = pika.BasicProperties(reply_to=feedback_queue_name)
            channel.basic_publish(exchange='', routing_key='lance_realizado', body=json.dumps(payload), properties=properties)
            
            print(f"Lance de R${valor:.2f} enviado para o leilão {leilao_id}!\n")

            if leilao_id not in subscribed_auctions:
                subscribed_auctions.add(leilao_id)

        except ValueError:
            print("Entrada inválida. Por favor, insira números válidos.")
        except (KeyboardInterrupt, EOFError):
            break
        except Exception as e:
            print(f"Ocorreu um erro: {e}")

    connection.close()
    print("\nCliente encerrado.")

if __name__ == '__main__':
    main()