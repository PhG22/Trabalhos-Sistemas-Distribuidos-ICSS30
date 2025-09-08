import pika
import json

def main():
    connection = pika.BlockingConnection(pika.ConnectionParameters('localhost'))
    channel = connection.channel()

    # Declara o exchange do tipo 'direct' que será usado para rotear as notificações
    channel.exchange_declare(exchange='notificacoes_exchange', exchange_type='direct')

    # Filas que este serviço consome
    channel.queue_declare(queue='lance_validado')
    channel.queue_declare(queue='leilao_vencedor')

    def callback_router(ch, method, properties, body):
        """Callback que consome eventos e os re-publica no exchange com a routing key correta."""
        try:
            data = json.loads(body)
            leilao_id = data.get('leilao_id')

            if leilao_id is None:
                print(f" Mensagem recebida sem 'leilao_id': {body}")
                ch.basic_ack(delivery_tag=method.delivery_tag)
                return

            # A routing key é semanticamente significativa (ex: 'leilao_1')
            routing_key = f'leilao_{leilao_id}'
            
            # Publica a mensagem no exchange, delegando o roteamento ao broker
            channel.basic_publish(
                exchange='notificacoes_exchange',
                routing_key=routing_key,
                body=body
            )
            
            print(f" Mensagem do tipo '{method.routing_key}' roteada para o exchange com a chave '{routing_key}'")

        except json.JSONDecodeError:
            print(f" Falha ao decodificar JSON: {body}")
        except Exception as e:
            print(f" Erro inesperado: {e}")
        
        ch.basic_ack(delivery_tag=method.delivery_tag)

    # Usa o mesmo callback para ambas as filas de entrada
    channel.basic_consume(queue='lance_validado', on_message_callback=callback_router)
    channel.basic_consume(queue='leilao_vencedor', on_message_callback=callback_router)

    print('--- MS Notificação iniciado. Aguardando mensagens para rotear... ---')
    channel.start_consuming()

if __name__ == '__main__':
    try:
        main()
    except KeyboardInterrupt:
        print('MS Notificação encerrado.')