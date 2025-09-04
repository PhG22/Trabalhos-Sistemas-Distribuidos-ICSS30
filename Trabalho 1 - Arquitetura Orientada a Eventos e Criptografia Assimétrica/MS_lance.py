# ms_lance.py
import pika
import json
from crypto_utils import verify_signature, get_public_key_path

leiloes_ativos = {} # { leilao_id: True/False }
lances_mais_altos = {} # { leilao_id: {"user_id": str, "valor": float} }

def main():
    connection = pika.BlockingConnection(pika.ConnectionParameters('localhost'))
    channel = connection.channel()

    # Filas que este serviço consome
    channel.queue_declare(queue='lance_realizado')
    channel.queue_declare(queue='leilao_iniciado') # Consome para saber quais leilões estão ativos
    channel.queue_declare(queue='leilao_finalizado')

    # Filas que este serviço publica
    channel.queue_declare(queue='lance_validado')
    channel.queue_declare(queue='leilao_vencedor')

    def callback_leilao_iniciado(ch, method, properties, body):
        leilao = json.loads(body)
        leilao_id = leilao['id']
        leiloes_ativos[leilao_id] = True
        lances_mais_altos[leilao_id] = {"user_id": None, "valor": 0.0}
        print(f"[INFO] Leilão {leilao_id} agora está ATIVO.")
        ch.basic_ack(delivery_tag=method.delivery_tag)

    def callback_leilao_finalizado(ch, method, properties, body):
        data = json.loads(body)
        leilao_id = data['id']
        if leilao_id in leiloes_ativos:
            leiloes_ativos[leilao_id] = False
            print(f"[INFO] Leilão {leilao_id} agora está ENCERRADO. Determinando vencedor...")
            
            vencedor = lances_mais_altos.get(leilao_id)
            
            message = {
                "leilao_id": leilao_id,
                "vencedor_id": vencedor['user_id'] if vencedor else None,
                "valor": vencedor['valor'] if vencedor else 0.0
            }
            
            channel.basic_publish(exchange='',
                                  routing_key='leilao_vencedor',
                                  body=json.dumps(message))
            print(f" Vencedor do leilão {leilao_id} publicado: {message['vencedor_id']}")
        ch.basic_ack(delivery_tag=method.delivery_tag)

    def callback_lance_realizado(ch, method, properties, body):
        lance = json.loads(body)
        leilao_id = lance['leilao_id']
        user_id = lance['user_id']
        valor = lance['valor']
        assinatura = lance['assinatura']
        
        print(f"[INFO] Recebido lance de {user_id} para leilão {leilao_id} no valor de {valor}")

        # 1. Reconstruir a mensagem original para verificação da assinatura
        mensagem_original = {
            "leilao_id": leilao_id,
            "user_id": user_id,
            "valor": valor
        }
        
        # 2. Validar assinatura
        public_key_path = get_public_key_path(user_id)
        assinatura_valida = verify_signature(public_key_path, mensagem_original, assinatura)
        if not assinatura_valida:
            print(f" Assinatura inválida para o lance de {user_id}.")
            ch.basic_ack(delivery_tag=method.delivery_tag)
            return

        # 3. Validar se o leilão está ativo
        if not leiloes_ativos.get(leilao_id, False):
            print(f" Leilão {leilao_id} não está ativo.")
            ch.basic_ack(delivery_tag=method.delivery_tag)
            return
            
        # 4. Validar se o lance é maior que o último
        ultimo_lance = lances_mais_altos.get(leilao_id, {"valor": 0.0})
        if valor <= ultimo_lance["valor"]:
            print(f" Valor do lance (R${valor}) não é maior que o último lance (R${ultimo_lance['valor']}).")
            ch.basic_ack(delivery_tag=method.delivery_tag)
            return

        # Se todas as validações passaram, o lance é válido
        print(f" Lance de {user_id} para o leilão {leilao_id} é válido.")
        
        # Atualiza o estado interno
        lances_mais_altos[leilao_id] = {"user_id": user_id, "valor": valor}
        
        # Publica o evento de lance validado
        message = {
            "leilao_id": leilao_id,
            "user_id": user_id,
            "valor": valor
        }
        channel.basic_publish(exchange='',
                              routing_key='lance_validado',
                              body=json.dumps(message))
        print(f" Lance validado publicado para o leilão {leilao_id}.")
        ch.basic_ack(delivery_tag=method.delivery_tag)

    channel.basic_consume(queue='leilao_iniciado', on_message_callback=callback_leilao_iniciado)
    channel.basic_consume(queue='leilao_finalizado', on_message_callback=callback_leilao_finalizado)
    channel.basic_consume(queue='lance_realizado', on_message_callback=callback_lance_realizado)

    print('--- MS Lance iniciado. Aguardando eventos... ---')
    channel.start_consuming()

if __name__ == '__main__':
    try:
        main()
    except KeyboardInterrupt:
        print('MS Lance encerrado.')