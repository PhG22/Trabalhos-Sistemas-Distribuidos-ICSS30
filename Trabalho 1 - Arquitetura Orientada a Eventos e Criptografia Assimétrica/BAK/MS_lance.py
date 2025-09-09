import pika
import json
from crypto_utils import verify_signature, get_public_key_path

leiloes_ativos = {} #{ leilao_id: True/False }
lances_mais_altos = {} #{ leilao_id: {"user_id": str, "valor": float} }

def main():
    connection = pika.BlockingConnection(pika.ConnectionParameters('localhost'))
    channel = connection.channel()

    # Inscrição nos anúncios de início de leilão
    channel.exchange_declare(exchange='leilao_iniciado_exchange', exchange_type='fanout')
     # Cria uma fila exclusiva para o MS Lance receber os anúncios de início
    result = channel.queue_declare(queue='', exclusive=True)
    queue_name_iniciado = result.method.queue
    channel.queue_bind(exchange='leilao_iniciado_exchange', queue=queue_name_iniciado)
    
    # Filas que este serviço consome
    channel.queue_declare(queue='lance_realizado')
    channel.queue_declare(queue='leilao_finalizado')

    # Filas que este serviço publica
    channel.queue_declare(queue='lance_validado')
    channel.queue_declare(queue='leilao_vencedor')

    def callback_leilao_iniciado(ch, method, properties, body):
        leilao = json.loads(body)
        leilao_id = leilao['id']
        leiloes_ativos[leilao_id] = True
        # Armazena o lance mínimo junto com as informações do leilão
        lances_mais_altos[leilao_id] = {
            "user_id": None,
            "valor": 0.0,
            "lance_minimo": leilao.get('lance_minimo', 0.0)
        }
        print(f"[INFO] Leilão {leilao_id} agora está ATIVO com lance mínimo de R${lances_mais_altos[leilao_id]['lance_minimo']:.2f}.")
        ch.basic_ack(delivery_tag=method.delivery_tag)

    def callback_leilao_finalizado(ch, method, properties, body):
        data = json.loads(body)
        leilao_id = data['id']
        if leilao_id in leiloes_ativos:
            leiloes_ativos[leilao_id] = False
            print(f"[INFO] Leilão {leilao_id} agora está ENCERRADO. Determinando vencedor...")
            
            vencedor_info = lances_mais_altos.get(leilao_id)
            
            message = {
                "leilao_id": leilao_id,
                "vencedor_id": vencedor_info['user_id'] if vencedor_info else None,
                "valor": vencedor_info['valor'] if vencedor_info else 0.0
            }
            
            channel.basic_publish(exchange='', routing_key='leilao_vencedor', body=json.dumps(message))
            print(f" Vencedor do leilão {leilao_id} publicado: {message['vencedor_id']}")
        ch.basic_ack(delivery_tag=method.delivery_tag)

    def callback_lance_realizado(ch, method, properties, body):
        reply_to = properties.reply_to
        
        def send_feedback(motivo):
            if reply_to:
                feedback_message = {"status": "recusado", "motivo": motivo}
                ch.basic_publish(exchange='', routing_key=reply_to, body=json.dumps(feedback_message))
                print(f" Enviado para '{reply_to}': {motivo}")

        lance = json.loads(body)
        leilao_id = lance['leilao_id']
        user_id = lance['user_id']
        valor = lance['valor']
        
        print(f"[INFO] Recebido lance de {user_id} para leilão {leilao_id} no valor de R${valor:.2f}")

        mensagem_original = {"leilao_id": leilao_id, "user_id": user_id, "valor": valor}
        
        public_key_path = get_public_key_path(user_id)
        if not verify_signature(public_key_path, mensagem_original, lance['assinatura']):
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

        print(f" Lance de {user_id} para o leilão {leilao_id} é VÁLIDO.")
        
        lances_mais_altos[leilao_id]["user_id"] = user_id
        lances_mais_altos[leilao_id]["valor"] = valor
        
        message = {"leilao_id": leilao_id, "user_id": user_id, "valor": valor}
        channel.basic_publish(exchange='', routing_key='lance_validado', body=json.dumps(message))
        print(f" Lance validado publicado para o leilão {leilao_id}.")
        ch.basic_ack(delivery_tag=method.delivery_tag)

    channel.basic_consume(queue=queue_name_iniciado, on_message_callback=callback_leilao_iniciado)
    channel.basic_consume(queue='leilao_finalizado', on_message_callback=callback_leilao_finalizado)
    channel.basic_consume(queue='lance_realizado', on_message_callback=callback_lance_realizado)

    print('--- MS Lance iniciado. Aguardando eventos... ---')
    channel.start_consuming()

if __name__ == '__main__':
    try:
        main()
    except KeyboardInterrupt:
        print('MS Lance encerrado.')