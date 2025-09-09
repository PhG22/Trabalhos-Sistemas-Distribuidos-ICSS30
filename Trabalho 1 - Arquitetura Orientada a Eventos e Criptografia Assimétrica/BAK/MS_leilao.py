import pika
import json
import time
from datetime import datetime, timedelta

# Lista de leilões pré-configurada
# Os horários são definidos dinamicamente com base no momento em que o script é iniciado.
LEILOES = [
            {"id": 1,
            "descricao": "1155 do ET",
            "inicio": datetime.now() + timedelta(seconds= 3),
            "fim": datetime.now() + timedelta(minutes = 2, seconds = 3),
            "status": "pendente",
            "lance_minimo": 10.0},
            {"id": 2,
            "descricao": "Carta MTG: Tifa, Martial Artist (Surge Foil)",
            "inicio": datetime.now() + timedelta(seconds= 3.5),
            "fim": datetime.now() + timedelta(minutes = 2.1, seconds= 3.5),
            "status": "pendente",
            "lance_minimo": 100.0}
            ]

def main():
    connection = pika.BlockingConnection(pika.ConnectionParameters('localhost'))
    channel = connection.channel()

    # Declara um exchange do tipo fanout para transmitir os anúncios de início de leilão.
    # Isso garante que todos os clientes e serviços interessados recebam a notificação.
    channel.exchange_declare(exchange='leilao_iniciado_exchange', exchange_type='fanout')
    
    # A fila para finalizar leilões continua sendo ponto-a-ponto.
    channel.queue_declare(queue='leilao_finalizado')

    print("--- MS Leilão iniciado. Monitorando horários... ---")

    while True:
        now = datetime.now()
        for leilao in LEILOES:
            # Inicia o leilão se o tempo for atingido e estiver pendente
            if leilao["status"] == "pendente" and now >= leilao["inicio"]:
                leilao["status"] = "ativo"
                
                message = {
                    "id": leilao["id"],
                    "descricao": leilao["descricao"],
                    "inicio": leilao["inicio"].isoformat(),
                    "fim": leilao["fim"].isoformat(),
                    "lance_minimo": leilao["lance_minimo"]
                }
                
                # Publica a mensagem no exchange, não em uma fila específica.
                # O exchange se encarregará de distribuir para todos os consumidores (clientes e MS Lance).
                channel.basic_publish(exchange='leilao_iniciado_exchange',
                                      routing_key='', # routing_key é ignorada em exchanges do tipo fanout
                                      body=json.dumps(message))
                print(f" Leilão {leilao['id']} INICIADO: {leilao['descricao']} (Lance Mínimo: R${leilao['lance_minimo']:.2f})")

            # Finaliza o leilão se o tempo expirar e estiver ativo
            elif leilao["status"] == "ativo" and now >= leilao["fim"]:
                leilao["status"] = "encerrado"
                
                message = {"id": leilao["id"]}
                
                channel.basic_publish(exchange='',
                                      routing_key='leilao_finalizado',
                                      body=json.dumps(message))
                print(f" Leilão {leilao['id']} FINALIZADO.")

        time.sleep(1) # Verifica a cada segundo

    connection.close()

if __name__ == '__main__':
    try:
        main()
    except KeyboardInterrupt:
        print('MS Leilão encerrado.')