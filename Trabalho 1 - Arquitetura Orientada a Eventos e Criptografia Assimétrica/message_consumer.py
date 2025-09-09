import pika
import json
import threading
import time
import uuid

class MessageConsumer(threading.Thread):
    """
    Uma thread dedicada para consumir mensagens do RabbitMQ.
    Lida com todas as atualizações de entrada em tempo real para o cliente.
    """

    def __init__(self, user_id, subscribed_auctions):
        super().__init__(daemon=True)
        self.user_id = user_id
        self.host = 'localhost'
        # Um conjunto compartilhado com a thread principal para rastrear as inscrições
        self.subscribed_auctions = subscribed_auctions
        
        self.connection = None
        self.channel = None
        self.feedback_queue_name = None
        # Um evento para sinalizar à thread principal que a fila de feedback está pronta
        self.ready_event = threading.Event()

    def _on_leilao_iniciado(self, ch, method, properties, body):
        """Callback para anúncios de novos leilões."""
        leilao = json.loads(body)
        print("\n\n--- NOVO LEILÃO INICIADO ---")
        print(f"  ID: {leilao['id']} | Descrição: {leilao['descricao']}")
        print(f"  Lance Mínimo: R${leilao.get('lance_minimo', 0.0):.2f}")
        print(f"  Início: {leilao['inicio']} | Fim: {leilao['fim']}")
        print("--------------------------\n")
        self._prompt_user()

    def _on_auction_update(self, ch, method, properties, body):
        """Callback para atualizações em leilões inscritos (novos lances ou vencedores)."""
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
        self._prompt_user()

    def _on_feedback(self, ch, method, properties, body):
        """Callback para feedback direto sobre um lance enviado pelo usuário."""
        feedback = json.loads(body)
        print(f"\n\n--- FEEDBACK DO LANCE ---")
        print(f"  Status: {feedback['status'].upper()}")
        print(f"  Motivo: {feedback['motivo']}")
        print("-------------------------\n")
        self._prompt_user()
    
    def _prompt_user(self):
        """Ajuda a reimprimir o prompt de entrada após a chegada de uma mensagem."""
        print("Digite o ID do leilão para dar um lance ou 'sair':", end=' ', flush=True)

    def run(self):
        """O método principal de execução para esta thread."""
        try:
            self.connection = pika.BlockingConnection(pika.ConnectionParameters(self.host))
            self.channel = self.connection.channel()

            # Configura o consumidor de broadcast para novos leilões
            self.channel.exchange_declare(exchange='leilao_iniciado_exchange', exchange_type='fanout')
            result_fanout = self.channel.queue_declare(queue='', exclusive=True)
            queue_fanout = result_fanout.method.queue
            self.channel.queue_bind(exchange='leilao_iniciado_exchange', queue=queue_fanout)
            self.channel.basic_consume(queue=queue_fanout, on_message_callback=self._on_leilao_iniciado, auto_ack=True)

            # Configura o consumidor direto para atualizações de leilões
            self.channel.exchange_declare(exchange='notificacoes_exchange', exchange_type='direct')
            result_direct = self.channel.queue_declare(queue='', exclusive=True)
            self.queue_direct = result_direct.method.queue
            self.channel.basic_consume(queue=self.queue_direct, on_message_callback=self._on_auction_update, auto_ack=True)

            # Configura a fila de feedback exclusiva
            self.feedback_queue_name = f'feedback_{self.user_id}_{uuid.uuid4().hex}'
            self.channel.queue_declare(queue=self.feedback_queue_name, exclusive=True)
            self.channel.basic_consume(queue=self.feedback_queue_name, on_message_callback=self._on_feedback, auto_ack=True)
            
            # Sinaliza que a fila de feedback está pronta
            self.ready_event.set()

            consuming_for = set()
            while True:
                # Vincula dinamicamente a notificações de novos leilões conforme necessário
                for auction_id in list(self.subscribed_auctions):
                    if auction_id not in consuming_for:
                        routing_key = f'leilao_{auction_id}'
                        self.channel.queue_bind(exchange='notificacoes_exchange', queue=self.queue_direct, routing_key=routing_key)
                        consuming_for.add(auction_id)
                        print(f"\n[INFO] Inscrito para receber notificações do leilão {auction_id}.")
                        self._prompt_user()
                # Processa eventos sem bloquear a thread inteira
                self.connection.process_data_events(time_limit=1)
                time.sleep(0.1)

        except pika.exceptions.AMQPConnectionError:
            print(f"--- [CONSUMER] ERRO: Não foi possível conectar ao RabbitMQ em '{self.host}'. ---")
        finally:
            if self.connection and self.connection.is_open:
                self.connection.close()