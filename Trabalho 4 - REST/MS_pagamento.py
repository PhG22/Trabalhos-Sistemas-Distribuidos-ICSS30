import pika
import json
import threading
import uvicorn
import httpx
import time
from fastapi import FastAPI, Request, HTTPException
from pydantic import BaseModel
from typing import Dict

# Porta em que este MS (Pagamento) rodará
PORTA_ATUAL = 8003
# URL do Sistema de Pagamento Externo
URL_SISTEMA_EXTERNO = "http://localhost:8004"
# URL do nosso próprio webhook, para informar ao sistema externo
URL_WEBHOOK = f"http://localhost:{PORTA_ATUAL}/webhook_pagamento"


# --- Modelos de Dados (Pydantic) ---

# Modelo para o webhook que receberemos do sistema externo
class WebhookPayload(BaseModel):
    transacao_id: str
    leilao_id: int
    status: str  # "aprovado" ou "recusado"
    valor: float
    cliente_id: str


# --- Microsserviço de Pagamento ---
class MSPagamento:
    def __init__(self, host="localhost"):
        self.host = host
        
        # O cliente HTTP para fazer requisições REST ao sistema externo
        # Usamos um cliente síncrono aqui por simplicidade na thread do Pika
        self.http_client = httpx.Client()

        try:
            self.connection = pika.BlockingConnection(
                pika.ConnectionParameters(self.host)
            )
            self.channel = self.connection.channel()

            # --- Configuração de Filas (RabbitMQ) ---

            # Publica
            self.channel.queue_declare(queue="link_pagamento")
            self.channel.queue_declare(queue="status_pagamento")
            
            print("--- [MS_Pagamento] Conectado ao RabbitMQ ---")

        except pika.exceptions.AMQPConnectionError as e:
            print(
                f"--- [MS_Pagamento] Erro: Não foi possível estabelecer conexão com o RabbitMQ em '{self.host}'. ---"
            )
            if hasattr(self, 'connection') and self.connection and self.connection.is_open:
                self.connection.close()
            raise e

    def _publish_message(self, routing_key: str, body: dict):
        """Função auxiliar para publicar mensagens JSON."""
        # Garante que o canal está disponível (pode ser chamado pela thread do FastAPI)
        self.channel.basic_publish(
            exchange="",
            routing_key=routing_key,
            body=json.dumps(body)
        )
        print(f" [MS_Pagamento] Publicado '{routing_key}': {body}")

    def _request_payment_link(self, vencedor_data: dict):
        """Faz a requisição REST para o sistema externo, com retentativas."""
        
        leilao_id = vencedor_data['leilao_id']
        valor = vencedor_data['valor']
        vencedor_id = vencedor_data['vencedor_id']

        payload = {
            "leilao_id": leilao_id,
            "valor": valor,
            "cliente_id": vencedor_id,
            "webhook_url": URL_WEBHOOK
        }
        
        print(f" [MS_Pagamento] Solicitando link de pagamento para leilão {leilao_id}...")
        
        # --- LÓGICA DE RETENTATIVA ---
        max_retries = 5
        retry_delay_seconds = 3
        
        for attempt in range(max_retries):
            try:
                response = self.http_client.post(
                    f"{URL_SISTEMA_EXTERNO}/iniciar_pagamento",
                    json=payload,
                    timeout=5.0
                )
                response.raise_for_status() # Lança exceção se for erro 4xx ou 5xx
                
                response_data = response.json()
                link = response_data.get("link_pagamento")
                
                if link:
                    print(f" [MS_Pagamento] Link recebido: {link}")
                    self._publish_message(
                        "link_pagamento",
                        {"leilao_id": leilao_id, "link": link, "vencedor_id": vencedor_id}
                    )
                    return # Sucesso, sair da função
                else:
                    print(" [MS_Pagamento] ERRO: Resposta do sistema externo não continha 'link_pagamento'")
                    # Não adianta tentar de novo se a resposta veio errada
                    return 

            except httpx.RequestError as exc:
                print(f" [MS_Pagamento] ERRO ao contatar Sistema Externo (Tentativa {attempt + 1}/{max_retries}): {exc}")
                if attempt < max_retries - 1:
                    time.sleep(retry_delay_seconds) # Espera antes de tentar de novo
                else:
                    print(f" [MS_Pagamento] FALHA TOTAL: Desistindo de gerar link para leilão {leilao_id}.")
            except Exception as e:
                print(f" [MS_Pagamento] ERRO inesperado ao processar pagamento: {e}")
                # Erro inesperado, provavelmente não adianta tentar de novo
                return

    # --- Callback do Consumidor RabbitMQ ---
    def callback_leilao_vencedor(self, ch, method, properties, body):
        """Consome 'leilao_vencedor' e inicia o processo de pagamento."""
        print(f" [MS_Pagamento] Recebido 'leilao_vencedor'")
        vencedor_data = json.loads(body)
        
        # Se não houver vencedor, não faz nada
        if vencedor_data.get("vencedor_id") is None:
            print(f" [MS_Pagamento] Leilão {vencedor_data['leilao_id']} terminou sem vencedor.")
            ch.basic_ack(delivery_tag=method.delivery_tag)
            return

        # Inicia a chamada REST para o sistema externo
        self._request_payment_link(vencedor_data)
        
        ch.basic_ack(delivery_tag=method.delivery_tag)

    # --- Thread de Consumo ---
    def start_consuming(self):
        """Inicia o consumo de mensagens do RabbitMQ em loop."""
        print("--- [MS_Pagamento] Consumidor RabbitMQ iniciado em thread separada... ---")
        try:
            # Declara o exchange
            self.channel.exchange_declare(
                exchange="leilao_vencedor_exchange", 
                exchange_type="fanout"
            )
            # Cria uma fila exclusiva para o MS Pagamento
            result = self.channel.queue_declare(queue="", exclusive=True)
            queue_name_vencedor = result.method.queue
            
            # Faz o bind da fila ao exchange
            self.channel.queue_bind(
                exchange="leilao_vencedor_exchange", 
                queue=queue_name_vencedor
            )
            # Consome da sua fila exclusiva
            self.channel.basic_consume(
                queue=queue_name_vencedor, 
                on_message_callback=self.callback_leilao_vencedor
            )
            self.channel.start_consuming()
        except KeyboardInterrupt:
            print("\n--- [MS_Pagamento] Consumidor RabbitMQ encerrando. ---")
        finally:
            if self.connection and self.connection.is_open:
                self.connection.close()
                print("--- [MS_Pagamento] Conexão RabbitMQ fechada. ---")

# --- Configuração do Servidor FastAPI ---
app = FastAPI()

try:
    payment_service = MSPagamento()
except Exception as e:
    print(f"Falha fatal ao inicializar o MSPagamento. Encerrando. Erro: {e}")
    exit(1)

# --- Endpoints da API REST (Webhook) ---

@app.post("/webhook_pagamento")
def webhook_pagamento_recebido(payload: WebhookPayload):
    """
    Endpoint REST que o Sistema de Pagamento Externo chamará
    para notificar o status da transação.
    """
    print(f" [MS_Pagamento] WEBHOOK RECEBIDO: Transação {payload.transacao_id} - Status {payload.status}")
    
    # Publica o status na fila 'status_pagamento'
    message = {
        "leilao_id": payload.leilao_id,
        "status": payload.status,
        "valor": payload.valor,
        "vencedor_id": payload.cliente_id
    }
    payment_service._publish_message("status_pagamento", message)
    
    return {"status": "ok", "message": "Notificação recebida."}

# --- Inicialização ---

if __name__ == "__main__":
    try:
        # Inicia o consumidor RabbitMQ em uma thread separada
        consumer_thread = threading.Thread(
            target=payment_service.start_consuming, daemon=True
        )
        consumer_thread.start()

        # Inicia o servidor web (FastAPI) na thread principal
        print(f"--- [MS_Pagamento] Iniciando servidor web FastAPI na porta {PORTA_ATUAL} ---")
        uvicorn.run(app, host="0.0.0.0", port=PORTA_ATUAL)

    except KeyboardInterrupt:
        print("--- [MS_Pagamento] Encerrando servidor web... ---")