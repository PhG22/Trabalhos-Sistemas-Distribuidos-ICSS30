import uvicorn
import httpx
import threading
import json
import grpc
from concurrent import futures
from fastapi import FastAPI
from pydantic import BaseModel
import queue

import pagamento_pb2, pagamento_pb2_grpc

# Configurações de Portas
PORTA_GRPC = 8003
PORTA_HTTP = 8005 # Webhook roda em porta separada
URL_SISTEMA_EXTERNO = "http://localhost:8004"
URL_WEBHOOK = f"http://localhost:{PORTA_HTTP}/webhook_pagamento"

# --- Servidor REST para Webhook ---
app = FastAPI()

class WebhookPayload(BaseModel):
    transacao_id: str
    leilao_id: int
    status: str
    valor: float
    cliente_id: str

# Fila global para comunicação entre a thread do FastAPI e a thread do gRPC
event_queues = set()
queue_lock = threading.Lock()

def broadcast_grpc_event(tipo, payload):
    """Função auxiliar usada pelo FastAPI para enviar eventos aos clientes gRPC (Gateway)."""
    msg = pagamento_pb2.Evento(tipo=tipo, payload_json=json.dumps(payload))
    with queue_lock:
        dead_queues = set()
        for q in event_queues:
            try:
                q.put(msg)
            except:
                dead_queues.add(q)
        for q in dead_queues:
            event_queues.remove(q)

@app.post("/webhook_pagamento")
def webhook(payload: WebhookPayload):
    """
    Recebe o callback do sistema externo (REST).
    Converte em evento gRPC para o Gateway.
    """
    print(f"[MS Pagamento] Webhook recebido: {payload.status}")
    
    msg = {
        "leilao_id": payload.leilao_id, 
        "status": payload.status,
        "valor": payload.valor, 
        "vencedor_id": payload.cliente_id
    }
    
    broadcast_grpc_event("status_pagamento", msg)
    return {"status": "ok"}

# --- Serviço gRPC (Comunicação Interna) ---
class PaymentService(pagamento_pb2_grpc.PaymentServiceServicer):
    
    def SubscribeEventos(self, request, context):
        """Stream de eventos para o Gateway."""
        print("[MS Pagamento] Gateway conectado para receber eventos.")
        q = queue.Queue()
        with queue_lock:
            event_queues.add(q)
        try:
            while True:
                yield q.get()
        except:
            with queue_lock:
                if q in event_queues:
                    event_queues.remove(q)
            print("[MS Pagamento] Gateway desconectado dos eventos.")

    def ProcessarPagamento(self, request, context):
        """
        Recebe a ordem de pagamento do MS Lance.
        Faz a chamada REST para o sistema externo.
        """
        lid = request.leilao_id
        vid = request.vencedor_id
        val = request.valor
        print(f"[MS Pagamento] Processando pagamento para Leilão {lid}, Vencedor {vid}")

        try:
            # Cliente HTTP síncrono
            with httpx.Client() as client:
                resp = client.post(f"{URL_SISTEMA_EXTERNO}/iniciar_pagamento", json={
                    "leilao_id": lid, 
                    "valor": val, 
                    "cliente_id": vid, 
                    "webhook_url": URL_WEBHOOK
                })
                
                if resp.status_code == 200:
                    data = resp.json()
                    link = data.get("link_pagamento")
                    print(f"[MS Pagamento] Link gerado: {link}")
                    
                    # Avisa o Gateway que o link está pronto
                    broadcast_grpc_event("link_pagamento", {
                        "leilao_id": lid, 
                        "link": link, 
                        "vencedor_id": vid, 
                        "valor": val
                    })
                else:
                    print(f"[Erro] Falha no sistema externo: {resp.text}")
                    
        except Exception as e:
            print(f"[Erro] Exceção ao chamar sistema externo: {e}")
        
        return pagamento_pb2.Empty()

def run_grpc():
    """Inicia o servidor gRPC."""
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=5))
    pagamento_pb2_grpc.add_PaymentServiceServicer_to_server(PaymentService(), server)
    server.add_insecure_port(f'[::]:{PORTA_GRPC}')
    print(f"--- [MS Pagamento] gRPC rodando na porta {PORTA_GRPC} ---")
    server.start()
    server.wait_for_termination()

if __name__ == "__main__":
    # Roda gRPC em uma thread separada para não bloquear o FastAPI
    t = threading.Thread(target=run_grpc, daemon=True)
    t.start()
    
    # Roda o servidor Web na thread principal
    print(f"--- [MS Pagamento] Webhook REST rodando na porta {PORTA_HTTP} ---")
    uvicorn.run(app, host="0.0.0.0", port=PORTA_HTTP)