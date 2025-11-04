import uvicorn
import httpx
import time
import threading
import uuid
import random
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
from typing import Dict

# Porta em que este MOCK rodará
PORTA_ATUAL = 8004

# --- Modelos de Dados (Pydantic) ---

# O que esperamos receber do MS_pagamento
class PagamentoRequest(BaseModel):
    leilao_id: int
    valor: float
    cliente_id: str
    webhook_url: str  # URL para onde devemos enviar a notificação

# O que enviaremos de volta no webhook
class WebhookPayload(BaseModel):
    transacao_id: str
    leilao_id: int
    status: str  # "aprovado" ou "recusado"
    valor: float
    cliente_id: str

# --- Funções de Simulação (em Thread) ---

def simular_processamento_e_enviar_webhook(
    webhook_url: str,
    transacao_id: str,
    leilao_id: int,
    valor: float,
    cliente_id: str
):
    """
    Simula o tempo que o usuário leva para pagar e envia o webhook.
    Isso roda em uma thread separada para não bloquear o servidor.
    """
    try:
        print(f"--- [MOCK_PAG] Iniciando processamento da transação {transacao_id} (Leilão {leilao_id}) ---")
        
        # Simula o tempo de processamento
        time.sleep(10)
        
        # Decide aleatoriamente o status do pagamento
        status = random.choice(["aprovado", "recusado"])
        
        print(f"--- [MOCK_PAG] Transação {transacao_id} finalizada com status: {status.upper()} ---")

        # Prepara o payload do webhook
        payload = WebhookPayload(
            transacao_id=transacao_id,
            leilao_id=leilao_id,
            status=status,
            valor=valor,
            cliente_id=cliente_id
        )

        # Envia o webhook (POST) de volta para o MS_pagamento
        with httpx.Client() as client:
            client.post(webhook_url, json=payload.dict())
        
        print(f"--- [MOCK_PAG] Webhook enviado para {webhook_url} ---")

    except Exception as e:
        print(f"--- [MOCK_PAG] ERRO na thread de simulação: {e} ---")

# --- Configuração do Servidor FastAPI ---
app = FastAPI()

@app.post("/iniciar_pagamento")
def iniciar_pagamento(request: PagamentoRequest):
    """
    Endpoint que o MS_pagamento chama.
    Recebe os dados, inicia a simulação em background e retorna o link.
    """
    print(f"--- [MOCK_PAG] Recebida solicitação de pagamento para Leilão {request.leilao_id} ---")
    
    # Gera dados falsos da transação
    transacao_id = str(uuid.uuid4())
    link_pagamento = f"http://localhost:{PORTA_ATUAL}/pagar/{transacao_id}"
    
    # Inicia a simulação em uma thread separada (para ser assíncrono)
    thread = threading.Thread(
        target=simular_processamento_e_enviar_webhook,
        args=(
            request.webhook_url,
            transacao_id,
            request.leilao_id,
            request.valor,
            request.cliente_id
        ),
        daemon=True
    )
    thread.start()
    
    # Retorna o link imediatamente
    print(f"--- [MOCK_PAG] Retornando link: {link_pagamento} ---")
    return {"transacao_id": transacao_id, "link_pagamento": link_pagamento}

@app.get("/pagar/{transacao_id}")
def pagina_pagamento_falsa(transacao_id: str):
    """Uma página falsa para onde o 'link_pagamento' aponta."""
    return {
        "message": "Esta é uma página de pagamento simulada.",
        "transacao_id": transacao_id,
        "instrucao": "Feche esta aba. O status será enviado ao sistema em 10 segundos."
    }

# --- Inicialização ---

if __name__ == "__main__":
    print(f"--- [MOCK_PAG] Iniciando Servidor de Pagamento Externo (Mock) na porta {PORTA_ATUAL} ---")
    uvicorn.run(app, host="0.0.0.0", port=PORTA_ATUAL)