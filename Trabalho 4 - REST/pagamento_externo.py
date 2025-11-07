import uvicorn
import httpx
import threading
import uuid
from fastapi import FastAPI, HTTPException, Request, Query
from fastapi.responses import HTMLResponse, JSONResponse
from pydantic import BaseModel
from typing import Dict

PORTA_ATUAL = 8004

# --- Modelos de Dados (Pydantic) ---
# Modelo do pedido recebido do MS_pagamento
class PagamentoRequest(BaseModel):
    leilao_id: int
    valor: float
    cliente_id: str
    webhook_url: str

# Modelo do payload que enviamos de volta (webhook)
class WebhookPayload(BaseModel):
    transacao_id: str
    leilao_id: int
    status: str
    valor: float
    cliente_id: str

# --- Armazenamento em Memória ---
# Dicionário para guardar pagamentos que esperam por ação manual
pagamentos_pendentes: Dict[str, PagamentoRequest] = {}
lock = threading.Lock() # Protege o dicionário
http_client = httpx.Client()


def enviar_webhook_para_ms_pagamento(transacao_id: str, status: str):
    """
    Busca o pagamento pendente, envia o webhook e o remove da lista.
    Esta é a ação principal disparada pelos botões "Aprovar"/"Recusar".
    """
    with lock:
        if transacao_id not in pagamentos_pendentes:
            print(f"--- [MOCK_PAG] ERRO: Transação {transacao_id} não encontrada.")
            return False
        # Remove o pagamento da lista de pendentes
        pagamento_data = pagamentos_pendentes.pop(transacao_id)
    
    print(f"--- [MOCK_PAG] Enviando webhook para {pagamento_data.webhook_url} com status: {status.upper()} ---")
    
    try:
        # Monta o payload do webhook
        payload = WebhookPayload(
            transacao_id=transacao_id,
            leilao_id=pagamento_data.leilao_id,
            status=status,
            valor=pagamento_data.valor,
            cliente_id=pagamento_data.cliente_id
        )
        # Envia o POST para o MS_pagamento
        http_client.post(pagamento_data.webhook_url, json=payload.dict())
        print(f"--- [MOCK_PAG] Webhook enviado com sucesso. ---")
        return True
    
    except Exception as e:
        print(f"--- [MOCK_PAG] ERRO ao enviar webhook: {e} ---")
        # Se falhar, coloca de volta na lista para tentar de novo
        with lock:
            pagamentos_pendentes[transacao_id] = pagamento_data
        return False

# --- Configuração do Servidor FastAPI ---
app = FastAPI()

# --- Endpoints da API ---

@app.post("/iniciar_pagamento")
def iniciar_pagamento(request: PagamentoRequest):
    """
    Endpoint que o MS_pagamento chama.
    Armazena o pedido e retorna um link para a PÁGINA DE PAGAMENTO.
    """
    print(f"--- [MOCK_PAG] Recebida solicitação de pagamento para Leilão {request.leilao_id} ---")
    transacao_id = str(uuid.uuid4())
    
    # Salva os dados da transação (incluindo o webhook_url)
    with lock:
        pagamentos_pendentes[transacao_id] = request
    
    # O link de pagamento aponta para este próprio servidor
    link_pagamento = f"http://localhost:{PORTA_ATUAL}/pagar/{transacao_id}"
    print(f"--- [MOCK_PAG] Pagamento {transacao_id} pendente. Link retornado: {link_pagamento} ---")
    return {"transacao_id": transacao_id, "link_pagamento": link_pagamento}


@app.post("/processar_pagamento/{transacao_id}")
def processar_pagamento(transacao_id: str, status: str = Query(..., pattern="^(aprovado|recusado)$")):
    """
    Endpoint que a PÁGINA DE PAGAMENTO (pagamento.html) chama
    quando o usuário clica em Aprovar ou Recusar.
    """
    print(f"--- [MOCK_PAG] Recebido comando manual para '{status}' a transação {transacao_id}")
    if enviar_webhook_para_ms_pagamento(transacao_id, status):
        return {"status": "ok", "message": f"Pagamento {status}."}
    else:
        raise HTTPException(status_code=500, detail="Falha ao enviar webhook.")


@app.get("/pagar/{transacao_id}", response_class=HTMLResponse)
def get_pagina_de_pagamento(transacao_id: str):
    """
    Serve a página HTML individual (pagamento.html) para o usuário
    aprovar ou recusar o pagamento.
    """
    with lock:
        pagamento_data = pagamentos_pendentes.get(transacao_id)
    
    if not pagamento_data:
        # Se não está pendente, já foi processado ou é inválido
        return HTMLResponse(
            content="<h1>Link de Pagamento Inválido</h1>"
                    "<p>Este pagamento já foi processado ou o link expirou.</p>",
            status_code=404
        )
    
    try:
        # Lê o arquivo HTML (com encoding utf-8)
        with open("pagamento.html", "r", encoding="utf-8") as f:
            content = f.read()
        
        # Injeta os dados dinâmicos nos placeholders {{...}}
        content = content.replace("{{CLIENTE_ID}}", pagamento_data.cliente_id)
        content = content.replace("{{LEILAO_ID}}", str(pagamento_data.leilao_id))
        content = content.replace("{{VALOR}}", f"{pagamento_data.valor:.2f}")
        content = content.replace("{{TRANSACAO_ID}}", transacao_id)
        
        # Retorna o HTML processado
        return HTMLResponse(content=content)

    except FileNotFoundError:
        return HTMLResponse(content="<h1>Erro 500</h1><p>Arquivo 'pagamento.html' não encontrado no servidor.</p>", status_code=500)


# --- Inicialização ---
if __name__ == "__main__":
    print(f"--- [MOCK_PAG] Iniciando Servidor de Pagamento (PÁGINA INDIVIDUAL) na porta {PORTA_ATUAL} ---")
    uvicorn.run(app, host="0.0.0.0", port=PORTA_ATUAL)