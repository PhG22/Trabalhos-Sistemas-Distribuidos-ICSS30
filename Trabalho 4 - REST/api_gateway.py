import asyncio
import json
import threading
import pika
import pika.exceptions
import httpx
import time
from fastapi import FastAPI, Request, HTTPException, Query
from fastapi.responses import HTMLResponse
from fastapi.middleware.cors import CORSMiddleware
from sse_starlette.sse import EventSourceResponse
from typing import List, Dict
import uvicorn
from contextlib import asynccontextmanager

# --- Configuração do Lifespan ---
@asynccontextmanager
async def lifespan(app: FastAPI):
    # Código que roda ANTES do servidor iniciar
    print("INFO: Iniciando evento lifespan startup...")
    loop = asyncio.get_event_loop()
    # Iniciar o consumidor RabbitMQ em uma thread separada
    consumer_thread = threading.Thread(
        target=rabbitmq_consumer, 
        args=(loop,),  # Passa o loop principal para a thread
        daemon=True
    )
    consumer_thread.start()
    print("INFO: Thread do consumidor RabbitMQ iniciada.")
    
    yield # O servidor roda aqui
    
    print("INFO: Evento lifespan shutdown.")


# --- Configuração da Aplicação FastAPI ---
app = FastAPI(lifespan=lifespan)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# --- URLs dos Microsserviços Internos ---
MS_LEILAO_URL = "http://localhost:8001"
MS_LANCE_URL = "http://localhost:8002"


# --- Gerenciamento de Conexões SSE e Interesses ---
class ConnectionManager:
    def __init__(self):
        self.active_connections: Dict[str, asyncio.Queue] = {} 
        self.client_interests: Dict[str, set] = {}

    async def connect(self, user_id: str) -> (str, asyncio.Queue): # type: ignore
        """Registra uma nova conexão de cliente pelo user_id."""
        
        # Se o usuário já estiver conectado (ex: aba antiga), 
        # podemos desconectar a antiga ou permitir múltiplas (aqui, substituímos)
        if user_id in self.active_connections:
            print(f"WARN: Usuário {user_id} já conectado. Substituindo conexão antiga.")
            # fechar a fila antiga(?)
            # await self.active_connections[user_id].put(None) 
            
        event_queue = asyncio.Queue()
        self.active_connections[user_id] = event_queue
        self.client_interests[user_id] = set()
        print(f"INFO: Cliente '{user_id}' conectou-se via SSE.")
        return user_id, event_queue
    
    def disconnect(self, user_id: str):
        """Remove uma conexão de cliente pelo user_id."""
        if user_id in self.active_connections:
            del self.active_connections[user_id]
            if user_id in self.client_interests:
                 del self.client_interests[user_id]
            print(f"INFO: Cliente '{user_id}' desconectou-se.")
            
    def add_interest(self, user_id: str, leilao_id: int):
        if user_id in self.client_interests:
            self.client_interests[user_id].add(leilao_id)
            print(f"INFO: Cliente '{user_id}' registrou interesse no leilão {leilao_id}.")

    def remove_interest(self, user_id: str, leilao_id: int):
        if user_id in self.client_interests and leilao_id in self.client_interests[user_id]:
            self.client_interests[user_id].remove(leilao_id)
            print(f"INFO: Cliente '{user_id}' removeu interesse no leilão {leilao_id}.")

    async def broadcast_event(self, event_name: str, data: dict):
        """Envia um evento para os clientes interessados."""
        leilao_id = data.get("leilao_id")
        user_id = data.get("user_id") # Para lance_invalidado
        vencedor_id = data.get("vencedor_id") # Para link_pagamento
        
        event_payload = { "event": event_name, "data": json.dumps(data) }

        # Eventos direcionados só para o usuário específico
        if event_name == "lance_invalidado" and user_id:
            if user_id in self.active_connections:
                await self.active_connections[user_id].put(event_payload)
            return
            
        if event_name == "link_pagamento" and vencedor_id:
            if vencedor_id in self.active_connections:
                await self.active_connections[vencedor_id].put(event_payload)
            return
        
        if event_name == "status_pagamento" and vencedor_id:
             if vencedor_id in self.active_connections:
                await self.active_connections[vencedor_id].put(event_payload)
             return

        # Eventos de broadcast para todos os interessados no leilão
        if event_name in ["lance_validado", "leilao_vencedor"]:
            for client_id, interests in self.client_interests.items():
                if leilao_id and leilao_id in interests:
                    if client_id in self.active_connections:
                        await self.active_connections[client_id].put(event_payload)

manager = ConnectionManager()

# --- Endpoint SSE ---
@app.get("/eventos")
async def sse_endpoint(request: Request, user_id: str = Query(...)):
    """Endpoint para clientes se conectarem e receberem notificações via SSE."""
    
    # Usa o user_id fornecido para conectar
    user_id, event_queue = await manager.connect(user_id)

    async def event_generator():
        try:      
            while True:
                event_payload = await event_queue.get()
                # se desconectamos a conexão antiga, 'None' pode ser um sinal para fechar
                # if event_payload is None:
                #     break
                yield event_payload
        except asyncio.CancelledError:
            manager.disconnect(user_id)
            print(f"INFO: Conexão SSE para cliente '{user_id}' fechada.")

    return EventSourceResponse(event_generator())


# --- Endpoints REST ---
@app.get("/", response_class=HTMLResponse)
async def read_root():
    try:
        with open("index.html", encoding="utf-8") as f:
            return HTMLResponse(content=f.read(), status_code=200)
    except FileNotFoundError:
        return HTMLResponse(content="<h1>API Gateway Leilão</h1><p>Arquivo index.html não encontrado.</p>", status_code=200)

@app.get("/leiloes/ativos")
async def consultar_leiloes_ativos():
    """Repassa a consulta de leilões ativos para o MS Leilão."""
    async with httpx.AsyncClient() as client:
        try:
            response = await client.get(f"{MS_LEILAO_URL}/leiloes")
            response.raise_for_status()
            return response.json()
        except httpx.RequestError as exc:
            raise HTTPException(status_code=503, detail=f"Erro ao comunicar com MS Leilão: {exc}")

@app.post("/leiloes")
async def criar_leilao(request: Request):
    """Repassa a criação de um leilão para o MS Leilão."""
    leilao_data = await request.json()
    async with httpx.AsyncClient() as client:
        try:
            response = await client.post(f"{MS_LEILAO_URL}/leiloes", json=leilao_data)
            response.raise_for_status()
            return response.json()
        except httpx.HTTPStatusError as exc:
            detail = exc.response.json().get("detail", exc.response.text)
            raise HTTPException(status_code=exc.response.status_code, detail=detail)
        except httpx.RequestError as exc:
            raise HTTPException(status_code=503, detail=f"Erro ao comunicar com MS Leilão: {exc}")
        

@app.post("/lances")
async def efetuar_lance(request: Request):
    """Repassa um lance para o MS Lance."""
    lance_data = await request.json()
    async with httpx.AsyncClient() as client:
        try:
            response = await client.post(f"{MS_LANCE_URL}/lances", json=lance_data)
            response.raise_for_status() 
            return response.json()
        except httpx.HTTPStatusError as exc:
            detail = exc.response.json().get("detail", exc.response.text)
            raise HTTPException(status_code=exc.response.status_code, detail=detail)
        except httpx.RequestError as exc:
            raise HTTPException(status_code=503, detail=f"Erro ao comunicar com MS Lance: {exc}")

@app.post("/leiloes/{leilao_id}/registrar/{user_id}")
def registrar_interesse(leilao_id: int, user_id: str):
    manager.add_interest(user_id, leilao_id)
    return {"status": "ok", "message": f"Interesse registrado para cliente '{user_id}' no leilão {leilao_id}"}
    
@app.delete("/leiloes/{leilao_id}/registrar/{user_id}")
def cancelar_interesse(leilao_id: int, user_id: str):
    manager.remove_interest(user_id, leilao_id)
    return {"status": "ok", "message": f"Interesse cancelado para cliente '{user_id}' no leilão {leilao_id}"}


# --- Consumidor RabbitMQ (em uma Thread separada) ---

def rabbitmq_consumer(loop: asyncio.AbstractEventLoop):
    """Função que conecta ao RabbitMQ e consome mensagens em loop."""
    
    queues_to_consume = [
        "lance_validado", "lance_invalidado",
        "link_pagamento", "status_pagamento"
    ]

    def callback(ch, method, properties, body):
        event_name = method.routing_key
        data = json.loads(body)
        print(f"RABBITMQ: Recebido '{event_name}' com dados: {data}")
        
        # Agenda a coroutine 'broadcast_event' para rodar
        # no loop de eventos principal do FastAPI (thread-safe)
        asyncio.run_coroutine_threadsafe(
            manager.broadcast_event(event_name, data),
            loop
        )
        
        ch.basic_ack(delivery_tag=method.delivery_tag)

    # Loop de reconexão
    while True:
        try:
            connection = pika.BlockingConnection(pika.ConnectionParameters(host='localhost'))
            channel = connection.channel()
            print("INFO: Consumidor RabbitMQ conectado.")

            for queue_name in queues_to_consume:
                channel.queue_declare(queue=queue_name)
                channel.basic_consume(queue=queue_name, on_message_callback=callback)

            channel.exchange_declare(
                exchange="leilao_vencedor_exchange", 
                exchange_type="fanout"
            )
            # Cria uma fila exclusiva e anônima para o Gateway
            result = channel.queue_declare(queue="", exclusive=True)
            queue_name_vencedor = result.method.queue
            
            # Faz o bind da fila ao exchange
            channel.queue_bind(
                exchange="leilao_vencedor_exchange", 
                queue=queue_name_vencedor
            )
            # Consome da sua fila exclusiva
            channel.basic_consume(
                queue=queue_name_vencedor, 
                on_message_callback=callback
            )

            print("INFO: Consumidor RabbitMQ iniciado e aguardando mensagens.")
            channel.start_consuming()

        except pika.exceptions.AMQPConnectionError as e:
            print(f"ERRO: Não foi possível conectar ao RabbitMQ: {e}. Tentando novamente em 5 segundos...")
            time.sleep(5)
        except Exception as e:
            print(f"ERRO: Consumidor RabbitMQ falhou: {e}. Reiniciando em 5 segundos...")
            time.sleep(5)


if __name__ == "__main__":
    # Inicia o servidor web FastAPI
    uvicorn.run(app, host="0.0.0.0", port=5000)