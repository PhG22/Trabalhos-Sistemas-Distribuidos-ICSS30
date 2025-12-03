import asyncio
import uvicorn
import json
import threading
import time
import grpc
from fastapi import FastAPI, Request, HTTPException, Query
from fastapi.responses import HTMLResponse
from fastapi.middleware.cors import CORSMiddleware
from sse_starlette.sse import EventSourceResponse
from typing import List, Dict
from contextlib import asynccontextmanager

import leilao_pb2, leilao_pb2_grpc
import lance_pb2, lance_pb2_grpc
import pagamento_pb2_grpc

# Endereços dos servidores gRPC
HOST_LEILAO = 'localhost:8001'
HOST_LANCE = 'localhost:8002'
HOST_PAGAMENTO = 'localhost:8003'

# --- Gerenciamento de Conexões SSE ---
class ConnectionManager:
    """
    Gerencia as conexões ativas de Server-Sent Events (SSE) e o roteamento de mensagens.
    """
    def __init__(self):
        # Mapeia user_id -> Lista de Filas (permite múltiplas abas por usuário)
        self.active_connections: Dict[str, List[asyncio.Queue]] = {} 
        # Mapeia user_id -> Set de IDs de leilão (interesses)
        self.client_interests: Dict[str, set] = {}
        # Lock para garantir thread-safety ao modificar interesses
        self.interests_lock = asyncio.Lock()

    async def connect(self, user_id: str) -> tuple[str, asyncio.Queue]:
        """Cria uma nova fila de eventos para um usuário que acabou de conectar."""
        event_queue = asyncio.Queue()
        if user_id not in self.active_connections:
            self.active_connections[user_id] = []
        
        async with self.interests_lock:
            if user_id not in self.client_interests:
                self.client_interests[user_id] = set()
            
        self.active_connections[user_id].append(event_queue)
        print(f"INFO: Cliente '{user_id}' conectou-se via SSE.")
        return user_id, event_queue

    async def disconnect(self, user_id: str, event_queue: asyncio.Queue):
        """Remove a fila de eventos quando o cliente desconecta."""
        if user_id in self.active_connections:
            try:
                self.active_connections[user_id].remove(event_queue)
                # Se não houver mais conexões ativas, limpa os interesses
                if not self.active_connections[user_id]:
                    del self.active_connections[user_id]
                    async with self.interests_lock:
                        if user_id in self.client_interests:
                            del self.client_interests[user_id]
                print(f"INFO: Cliente '{user_id}' desconectou uma aba.")
            except ValueError:
                pass
            
    async def add_interest(self, user_id: str, leilao_id: int):
        """Registra que o usuário quer receber updates de um leilão específico."""
        async with self.interests_lock:
            if user_id not in self.client_interests:
                self.client_interests[user_id] = set()
            self.client_interests[user_id].add(leilao_id)

    async def remove_interest(self, user_id: str, leilao_id: int):
        """Remove o interesse de um usuário em um leilão."""
        async with self.interests_lock:
            if user_id in self.client_interests and leilao_id in self.client_interests[user_id]:
                self.client_interests[user_id].remove(leilao_id)

    # Lógicas de limpeza automática de interesses baseadas no ciclo de vida do leilão
    async def clear_interests_for_losers(self, leilao_id: int, winner_id: str):
        async with self.interests_lock:
            for user_id, interests in list(self.client_interests.items()):
                if user_id != winner_id and leilao_id in interests:
                    interests.remove(leilao_id)

    async def clear_interest_for_winner(self, leilao_id: int, winner_id: str):
        await self.remove_interest(user_id=winner_id, leilao_id=leilao_id)

    async def broadcast_event(self, event_name: str, data: dict):
        """
        Recebe um evento (vindo do gRPC) e o distribui para as filas SSE corretas
        baseado no ID do leilão, ID do usuário ou broadcast global.
        """
        leilao_id = data.get("leilao_id")
        user_id = data.get("user_id")
        vencedor_id = data.get("vencedor_id")
        
        event_payload = { "event": event_name, "data": json.dumps(data) }

        async def send_to_user(target_user_id: str):
            if target_user_id in self.active_connections:
                for queue in self.active_connections.get(target_user_id, []):
                    await queue.put(event_payload)

        # Roteamento específico (mensagens privadas)
        target_user = None
        if event_name == "lance_invalidado" and user_id:
            target_user = user_id
        elif event_name in ["link_pagamento", "status_pagamento"] and vencedor_id:
            target_user = vencedor_id
        
        if target_user:
            await send_to_user(target_user)
            return

        # Roteamento Global (todos recebem)
        if event_name == "leilao_iniciado":
             for uid in list(self.active_connections.keys()):
                 await send_to_user(uid)
             return

        # Roteamento por Interesse
        async with self.interests_lock:
            if event_name in ["lance_validado", "leilao_vencedor"]:
                for client_id, interests in self.client_interests.items():
                    if leilao_id and leilao_id in interests:
                        await send_to_user(client_id)

manager = ConnectionManager()

# --- Consumidor de Streams gRPC ---

def listen_to_grpc_stream(stub_func, service_name, loop):
    """
    Função executada em thread separada.
    Conecta-se ao método `SubscribeEventos` de um serviço gRPC e fica ouvindo.
    Quando um evento chega, repassa para o loop principal do FastAPI/Asyncio.
    """
    while True:
        try:
            print(f"INFO: Conectando ao stream de eventos de {service_name}...")
            # stub_func é a função gRPC (ex: stub.SubscribeEventos)
            stream = stub_func(leilao_pb2.Empty())
            
            for evento in stream:
                print(f"gRPC Evento de {service_name}: {evento.tipo}")
                data = json.loads(evento.payload_json)
                
                # Lógica de limpeza de interesses (Side effects no Gateway)
                if evento.tipo == "status_pagamento":
                     asyncio.run_coroutine_threadsafe(
                        manager.clear_interest_for_winner(data.get("leilao_id"), data.get("vencedor_id")), loop
                    )
                elif evento.tipo == "leilao_vencedor":
                     asyncio.run_coroutine_threadsafe(
                        manager.clear_interests_for_losers(data.get("leilao_id"), data.get("vencedor_id")), loop
                    )

                # Envia para o frontend via SSE
                asyncio.run_coroutine_threadsafe(
                    manager.broadcast_event(evento.tipo, data), loop
                )
                
        except grpc.RpcError as e:
            print(f"WARN: Conexão gRPC com {service_name} perdida ({e.code()}). Reconectando em 5s...")
            time.sleep(5)
        except Exception as e:
            print(f"ERRO genérico em {service_name}: {e}")
            time.sleep(5)

def start_grpc_consumers(loop):
    """Inicia uma thread de escuta para cada microsserviço."""
    
    # Listener MS Leilão
    chan_leilao = grpc.insecure_channel(HOST_LEILAO)
    stub_leilao = leilao_pb2_grpc.AuctionServiceStub(chan_leilao)
    threading.Thread(target=listen_to_grpc_stream, args=(stub_leilao.SubscribeEventos, "MS Leilão", loop), daemon=True).start()

    # Listener MS Lance
    chan_lance = grpc.insecure_channel(HOST_LANCE)
    stub_lance = lance_pb2_grpc.LanceServiceStub(chan_lance)
    threading.Thread(target=listen_to_grpc_stream, args=(stub_lance.SubscribeEventos, "MS Lance", loop), daemon=True).start()

    # Listener MS Pagamento
    chan_pag = grpc.insecure_channel(HOST_PAGAMENTO)
    stub_pag = pagamento_pb2_grpc.PaymentServiceStub(chan_pag)
    threading.Thread(target=listen_to_grpc_stream, args=(stub_pag.SubscribeEventos, "MS Pagamento", loop), daemon=True).start()

# --- Ciclo de Vida ---
@asynccontextmanager
async def lifespan(app: FastAPI):
    print("INFO: Iniciando Gateway gRPC...")
    loop = asyncio.get_event_loop()
    # Inicia as threads que vão ouvir os microsserviços
    start_grpc_consumers(loop)
    yield
    print("INFO: Gateway encerrado.")

app = FastAPI(lifespan=lifespan)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# --- Endpoint SSE ---
@app.get("/eventos")
async def sse_endpoint(request: Request, user_id: str = Query(...)):
    """Endpoint onde o navegador se conecta para receber atualizações."""
    user_id, event_queue = await manager.connect(user_id)
    async def event_generator():
        while True:
            try:
                # Espera mensagem ou envia ping a cada 30s
                message = await asyncio.wait_for(event_queue.get(), timeout=30.0)
                yield message
            except asyncio.TimeoutError:
                yield { "event": "ping", "data": "keep-alive" }
            except asyncio.CancelledError:
                await manager.disconnect(user_id, event_queue)
                break
    return EventSourceResponse(event_generator())

# --- Endpoints REST (Proxy para gRPC) ---

@app.get("/", response_class=HTMLResponse)
async def read_root():
    """Serve o arquivo HTML principal."""
    try:
        with open("index.html", "r", encoding="utf-8") as f:
            return HTMLResponse(content=f.read(), status_code=200)
    except FileNotFoundError:
        return HTMLResponse(content="Index não encontrado", status_code=404)

@app.get("/leiloes/ativos")
async def consultar_leiloes_ativos():
    """
    Endpoint complexo que agrega dados de dois serviços gRPC:
    1. Pega a lista de leilões do MS Leilão.
    2. Pega os valores atuais do MS Lance.
    3. Mescla os resultados.
    """
    try:
        # Chamada gRPC para MS Leilão
        with grpc.insecure_channel(HOST_LEILAO) as chan_leilao:
            stub_leilao = leilao_pb2_grpc.AuctionServiceStub(chan_leilao)
            leiloes_proto = list(stub_leilao.GetLeiloesAtivos(leilao_pb2.Empty()))
        
        # Chamada gRPC para MS Lance
        with grpc.insecure_channel(HOST_LANCE) as chan_lance:
            stub_lance = lance_pb2_grpc.LanceServiceStub(chan_lance)
            lances_proto = list(stub_lance.GetLancesAtuais(leilao_pb2.Empty()))
        
        # Agregação de dados
        lances_dict = {l.id: l.valor_atual for l in lances_proto}
        
        result = []
        for l in leiloes_proto:
            val_lance = lances_dict.get(l.id, 0.0)
            # Se não houver lances, usa o valor inicial
            atual = val_lance if val_lance > 0 else l.valor_inicial
            
            result.append({
                "id": l.id, 
                "descricao": l.descricao, 
                "valor_inicial": l.valor_inicial,
                "valor_atual": atual
            })
        return result
        
    except grpc.RpcError as e:
        raise HTTPException(status_code=503, detail=f"Erro de comunicação gRPC: {e.code()}")
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Erro interno: {e}")

@app.post("/leiloes")
async def criar_leilao(request: Request):
    """Recebe JSON do frontend e chama RPC CriarLeilao no MS Leilão."""
    data = await request.json()
    try:
        with grpc.insecure_channel(HOST_LEILAO) as channel:
            stub = leilao_pb2_grpc.AuctionServiceStub(channel)
            req = leilao_pb2.LeilaoData(
                descricao=data['descricao'], 
                valor_inicial=data['valor_inicial'],
                inicio=data['inicio'], 
                fim=data['fim']
            )
            resp = stub.CriarLeilao(req)
            return {"id": resp.id, "status": "criado"}
    except grpc.RpcError as e:
        raise HTTPException(status_code=500, detail=f"Falha gRPC: {e.details()}")

@app.post("/lances")
async def efetuar_lance(request: Request):
    """Recebe JSON do frontend e chama RPC ProcessarLance no MS Lance."""
    data = await request.json()
    try:
        with grpc.insecure_channel(HOST_LANCE) as channel:
            stub = lance_pb2_grpc.LanceServiceStub(channel)
            req = lance_pb2.LanceData(
                leilao_id=data['leilao_id'], 
                user_id=data['user_id'], 
                valor=data['valor']
            )
            resp = stub.ProcessarLance(req)
            
            # Trata erro de negócio retornado pelo gRPC
            if resp.status == "erro":
                raise HTTPException(status_code=400, detail=resp.message)
            
            # Se o lance foi aceito, inscrevemos o usuário automaticamente
            # para receber atualizações desse leilão.
            await manager.add_interest(data['user_id'], data['leilao_id'])

            return {"status": "ok", "message": resp.message}
            
    except grpc.RpcError as e:
         raise HTTPException(status_code=500, detail=f"Falha gRPC: {e.details()}")

# Endpoints locais para gerenciar estado de interesse do Gateway
@app.post("/leiloes/{leilao_id}/registrar/{user_id}")
async def registrar_interesse(leilao_id: int, user_id: str):
    await manager.add_interest(user_id, leilao_id)
    return {"status": "ok"}
    
@app.delete("/leiloes/{leilao_id}/registrar/{user_id}")
async def cancelar_interesse(leilao_id: int, user_id: str):
    await manager.remove_interest(user_id, leilao_id)
    return {"status": "ok"}

if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=5000)