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

# --- Configuração do Ciclo de Vida ---
@asynccontextmanager
async def lifespan(app: FastAPI):
    """
    Gerenciador de ciclo de vida do FastAPI.
    Código aqui é executado ANTES do servidor iniciar (startup).
    """
    print("INFO: Iniciando evento lifespan startup...")
    loop = asyncio.get_event_loop()
    # Inicia o consumidor RabbitMQ em uma thread separada
    consumer_thread = threading.Thread(
        target=rabbitmq_consumer, 
        args=(loop,), # Passa o loop asyncio principal para a thread
        daemon=True  # Garante que a thread morra se o app principal morrer
    )
    consumer_thread.start()
    print("INFO: Thread do consumidor RabbitMQ iniciada.")
    
    yield # O servidor roda aqui
    
    print("INFO: Evento lifespan shutdown.")

# --- Configuração da Aplicação FastAPI ---
app = FastAPI(lifespan=lifespan)

# Adiciona o middleware de CORS para permitir que o navegador (de outra origem)
# se conecte ao servidor na porta 5000.
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"], # Permite todas as origens (inseguro para produção)
    allow_credentials=True,
    allow_methods=["*"], # Permite todos os métodos (GET, POST, DELETE, etc)
    allow_headers=["*"], # Permite todos os cabeçalhos
)

# --- URLs dos Microsserviços Internos ---
MS_LEILAO_URL = "http://localhost:8001"
MS_LANCE_URL = "http://localhost:8002"


# --- Gerenciamento de Conexões SSE ---
class ConnectionManager:
    """
    Classe central que gerencia quem está conectado (SSE) e
    quais são seus interesses (em quais leilões estão inscritos).
    """
    def __init__(self):
        # Armazena conexões ativas: { "ana": [<Fila_Aba1>, <Fila_Aba2>], "bob": [<Fila_Aba1>] }
        self.active_connections: Dict[str, List[asyncio.Queue]] = {} 
        # Armazena interesses: { "ana": {1, 2}, "bob": {1} }
        self.client_interests: Dict[str, set] = {}
        # Lock para proteger o dicionário client_interests de acesso concorrente
        self.interests_lock = asyncio.Lock()

    async def connect(self, user_id: str) -> (str, asyncio.Queue):
        """
        Registra uma nova conexão SSE (ex: uma nova aba do navegador)
        para um usuário específico.
        """
        event_queue = asyncio.Queue()
        
        # Cria a lista de conexões
        if user_id not in self.active_connections:
            self.active_connections[user_id] = []
        
        # Cria o set de interesses (protegido por lock)
        async with self.interests_lock:
            if user_id not in self.client_interests:
                self.client_interests[user_id] = set()
            
        self.active_connections[user_id].append(event_queue)
        
        print(f"INFO: Cliente '{user_id}' conectou-se via SSE. Total de conexões para ele: {len(self.active_connections[user_id])}")
        return user_id, event_queue

    async def disconnect(self, user_id: str, event_queue: asyncio.Queue):
        """
        Remove uma conexão SSE específica (ex: fechar uma aba) de um usuário.
        Se for a última aba, limpa os interesses.
        """
        if user_id in self.active_connections:
            try:
                self.active_connections[user_id].remove(event_queue)
                print(f"INFO: Cliente '{user_id}' desconectou uma aba. Conexões restantes: {len(self.active_connections[user_id])}")
                
                # Se for a última conexão dele, limpa os interesses
                if not self.active_connections[user_id]:
                    del self.active_connections[user_id]
                    async with self.interests_lock:
                        if user_id in self.client_interests:
                            del self.client_interests[user_id]
                    print(f"INFO: Cliente '{user_id}' desconectou-se completamente.")
            except ValueError:
                pass # A fila não estava na lista (raro, mas seguro)
            
    async def add_interest(self, user_id: str, leilao_id: int):
        """Adiciona um interesse de leilão para um usuário (Thread-safe)"""
        async with self.interests_lock:
            if user_id not in self.client_interests:
                self.client_interests[user_id] = set()
            self.client_interests[user_id].add(leilao_id)
            print(f"INFO: Cliente '{user_id}' registrou interesse no leilão {leilao_id}.")

    async def remove_interest(self, user_id: str, leilao_id: int):
        """Remove um interesse de leilão para um usuário (Thread-safe)"""
        async with self.interests_lock:
            if user_id in self.client_interests and leilao_id in self.client_interests[user_id]:
                self.client_interests[user_id].remove(leilao_id)
                print(f"INFO: Cliente '{user_id}' removeu interesse no leilão {leilao_id}.")

    async def clear_interests_for_losers(self, leilao_id: int, winner_id: str):
        """
        Chamado quando um leilão termina ('leilao_vencedor').
        Remove o interesse do leilão de todos, *exceto* do vencedor.
        """
        print(f"INFO: Limpando interesses do leilão {leilao_id} (exceto para {winner_id})...")
        async with self.interests_lock:
            for user_id, interests in list(self.client_interests.items()):
                if user_id != winner_id and leilao_id in interests:
                    interests.remove(leilao_id)
                    print(f"  - Interesse removido para {user_id}")

    async def clear_interest_for_winner(self, leilao_id: int, winner_id: str):
        """
        Chamado quando o pagamento é processado ('status_pagamento').
        Remove o interesse do leilão para o vencedor.
        """
        print(f"INFO: Limpando interesse final do leilão {leilao_id} para o vencedor {winner_id}...")
        await self.remove_interest(user_id=winner_id, leilao_id=leilao_id)

    async def broadcast_event(self, event_name: str, data: dict):
        """
        Envia um evento (recebido do RabbitMQ) para os clientes SSE corretos.
        """
        leilao_id = data.get("leilao_id")
        user_id = data.get("user_id") # Para lance_invalidado
        vencedor_id = data.get("vencedor_id") # Para link_pagamento/status
        
        # Formata o payload que o sse-starlette espera: um dicionário
        event_payload = { "event": event_name, "data": json.dumps(data) }

        async def send_to_user(target_user_id: str):
            """Função auxiliar para enviar para todas as abas de um usuário."""
            if target_user_id in self.active_connections:
                for queue in self.active_connections.get(target_user_id, []):
                    await queue.put(event_payload)

        # --- Roteamento de Eventos ---
        target_user = None
        # Eventos Direcionados (enviados apenas para um usuário)
        if event_name == "lance_invalidado" and user_id:
            target_user = user_id
        elif event_name in ["link_pagamento", "status_pagamento"] and vencedor_id:
            target_user = vencedor_id
        
        if target_user:
            await send_to_user(target_user)
            return

        # Eventos de Broadcast (enviados para todos os interessados)
        async with self.interests_lock: # Usa lock para ler 'client_interests'
            if event_name in ["lance_validado", "leilao_vencedor"]:
                for client_id, interests in self.client_interests.items():
                    if leilao_id and leilao_id in interests:
                        await send_to_user(client_id)

# Instância única do nosso gerenciador
manager = ConnectionManager()

# --- Endpoint SSE ---
@app.get("/eventos")
async def sse_endpoint(request: Request, user_id: str = Query(...)):
    """
    Endpoint principal de Server-Sent Events.
    O navegador conecta aqui (ex: /eventos?user_id=ana).
    """
    
    # Registra a nova conexão/fila
    user_id, event_queue = await manager.connect(user_id)

    async def event_generator():
        """
        Gera eventos para o cliente.
        Envia um "ping" a cada 30s se não houver outras mensagens,
        para manter a conexão de rede ativa.
        """
        while True:
            try:
                # Espera por uma mensagem da fila por 30 segundos
                message = await asyncio.wait_for(event_queue.get(), timeout=30.0)
                yield message
            
            except asyncio.TimeoutError:
                # Se nada chegou em 30s, envia um "ping"
                yield { "event": "ping", "data": "keep-alive" }
            
            except asyncio.CancelledError:
                # Cliente se desconectou (ex: fechou a aba)
                await manager.disconnect(user_id, event_queue)
                print(f"INFO: Conexão SSE para '{user_id}' fechada (generator cancelled).")
                break

    return EventSourceResponse(event_generator())


# --- Endpoints REST ---

@app.get("/", response_class=HTMLResponse)
async def read_root():
    """Serve o arquivo principal do frontend."""
    try:
        # Abre o arquivo especificando a codificação UTF-8
        with open("index.html", "r", encoding="utf-8") as f:
            return HTMLResponse(content=f.read(), status_code=200)
    except FileNotFoundError:
        return HTMLResponse(content="<h1>API Gateway Leilão</h1><p>Arquivo index.html não encontrado.</p>", status_code=200)

@app.get("/leiloes/ativos")
async def consultar_leiloes_ativos():
    """
    Orquestra a busca de leilões ativos.
    1. Busca a lista de leilões do MS Leilão.
    2. Busca os lances atuais do MS Lance.
    3. Combina os dados para mostrar o "lance atual" correto.
    """
    async with httpx.AsyncClient() as client:
        try:
            # 1. Faz as duas chamadas em paralelo
            leiloes_response_task = client.get(f"{MS_LEILAO_URL}/leiloes")
            lances_response_task = client.get(f"{MS_LANCE_URL}/lances/atuais")
            
            leiloes_response, lances_response = await asyncio.gather(
                leiloes_response_task,
                lances_response_task
            )
            
            leiloes_response.raise_for_status()
            lances_response.raise_for_status()
            
            leiloes_ativos = leiloes_response.json()
            lances_atuais = lances_response.json() 
            
            # Combina os dados
            merged_list = []
            for leilao in leiloes_ativos:
                leilao_id_str = str(leilao['id'])
                lance_info = lances_atuais.get(leilao_id_str)
                
                # Define 'valor_atual' como o lance mais alto ou o inicial
                if lance_info and lance_info.get('valor', 0) > 0:
                    leilao['valor_atual'] = lance_info['valor']
                else:
                    leilao['valor_atual'] = leilao['valor_inicial']
                
                merged_list.append(leilao)
            return merged_list
        
        except httpx.RequestError as exc:
            raise HTTPException(status_code=503, detail=f"Erro ao comunicar com microsserviços: {exc}")
        except Exception as e:
            raise HTTPException(status_code=500, detail=f"Erro interno ao processar leilões: {e}")

@app.post("/leiloes")
async def criar_leilao(request: Request):
    """Repassa o pedido de criação de leilão para o MS Leilão."""
    leilao_data = await request.json()
    async with httpx.AsyncClient() as client:
        try:
            response = await client.post(f"{MS_LEILAO_URL}/leiloes", json=leilao_data)
            response.raise_for_status() # Lança erro se o MS Leilão retornar 422 (validação)
            return response.json()
        except httpx.HTTPStatusError as exc:
            # Repassa o erro de validação (ex: 422) do MS para o frontend
            detail = exc.response.json().get("detail", exc.response.text)
            raise HTTPException(status_code=exc.response.status_code, detail=detail)
        except httpx.RequestError as exc:
            raise HTTPException(status_code=503, detail=f"Erro ao comunicar com MS Leilão: {exc}")
        

@app.post("/lances")
async def efetuar_lance(request: Request):
    """Repassa o pedido de lance para o MS Lance."""
    lance_data = await request.json()
    async with httpx.AsyncClient() as client:
        try:
            response = await client.post(f"{MS_LANCE_URL}/lances", json=lance_data)
            response.raise_for_status() # Lança erro se o MS Lance retornar 400 (lance inválido)
            return response.json()
        except httpx.HTTPStatusError as exc:
            # Repassa o erro de lance inválido (400) do MS para o frontend
            detail = exc.response.json().get("detail", exc.response.text)
            raise HTTPException(status_code=exc.response.status_code, detail=detail)
        except httpx.RequestError as exc:
            raise HTTPException(status_code=503, detail=f"Erro ao comunicar com MS Lance: {exc}")

@app.post("/leiloes/{leilao_id}/registrar/{user_id}")
async def registrar_interesse(leilao_id: int, user_id: str):
    """Registra o interesse de um usuário em um leilão."""
    await manager.add_interest(user_id, leilao_id)
    return {"status": "ok", "message": f"Interesse registrado para cliente '{user_id}' no leilão {leilao_id}"}
    
@app.delete("/leiloes/{leilao_id}/registrar/{user_id}")
async def cancelar_interesse(leilao_id: int, user_id: str):
    """Remove o interesse de um usuário em um leilão."""
    await manager.remove_interest(user_id, leilao_id)
    return {"status": "ok", "message": f"Interesse cancelado para cliente '{user_id}' no leilão {leilao_id}"}


# --- Consumidor RabbitMQ ---

def rabbitmq_consumer(loop: asyncio.AbstractEventLoop):
    """
    Função que roda na thread daemon.
    Conecta ao RabbitMQ e consome eventos.
    """
    
    # Filas Ponto-a-Ponto que este gateway consome
    queues_to_consume = [
        "lance_validado", "lance_invalidado",
        "link_pagamento", "status_pagamento"
    ]
    
    # Callback para filas P2P
    def callback_p2p(ch, method, properties, body):
        event_name = method.routing_key # ex: "lance_validado"
        data = json.loads(body)
        print(f"RABBITMQ (P2P): Recebido '{event_name}' com dados: {data}")
        
        # Envia o evento para o cliente
        asyncio.run_coroutine_threadsafe(
            manager.broadcast_event(event_name, data),
            loop
        )
        
        # Se for 'status_pagamento', limpa o interesse do vencedor
        if event_name == "status_pagamento":
            leilao_id = data.get("leilao_id")
            vencedor_id = data.get("vencedor_id")
            if leilao_id and vencedor_id:
                asyncio.run_coroutine_threadsafe(
                    manager.clear_interest_for_winner(leilao_id, vencedor_id),
                    loop
                )
        
        ch.basic_ack(delivery_tag=method.delivery_tag)

    # Callback para exchange
    def callback_fanout(ch, method, properties, body):
        event_name = "leilao_vencedor"
        data = json.loads(body)
        print(f"RABBITMQ (FANOUT): Recebido '{event_name}' com dados: {data}")
        
        # Envia o evento para os clientes
        asyncio.run_coroutine_threadsafe(
            manager.broadcast_event(event_name, data),
            loop
        )
        
        # Limpa o interesse de todos os perdedores
        leilao_id = data.get("leilao_id")
        vencedor_id = data.get("vencedor_id") # Pode ser None se não houver vencedor
        
        if leilao_id:
            asyncio.run_coroutine_threadsafe(
                manager.clear_interests_for_losers(leilao_id, vencedor_id),
                loop
            )

        ch.basic_ack(delivery_tag=method.delivery_tag)

    # Loop de reconexão
    while True:
        try:
            connection = pika.BlockingConnection(pika.ConnectionParameters(host='localhost'))
            channel = connection.channel()
            print("INFO: Consumidor RabbitMQ conectado.")

            # Configura consumo das filas P2P
            for queue_name in queues_to_consume:
                channel.queue_declare(queue=queue_name)
                channel.basic_consume(queue=queue_name, on_message_callback=callback_p2p)

            # Configura consumo do exchange 'leilao_vencedor'
            channel.exchange_declare(exchange="leilao_vencedor_exchange", exchange_type="fanout")
            result = channel.queue_declare(queue="", exclusive=True)
            queue_name_vencedor = result.method.queue
            channel.queue_bind(exchange="leilao_vencedor_exchange", queue=queue_name_vencedor)
            channel.basic_consume(queue=queue_name_vencedor, on_message_callback=callback_fanout)

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