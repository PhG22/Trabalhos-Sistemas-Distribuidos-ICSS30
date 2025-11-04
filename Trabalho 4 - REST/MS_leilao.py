import pika
import json
import time
import threading
import uvicorn
from fastapi import FastAPI
from pydantic import BaseModel
from datetime import datetime, timedelta
from typing import List

# --- Modelos de Dados (Pydantic) ---
# Define a estrutura de dados esperada para a criação de um leilão via REST
class LeilaoCreate(BaseModel):
    descricao: str
    valor_inicial: float
    inicio: datetime
    fim: datetime

# Define a estrutura de dados interna, incluindo campos gerenciados pelo serviço
class LeilaoInDB(LeilaoCreate):
    id: int
    status: str = "pendente"

# --- Microsserviço de Leilão ---
class MS_Auctions:

    def __init__(self, host="localhost", exchange="leilao_iniciado_exchange"):
        # Armazenamento em memória para os leilões
        self.leiloes: List[LeilaoInDB] = []
        self.leilao_id_counter = 0
        # Lock para garantir que a lista de leilões não seja modificada e lida ao mesmo tempo
        self.lock = threading.Lock()
        
        self.host = host
        self.exchange = exchange
        
        try:
            # --- Conexão RabbitMQ ---
            self.connection = pika.BlockingConnection(
                pika.ConnectionParameters(self.host)
            )
            self.channel = self.connection.channel()

            # Declara exchange para leilões iniciados (conforme PDF [cite: 248])
            self.channel.exchange_declare(
                exchange=self.exchange, exchange_type="fanout"
            )

            # Declara fila para leilões finalizados (conforme PDF [cite: 249])
            self.channel.queue_declare(queue="leilao_finalizado")
            print("--- [MS_Auctions] Conectado ao RabbitMQ ---")
            
            # --- Popula dados iniciais para teste (do seu arquivo original) ---
            self._populate_initial_data()

        except pika.exceptions.AMQPConnectionError as e:
            print(
                f"--- [MS_Auctions] Erro: Não foi possível estabelecer conexão com o RabbitMQ em '{self.host}'. ---"
            )
            if hasattr(self, 'connection') and self.connection and self.connection.is_open:
                self.connection.close()
            # Se não puder conectar ao Pika, o serviço não pode funcionar.
            raise e
            
    def _populate_initial_data(self):
        """Adiciona leilões de exemplo na inicialização."""
        leiloes_iniciais = [
            {
                "descricao": "1155 do ET",
                "inicio": datetime.now() + timedelta(seconds=30),
                "fim": datetime.now() + timedelta(minutes=2, seconds=30),
                "valor_inicial": 10.0,
            },
            {
                "descricao": "Carta MTG: Tifa, Martial Artist (Surge Foil)",
                "inicio": datetime.now() + timedelta(seconds=35),
                "fim": datetime.now() + timedelta(minutes=2.1, seconds=35),
                "valor_inicial": 100.0,
            },
        ]
        
        with self.lock:
            for leilao_data in leiloes_iniciais:
                self.leilao_id_counter += 1
                leilao_obj = LeilaoInDB(
                    id=self.leilao_id_counter,
                    status="pendente",
                    **leilao_data
                )
                self.leiloes.append(leilao_obj)
            print(f"--- [MS_Auctions] {len(self.leiloes)} leilões de teste carregados ---")

    # --- Métodos de Publicação (RabbitMQ) ---

    def _start_auction(self, leilao: LeilaoInDB):
        """Publica o evento 'leilao_iniciado'."""
        leilao.status = "ativo"
        
        message = {
            "id": leilao.id,
            "descricao": leilao.descricao,
            "inicio": leilao.inicio.isoformat(),
            "fim": leilao.fim.isoformat(),
            "lance_minimo": leilao.valor_inicial # Nome do campo no MS_Lance
        }

        self.channel.basic_publish(
            exchange=self.exchange,
            routing_key="",  # fanout ignora a routing_key
            body=json.dumps(message),
        )
        print(
            f" [MS_Auctions] Leilão {leilao.id} INICIADO: {leilao.descricao}"
        )

    def _end_auction(self, leilao: LeilaoInDB):
        """Publica o evento 'leilao_finalizado'."""
        leilao.status = "encerrado"
        message = {"id": leilao.id}

        self.channel.basic_publish(
            exchange="",
            routing_key="leilao_finalizado",
            body=json.dumps(message),
        )
        print(f" [MS_Auctions] Leilão {leilao.id} FINALIZADO.")

    # --- Loop de Monitoramento (Thread) ---

    def monitor_auctions(self):
        """Loop principal que verifica o status dos leilões."""
        print(
            "--- [MS_Auctions] Monitor de leilões iniciado em thread separada... ---"
        )
        try:
            while True:
                now = datetime.now()
                
                # Usamos um lock para iterar na lista com segurança
                with self.lock:
                    for leilao in self.leiloes:
                        if leilao.status == "pendente" and now >= leilao.inicio:
                            self._start_auction(leilao)
                        elif leilao.status == "ativo" and now >= leilao.fim:
                            self._end_auction(leilao)
                
                time.sleep(1)
        except KeyboardInterrupt:
            print("\n--- [MS_Auctions] Monitor de leilões encerrando. ---")
        finally:
            if self.connection and self.connection.is_open:
                self.connection.close()
                print("--- [MS_Auctions] Conexão RabbitMQ fechada. ---")

    # --- Métodos para a API REST ---
    
    def rest_create_leilao(self, leilao_data: LeilaoCreate) -> LeilaoInDB:
        """Cria um novo leilão a partir de uma requisição REST."""
        with self.lock:
            self.leilao_id_counter += 1
            new_leilao = LeilaoInDB(
                id=self.leilao_id_counter,
                status="pendente",
                **leilao_data.dict() # Converte modelo Pydantic para dict
            )
            self.leiloes.append(new_leilao)
            print(f"--- [MS_Auctions] REST: Leilão {new_leilao.id} criado.")
            return new_leilao

    def rest_get_active_auctions(self) -> List[LeilaoInDB]:
        """Retorna leilões ativos para uma requisição REST."""
        # O PDF [cite: 230] menciona "leilões ativos".
        # Vamos retornar os que estão com status 'ativo'.
        with self.lock:
            ativos = [leilao for leilao in self.leiloes if leilao.status == 'ativo']
        return ativos

# --- Configuração do Servidor FastAPI ---

# Cria a instância da aplicação FastAPI
app = FastAPI()

# Cria uma instância única do nosso serviço
try:
    auction_service = MS_Auctions()
except Exception as e:
    print(f"Falha fatal ao inicializar o MS_Auctions. Encerrando. Erro: {e}")
    exit(1)

# --- Endpoints da API REST ---

@app.post("/leiloes", response_model=LeilaoInDB)
def create_leilao(leilao_data: LeilaoCreate):
    """
    Endpoint REST para criar um novo leilão.
    Recebe os dados do leilão e os repassa ao serviço[cite: 229, 244].
    """
    return auction_service.rest_create_leilao(leilao_data)


@app.get("/leiloes", response_model=List[LeilaoInDB])
def get_active_auctions():
    """
    Endpoint REST para consultar leilões ativos[cite: 230, 245].
    (Nota: O PDF [cite: 230] menciona 'valor inicial ou último lance'. 
    Este MS só conhece o 'valor_inicial' (lance_minimo). 
    O 'último lance' só é conhecido pelo MS Lance.)
    """
    return auction_service.rest_get_active_auctions()
    
# --- Inicialização ---

if __name__ == "__main__":
    try:
        # 1. Inicia o monitor de leilões (Pika) em uma thread separada
        monitor_thread = threading.Thread(
            target=auction_service.monitor_auctions, daemon=True
        )
        monitor_thread.start()
        
        # 2. Inicia o servidor web (FastAPI) na thread principal
        # Use a porta 8001 (ou outra de sua escolha)
        print("--- [MS_Auctions] Iniciando servidor web FastAPI na porta 8001 ---")
        uvicorn.run(app, host="0.0.0.0", port=8001)
        
    except KeyboardInterrupt:
        print("--- [MS_Auctions] Encerrando servidor web... ---")