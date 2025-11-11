import pika
import json
import threading
import uvicorn
import copy
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
from typing import Dict

# --- Modelos de Dados (Pydantic) ---

class LanceCreate(BaseModel):
    """Modelo Pydantic para *receber* um lance via API REST."""
    leilao_id: int
    user_id: str
    valor: float

# --- Microsserviço de Lances ---
class MSBid:
    """
    Classe principal do microsserviço de Lances.
    Gerencia o estado dos lances e valida novas propostas.
    """
    def __init__(self, host="localhost", exchange_iniciado="leilao_iniciado_exchange"):
        # --- Armazenamento de Estado ---
        # { leilao_id: True/False }
        self.leiloes_ativos: Dict[int, bool] = {}
        # { leilao_id: {"user_id": str, "valor": float, "lance_minimo": float} }
        self.lances_mais_altos: Dict[int, dict] = {}
        # Lock para proteger os dicionários de estado de acesso concorrente
        self.lock = threading.Lock()

        self.host = host
        self.exchange_iniciado = exchange_iniciado

        try:
            self.connection = pika.BlockingConnection(
                pika.ConnectionParameters(self.host)
            )
            self.channel = self.connection.channel()

            # --- Configuração de Consumo (RabbitMQ) ---
            
            # Consome 'leilao_iniciado' (via exchange fanout)
            self.channel.exchange_declare(
                exchange=self.exchange_iniciado, exchange_type="fanout"
            )
            result = self.channel.queue_declare(queue="", exclusive=True) # Fila exclusiva
            self.queue_name_iniciado = result.method.queue
            self.channel.queue_bind(
                exchange=self.exchange_iniciado, queue=self.queue_name_iniciado
            )

            # Consome 'leilao_finalizado' (via fila P2P)
            self.channel.queue_declare(queue="leilao_finalizado")

            # --- Configuração de Publicação (RabbitMQ) ---
            self.channel.queue_declare(queue="lance_validado")
            self.channel.queue_declare(queue="lance_invalidado")
            # Publica 'leilao_vencedor' em um exchange fanout
            self.channel.exchange_declare(
                exchange="leilao_vencedor_exchange", 
                exchange_type="fanout"
            )
            
            print("--- [MS_Bid] Conectado ao RabbitMQ ---")

        except pika.exceptions.AMQPConnectionError as e:
            print(
                f"--- [MS_Bid] Erro: Não foi possível estabelecer conexão com o RabbitMQ em '{self.host}'. ---"
            )
            if hasattr(self, 'connection') and self.connection and self.connection.is_open:
                self.connection.close()
            raise e

    def _publish_message(self, routing_key: str, body: dict, exchange=""):
        """Função auxiliar para publicar mensagens JSON."""
        self.channel.basic_publish(
            exchange=exchange,
            routing_key=routing_key,
            body=json.dumps(body)
        )
        print(f" [MS_Bid] Publicado '{routing_key}' (Exchange: '{exchange}'): {body}")

    # --- Callbacks do Consumidor RabbitMQ ---

    def callback_leilao_iniciado(self, ch, method, properties, body):
        """
        Callback para 'leilao_iniciado'.
        Armazena o estado inicial do leilão (ativo, lance mínimo).
        """
        leilao = json.loads(body)
        leilao_id = leilao["id"]
        
        with self.lock: # Protege os dicionários de estado
            self.leiloes_ativos[leilao_id] = True
            self.lances_mais_altos[leilao_id] = {
                "user_id": None,
                "valor": 0.0,
                "lance_minimo": leilao.get("lance_minimo", 0.0),
            }
        
        print(
            f"[MS_Bid] [INFO] Leilão {leilao_id} agora está ATIVO com lance mínimo de R${leilao.get('lance_minimo', 0.0):.2f}."
        )
        ch.basic_ack(delivery_tag=method.delivery_tag)

    def callback_leilao_finalizado(self, ch, method, properties, body):
        """
        Callback para 'leilao_finalizado'.
        Determina o vencedor e publica o evento 'leilao_vencedor'.
        """
        data = json.loads(body)
        leilao_id = data["id"]
        
        with self.lock: # Protege os dicionários de estado
            if leilao_id in self.leiloes_ativos:
                self.leiloes_ativos[leilao_id] = False
                print(
                    f"[MS_Bid] [INFO] Leilão {leilao_id} agora está ENCERRADO. Determinando vencedor..."
                )

                vencedor_info = self.lances_mais_altos.get(leilao_id)
                
                message = {
                    "leilao_id": leilao_id,
                    "vencedor_id": vencedor_info["user_id"] if vencedor_info else None,
                    "valor": vencedor_info["valor"] if vencedor_info else 0.0,
                }
                
                # Publica no exchange para Gateway e MS Pagamento
                self._publish_message(
                    routing_key="",
                    body=message, 
                    exchange="leilao_vencedor_exchange"
                )

        ch.basic_ack(delivery_tag=method.delivery_tag)

    # --- Loop de Consumo (Thread) ---

    def start_consuming(self):
        """Inicia o consumo de mensagens do RabbitMQ em loop (em uma thread)."""
        print("--- [MS_Bid] Consumidor RabbitMQ iniciado em thread separada... ---")
        try:
            self.channel.basic_consume(
                queue=self.queue_name_iniciado,
                on_message_callback=self.callback_leilao_iniciado,
            )
            self.channel.basic_consume(
                queue="leilao_finalizado",
                on_message_callback=self.callback_leilao_finalizado,
            )
            self.channel.start_consuming()
        except KeyboardInterrupt:
            print("\n--- [MS_Bid] Consumidor RabbitMQ encerrando. ---")
        finally:
            if self.connection and self.connection.is_open:
                self.connection.close()
                print("--- [MS_Bid] Conexão RabbitMQ fechada. ---")

    # --- Lógica da API REST ---
    
    def rest_process_bid(self, lance: LanceCreate):
        """
        Processa um lance recebido via REST (do API Gateway).
        Valida o lance e publica 'lance_validado' ou 'lance_invalidado'.
        """
        leilao_id = lance.leilao_id
        user_id = lance.user_id
        valor = lance.valor

        # Usa o lock para ler/escrever o estado
        with self.lock:
            # Validação 1: Leilão está ativo?
            if not self.leiloes_ativos.get(leilao_id, False):
                motivo = f"Leilão {leilao_id} não está ativo ou não existe."
                self._publish_message("lance_invalidado", {"leilao_id": leilao_id, "user_id": user_id, "motivo": motivo})
                raise HTTPException(status_code=400, detail=motivo)
            
            ultimo_lance_info = self.lances_mais_altos.get(leilao_id)
            valor_atual = ultimo_lance_info["valor"]
            lance_minimo = ultimo_lance_info["lance_minimo"]

            # Validação 2: É maior que o lance mínimo (se for o primeiro lance)?
            if valor_atual == 0.0 and valor < lance_minimo:
                motivo = f"Seu lance (R${valor:.2f}) é menor que o lance mínimo inicial (R${lance_minimo:.2f})."
                self._publish_message("lance_invalidado", {"leilao_id": leilao_id, "user_id": user_id, "motivo": motivo})
                raise HTTPException(status_code=400, detail=motivo)

            # Validação 3: É maior que o lance atual (se não for o primeiro)?
            if valor_atual > 0.0 and valor <= valor_atual:
                motivo = f"Seu lance (R${valor:.2f}) não é maior que o lance atual (R${valor_atual:.2f})."
                self._publish_message("lance_invalidado", {"leilao_id": leilao_id, "user_id": user_id, "motivo": motivo})
                raise HTTPException(status_code=400, detail=motivo)

            # --- Lance é Válido ---
            print(f" [MS_Bid] REST: Lance de {user_id} para o leilão {leilao_id} é VÁLIDO.")
            
            # Atualiza o estado interno
            self.lances_mais_altos[leilao_id]["user_id"] = user_id
            self.lances_mais_altos[leilao_id]["valor"] = valor

            # Publica o evento 'lance_validado'
            message = {"leilao_id": leilao_id, "user_id": user_id, "valor": valor}
            self._publish_message("lance_validado", message)

            return {"status": "ok", "message": "Lance validado e publicado."}
        
    def rest_get_all_highest_bids(self):
        """Retorna uma cópia thread-safe do dicionário de lances mais altos."""
        with self.lock:
            # Retorna uma cópia para evitar que o Gateway itere sobre um
            # dicionário que está sendo modificado ao mesmo tempo.
            return copy.deepcopy(self.lances_mais_altos)

# --- Configuração do Servidor FastAPI ---
app = FastAPI()

try:
    bid_service = MSBid()
except Exception as e:
    print(f"Falha fatal ao inicializar o MSBid. Encerrando. Erro: {e}")
    exit(1)

# --- Endpoints da API REST ---

@app.get("/lances/atuais")
def get_all_bids():
    """
    Endpoint REST para o API Gateway consultar o estado
    atual de todos os lances.
    """
    return bid_service.rest_get_all_highest_bids()

@app.post("/lances")
def create_lance(lance: LanceCreate):
    """
    Endpoint REST para receber um novo lance do API Gateway.
    """
    # A exceção HTTPException (lançada por rest_process_bid) 
    # será automaticamente convertida em uma resposta HTTP 400.
    return bid_service.rest_process_bid(lance)

# --- Inicialização ---
if __name__ == "__main__":
    try:
        # Inicia o consumidor RabbitMQ em uma thread separada
        consumer_thread = threading.Thread(
            target=bid_service.start_consuming, daemon=True
        )
        consumer_thread.start()

        # Inicia o servidor web (FastAPI) na thread principal
        print("--- [MS_Bid] Iniciando servidor web FastAPI na porta 8002 ---")
        uvicorn.run(app, host="0.0.0.0", port=8002)

    except KeyboardInterrupt:
        print("--- [MS_Bid] Encerrando servidor web... ---")