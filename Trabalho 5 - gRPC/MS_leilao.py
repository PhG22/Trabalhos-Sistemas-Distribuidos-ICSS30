import time
import threading
import json
import grpc
from concurrent import futures
from datetime import datetime, timedelta
from typing import List
import queue

import leilao_pb2
import leilao_pb2_grpc

# Modelo interno simplificado
class LeilaoInterno:
    def __init__(self, id, descricao, valor_inicial, inicio, fim, status="pendente"):
        self.id = id
        self.descricao = descricao
        self.valor_inicial = valor_inicial
        self.inicio = inicio
        self.fim = fim
        self.status = status

class AuctionService(leilao_pb2_grpc.AuctionServiceServicer):
    def __init__(self):
        self.leiloes: List[LeilaoInterno] = []
        self.leilao_id_counter = 0
        self.lock = threading.Lock() # Lock para proteger a lista de leilões
        
        # Conjunto de filas para enviar eventos de streaming para o Gateway
        self.event_queues = set()
        
        self._populate_initial_data()
        
        # Thread de monitoramento do tempo dos leilões
        threading.Thread(target=self.monitor_auctions, daemon=True).start()

    def _populate_initial_data(self):
        """Cria leilões de exemplo para testes rápidos."""
        iniciais = [
            {"descricao": "1155 do ET", "seconds": 30, "valor": 10.0},
            {"descricao": "Carta MTG: Tifa", "seconds": 35, "valor": 100.0},
        ]
        with self.lock:
            for item in iniciais:
                self.leilao_id_counter += 1
                l = LeilaoInterno(
                    self.leilao_id_counter, 
                    item["descricao"], 
                    item["valor"],
                    datetime.now() + timedelta(seconds=item["seconds"]),
                    datetime.now() + timedelta(minutes=2, seconds=item["seconds"])
                )
                self.leiloes.append(l)

    def _broadcast_event(self, tipo, payload):
        """
        Envia uma mensagem para todas as filas de eventos ativas (conectadas pelo Gateway).
        """
        event_msg = leilao_pb2.Evento(tipo=tipo, payload_json=json.dumps(payload))
        dead_queues = set()
        for q in self.event_queues:
            try:
                q.put(event_msg)
            except:
                dead_queues.add(q)
        for q in dead_queues:
            self.event_queues.remove(q)

    # --- Implementação dos Métodos gRPC definidos no .proto ---

    def CriarLeilao(self, request, context):
        """Recebe dados, cria leilão em memória e retorna o objeto."""
        with self.lock:
            self.leilao_id_counter += 1
            inicio_dt = datetime.fromisoformat(request.inicio)
            fim_dt = datetime.fromisoformat(request.fim)
            
            novo = LeilaoInterno(self.leilao_id_counter, request.descricao, request.valor_inicial, inicio_dt, fim_dt)
            self.leiloes.append(novo)
            
            print(f"[MS Leilão] Criado leilão ID {novo.id}")
            return leilao_pb2.LeilaoData(
                id=novo.id, descricao=novo.descricao, valor_inicial=novo.valor_inicial,
                inicio=request.inicio, fim=request.fim, status=novo.status
            )

    def GetLeiloesAtivos(self, request, context):
        """Retorna um STREAM de leilões ativos."""
        with self.lock:
            for l in self.leiloes:
                if l.status == "ativo":
                    yield leilao_pb2.LeilaoData(
                        id=l.id, descricao=l.descricao, valor_inicial=l.valor_inicial,
                        status=l.status
                    )

    def SubscribeEventos(self, request, context):
        """
        O Gateway chama este método para ficar 'ouvindo' eventos.
        O servidor mantém o loop rodando enquanto a conexão existir.
        """
        print("[MS Leilão] Novo cliente inscrito para eventos.")
        q = queue.Queue()
        self.event_queues.add(q)
        try:
            while True:
                # Bloqueia até ter um evento para enviar
                evento = q.get()
                yield evento
        except Exception:
            self.event_queues.remove(q)

    # --- Lógica de Monitoramento Temporal ---
    def monitor_auctions(self):
        """
        Loop infinito que verifica o estado dos leilões a cada segundo.
        É responsável por comunicar o início e fim dos leilões.
        """
        # Canal persistente para falar com o MS Lance
        channel_lance = grpc.insecure_channel('localhost:8002')
        stub_lance = leilao_pb2_grpc.LanceServiceStub(channel_lance)

        print("--- [MS Leilão] Monitor iniciado ---")
        while True:
            now = datetime.now()
            with self.lock:
                for l in self.leiloes:
                    # Início de Leilão
                    if l.status == "pendente" and now >= l.inicio:
                        l.status = "ativo"
                        payload = {
                            "id": l.id, "descricao": l.descricao, 
                            "inicio": l.inicio.isoformat(), "fim": l.fim.isoformat(),
                            "lance_minimo": l.valor_inicial
                        }
                        print(f"[MS Leilão] Iniciado: {l.id}")
                        
                        # Avisa o Gateway para atualizar frontends
                        self._broadcast_event("leilao_iniciado", payload)
                        
                        # Comanda o MS Lance a ativar o leilão
                        try:
                            stub_lance.NotificarInicioLeilao(
                                leilao_pb2.LeilaoData(id=l.id, valor_inicial=l.valor_inicial)
                            )
                        except grpc.RpcError:
                            print(f"[Erro] Falha ao notificar início do leilão {l.id} ao MS Lance")
                    
                    # Fim de Leilão
                    elif l.status == "ativo" and now >= l.fim:
                        l.status = "encerrado"
                        print(f"[MS Leilão] Finalizado: {l.id}")
                        
                        payload = {"id": l.id}
                        # Avisa o Gateway
                        self._broadcast_event("leilao_finalizado", payload)
                        
                        # Comanda o MS Lance a finalizar e calcular vencedor
                        try:
                            stub_lance.NotificarFimLeilao(leilao_pb2.LeilaoData(id=l.id))
                        except grpc.RpcError:
                            print(f"[Erro] Falha ao notificar fim do leilão {l.id} ao MS Lance")

            time.sleep(1)

if __name__ == "__main__":
    # Configura e inicia o servidor gRPC
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
    leilao_pb2_grpc.add_AuctionServiceServicer_to_server(AuctionService(), server)
    server.add_insecure_port('[::]:8001')
    print("--- [MS Leilão] Servidor gRPC rodando na porta 8001 ---")
    server.start()
    server.wait_for_termination()