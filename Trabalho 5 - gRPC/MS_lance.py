import threading
import json
import copy
import grpc
from concurrent import futures
from typing import Dict

import leilao_pb2
import leilao_pb2_grpc
import queue

class LanceService(leilao_pb2_grpc.LanceServiceServicer):
    def __init__(self):
        # Estado em memória
        self.leiloes_ativos: Dict[int, bool] = {}
        self.lances_mais_altos: Dict[int, dict] = {}
        self.lock = threading.Lock()
        
        self.event_queues = set()

        # Canal para falar com MS Pagamento
        self.channel_pagamento = grpc.insecure_channel('localhost:8003')
        self.stub_pagamento = leilao_pb2_grpc.PaymentServiceStub(self.channel_pagamento)

    def _broadcast_event(self, tipo, payload):
        """Envia eventos para o Gateway."""
        event_msg = leilao_pb2.Evento(tipo=tipo, payload_json=json.dumps(payload))
        dead_queues = set()
        for q in self.event_queues:
            try:
                q.put(event_msg)
            except:
                dead_queues.add(q)
        for q in dead_queues:
            self.event_queues.remove(q)

    # --- Implementação gRPC ---

    def SubscribeEventos(self, request, context):
        """Stream de eventos (lances validados, etc)."""
        print("[MS Lance] Novo cliente inscrito para eventos.")
        q = queue.Queue()
        self.event_queues.add(q)
        try:
            while True:
                yield q.get()
        except Exception:
            self.event_queues.remove(q)

    def GetLancesAtuais(self, request, context):
        """Retorna snapshot dos valores atuais para o Gateway."""
        with self.lock:
            snapshot = copy.deepcopy(self.lances_mais_altos)
        
        for lid, data in snapshot.items():
            yield leilao_pb2.LeilaoData(id=lid, valor_atual=data['valor'])

    def ProcessarLance(self, request, context):
        """
        Recebe um lance do Gateway e aplica regras de negócio.
        Retorna sucesso ou erro imediatamente.
        """
        leilao_id = request.leilao_id
        user_id = request.user_id
        valor = request.valor
        
        with self.lock:
            # Regra 1: Leilão deve estar ativo
            if not self.leiloes_ativos.get(leilao_id, False):
                msg = f"Leilão {leilao_id} não está ativo."
                self._broadcast_event("lance_invalidado", {"leilao_id": leilao_id, "user_id": user_id, "motivo": msg})
                return leilao_pb2.LanceResponse(status="erro", message=msg)

            info = self.lances_mais_altos.get(leilao_id, {"valor": 0.0, "lance_minimo": 0.0})
            atual = info["valor"]
            minimo = info.get("lance_minimo", 0.0)

            # Regra 2: Deve ser maior que o mínimo (se for o primeiro)
            if atual == 0.0 and valor < minimo:
                msg = f"Lance menor que o mínimo (R${minimo})."
                self._broadcast_event("lance_invalidado", {"leilao_id": leilao_id, "user_id": user_id, "motivo": msg})
                return leilao_pb2.LanceResponse(status="erro", message=msg)
            
            # Regra 3: Deve ser maior que o lance atual
            if valor <= atual:
                msg = f"Lance não supera o atual (R${atual})."
                self._broadcast_event("lance_invalidado", {"leilao_id": leilao_id, "user_id": user_id, "motivo": msg})
                return leilao_pb2.LanceResponse(status="erro", message=msg)

            # Lance Aceito
            self.lances_mais_altos[leilao_id] = {"user_id": user_id, "valor": valor, "lance_minimo": minimo}
            payload = {"leilao_id": leilao_id, "user_id": user_id, "valor": valor}
            print(f"[MS Lance] Lance aceito: {payload}")
            self._broadcast_event("lance_validado", payload)
            return leilao_pb2.LanceResponse(status="ok", message="Lance aceito")

    def NotificarInicioLeilao(self, request, context):
        """Chamado pelo MS Leilão para 'ligar' o leilão."""
        lid = request.id
        lance_minimo = request.valor_inicial
        print(f"[MS Lance] Recebida notificação de INÍCIO do leilão {lid} (Mínimo: {lance_minimo})")
        
        with self.lock:
            self.leiloes_ativos[lid] = True
            self.lances_mais_altos[lid] = {
                "user_id": None, 
                "valor": 0.0, 
                "lance_minimo": lance_minimo
            }
            
        return leilao_pb2.Empty()

    def NotificarFimLeilao(self, request, context):
        """
        Chamado pelo MS Leilão para encerrar.
        Determina o vencedor e chama o MS Pagamento.
        """
        lid = request.id
        print(f"[MS Lance] Recebida notificação de FIM do leilão {lid}")
        
        vencedor_id = None
        valor = 0.0
        
        with self.lock:
            self.leiloes_ativos[lid] = False
            info = self.lances_mais_altos.get(lid)
            if info:
                vencedor_id = info['user_id']
                valor = info['valor']

        # Avisa o Gateway via Stream para atualizar UI e mostrar vencedor
        payload = {"leilao_id": lid, "vencedor_id": vencedor_id, "valor": valor}
        self._broadcast_event("leilao_vencedor", payload)
        
        # Se tiver vencedor, chama MS Pagamento DIRETAMENTE via gRPC
        if vencedor_id:
            print(f"[MS Lance] Enviando vencedor {vencedor_id} para MS Pagamento...")
            try:
                self.stub_pagamento.ProcessarPagamento(
                    leilao_pb2.PaymentData(leilao_id=lid, vencedor_id=vencedor_id, valor=valor)
                )
            except grpc.RpcError as e:
                print(f"[Erro] Falha ao chamar MS Pagamento: {e}")
        
        return leilao_pb2.Empty()


if __name__ == "__main__":
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
    leilao_pb2_grpc.add_LanceServiceServicer_to_server(LanceService(), server)
    server.add_insecure_port('[::]:8002')
    print("--- [MS Lance] Servidor gRPC rodando na porta 8002 ---")
    server.start()
    server.wait_for_termination()