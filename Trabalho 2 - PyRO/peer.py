import sys
import threading
import time
import Pyro5.api
import Pyro5.errors
import subprocess
from constants import (
    PEER_NAMES, NAME_SERVER_HOST, NAME_SERVER_PORT,
    HEARTBEAT_INTERVAL, HEARTBEAT_TIMEOUT, REQUEST_TIMEOUT,
    CRITICAL_SECTION_DURATION
)

# Define o serializador a ser usado pelo PyRO. 'serpent' é o padrão e recomendado.
Pyro5.config.SERIALIZER = "serpent"

@Pyro5.api.expose
@Pyro5.api.behavior(instance_mode="single")
class Peer:
    def __init__(self, name):
        # --- Inicialização do Estado do Peer ---
        self.name = name

        # Estado do processo em relação à Seção Crítica (SC)
        # 'RELEASED': Fora da SC e não tentando entrar.
        # 'WANTED': Deseja entrar na SC.
        # 'HELD': Atualmente dentro da SC.
        self.state = 'RELEASED'

        self.logical_clock = 0 # Relógio lógico de Lamport para ordenação de eventos.
        self.request_timestamp = -1 # Timestamp da última requisição feita por este peer para entrar na SC.

        self.request_queue = [] # Fila para armazenar requisições recebidas enquanto o peer está em 'HELD' ou 'WANTED'
                                # com prioridade maior. Armazena tuplas (timestamp, nome_do_requisitante).

        self.reply_count = 0# Contagem de respostas ('OK') recebidas para a própria requisição de SC.
        
        self.active_peers = {} # Dicionário para armazenar URIs dos outros peers ativos.
        
        self.last_heartbeat = {} # Dicionário para rastrear o tempo do último heartbeat recebido de cada peer.

        # Lock para garantir acesso thread-safe às variáveis de estado compartilhadas.
        # Essencial devido aos múltiplos threads (daemon PyRO, heartbeat, detector de falhas).
        self.state_lock = threading.Lock()

        # Evento para sinalizar que o peer recebeu todas as respostas necessárias para entrar na SC.
        self.permission_granted = threading.Event()

        # Dicionário para rastrear os timers de requisição individuais.
        self.request_timers = {}

        print(f"[{self.name}] Peer inicializado no estado {self.state}.")

    # --- Métodos do Algoritmo de Ricart & Agrawala ---

    def request_critical_section(self):
        """
        Inicia o processo para solicitar acesso à Seção Crítica (SC).
        """
        with self.state_lock:
            # Atualizar estado e relógio lógico
            self.state = 'WANTED'
            self.logical_clock += 1
            self.request_timestamp = self.logical_clock
            self.reply_count = 0
            self.permission_granted.clear()
            # Limpa timers de requisições anteriores.
            self.request_timers.clear()
            
            print(f"[{self.name}] Estado: {self.state}. Timestamp da requisição: {self.request_timestamp}")

            # Se não houver outros peers ativos, entra na SC imediatamente.
            if not self.active_peers:
                print(f"[{self.name}] Nenhum outro peer ativo. Entrando na SC diretamente.")
                self._enter_critical_section()
                return

        # Enviar requisições para todos os outros peers ativos, em unicast
        print(f"[{self.name}] Enviando requisições para {list(self.active_peers.keys())}...")

        # Cria uma cópia da lista de peers para evitar problemas com modificação concorrente  
        peers_to_request = list(self.active_peers.items())

        for peer_name, peer_uri in peers_to_request:
            # Inicia um temporizador para cada requisição para detectar falhas de resposta.
            timer = threading.Timer(REQUEST_TIMEOUT, self._handle_request_timeout, args=[peer_name])
            self.request_timers[peer_name] = timer
            timer.start()
            
            try:
                # Cria um proxy temporário para a chamada e chama o método remoto no outro peer.
                with Pyro5.api.Proxy(peer_uri) as peer_proxy:
                    peer_proxy.handle_request(self.name, self.request_timestamp)
            except Pyro5.errors.CommunicationError:
                print(f"[{self.name}] Falha de comunicação ao enviar requisição para {peer_name}. Considerado falho.")
                with self.state_lock:
                    self._remove_failed_peer(peer_name)
                timer.cancel()

        # Aguardar por (N-1) respostas
        print(f"[{self.name}] Aguardando {len(self.active_peers)} respostas...")
        
        # Bloqueia até que o evento seja setado ou o timeout ocorra.
        # O timeout aqui é uma salvaguarda geral.
        required_replies = len(self.active_peers)
        timeout_value = (REQUEST_TIMEOUT * required_replies) + 2.0 if required_replies > 0 else 1.0
        success = self.permission_granted.wait(timeout=timeout_value)
        
        if success:
            with self.state_lock:
                self._enter_critical_section()
        else:
            print(f"[{self.name}] Timeout geral ao esperar por permissões. Abortando requisição.")
            with self.state_lock:
                self.state = 'RELEASED'

    @Pyro5.api.oneway
    def handle_reply(self, sender_name):
        """
        Processa uma resposta 'OK' de outro peer.
        """
        with self.state_lock:
            if self.state!= 'WANTED':
                return # Ignora respostas se não estiver mais esperando

            if sender_name in self.request_timers:
                timer = self.request_timers.pop(sender_name)
                timer.cancel()

            self.reply_count += 1
            print(f"[{self.name}] Resposta recebida de {sender_name}. Total de respostas: {self.reply_count}/{len(self.active_peers)}")

            # Se todas as respostas foram recebidas, concede permissão para entrar na SC.
            if self.active_peers and self.reply_count == len(self.active_peers):
                self.permission_granted.set()

    @Pyro5.api.oneway
    def handle_request(self, requester_name, timestamp):
        """
        Processa uma requisição de SC vinda de outro peer.
        """
        with self.state_lock:
            # Verifica se o requisitante ainda é considerado ativo.
            if requester_name not in self.active_peers:
                print(f"[{self.name}] Requisição de {requester_name} ignorada (peer inativo).")
                return

            # Atualiza o relógio lógico (Regra de Lamport)
            self.logical_clock = max(self.logical_clock, timestamp) + 1
            print(f"[{self.name}] Requisição recebida de {requester_name} com timestamp {timestamp}. Meu relógio: {self.logical_clock}")

            should_defer = (
                self.state == 'HELD' or
                (self.state == 'WANTED' and (self.request_timestamp, self.name) < (timestamp, requester_name))
            )

            # Lógica de decisão de Ricart & Agrawala
            # Adia a resposta se:
            # 1. Eu estou na SC ('HELD').
            # 2. Eu quero entrar na SC ('WANTED') e minha requisição tem maior prioridade
            #    (timestamp menor, ou timestamp igual e nome lexicograficamente menor).
            try:
                # Cria um proxy temporário para a chamada de resposta.
                requester_uri = self.active_peers[requester_name]
                with Pyro5.api.Proxy(requester_uri) as requester_proxy:
                    if should_defer:
                        print(f"[{self.name}] Adiando resposta para {requester_name}. Meu estado: {self.state}, Meu TS: {self.request_timestamp}")
                        self.request_queue.append((timestamp, requester_name))
                        requester_proxy.receive_deferral(self.name)
                    else:
                        print(f"[{self.name}] Enviando resposta imediata para {requester_name}.")
                        requester_proxy.handle_reply(self.name)
            except (KeyError, Pyro5.errors.CommunicationError):
                # O requisitante pode ter falhado nesse meio tempo.
                print(f"[{self.name}] Falha ao enviar resposta para {requester_name}. Pode ter falhado.")
                self._remove_failed_peer(requester_name)

    @Pyro5.api.oneway
    def receive_deferral(self, sender_name):
        """
        Método remoto para receber uma mensagem de adiamento (DEFER).
        """
        with self.state_lock:
            if self.state!= 'WANTED':
                return
            
            print(f"[{self.name}] Resposta DEFER recebida de {sender_name}. Aguardando permissão futura.")
            # Cancela o timer, pois sabemos que o peer está vivo.
            if sender_name in self.request_timers:
                timer = self.request_timers.pop(sender_name)
                timer.cancel()

    def _enter_critical_section(self):
        """
        Lógica para entrar e permanecer na Seção Crítica.
        """
        self.state = 'HELD'
        
        print(f"\n>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>")
        print(f"[{self.name}] ENTROU NA SEÇÃO CRÍTICA. Estado: {self.state}")
        print(f"[{self.name}] O recurso será liberado em {CRITICAL_SECTION_DURATION} segundos.")
        print(f">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>\n")

        # Inicia um temporizador para liberar a SC automaticamente.
        release_timer = threading.Timer(CRITICAL_SECTION_DURATION, self.release_critical_section)
        release_timer.start()

    def release_critical_section(self):
        """
        Libera a Seção Crítica e responde a todas as requisições pendentes.
        """
        with self.state_lock:
            if self.state!= 'HELD':
                return # Evita liberações múltiplas

            self.state = 'RELEASED'
            print(f"\n<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<")
            print(f"[{self.name}] SAIU DA SEÇÃO CRÍTICA. Estado: {self.state}")
            
            # Responde a todas as requisições na fila.
            if self.request_queue:
                print(f"[{self.name}] Respondendo a {len(self.request_queue)} requisições pendentes...")
                self.request_queue.sort()
                # Cria uma cópia para iterar, pois a lista pode ser modificada
                queue_copy = self.request_queue[:]
                self.request_queue.clear()

                for _, peer_name in queue_copy:
                    try:
                        if peer_name in self.active_peers:
                            # Cria um proxy temporário para a chamada de resposta.
                            peer_uri = self.active_peers[peer_name]
                            with Pyro5.api.Proxy(peer_uri) as peer_proxy:
                                peer_proxy.handle_reply(self.name)
                    except (KeyError, Pyro5.errors.CommunicationError):
                        print(f"[{self.name}] Falha ao enviar resposta pendente para {peer_name}.")
                        self._remove_failed_peer(peer_name)
            
            print(f"<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<\n")

    # --- Métodos de Tolerância a Falhas ---

    @Pyro5.api.oneway
    def receive_heartbeat(self, sender_name):
        """
        Atualiza o timestamp do último heartbeat recebido de um peer.
        """
        with self.state_lock:
            # Apenas atualiza o timestamp, não precisa de proxy.
            if sender_name in self.active_peers:
                self.last_heartbeat[sender_name] = time.time()

    def _heartbeat_sender_thread(self):
        """
        Thread que envia heartbeats periodicamente para outros peers.
        """
        while True:
            time.sleep(HEARTBEAT_INTERVAL)
            with self.state_lock:
                # Pega uma cópia dos itens para evitar problemas de concorrência
                peers_to_ping = list(self.active_peers.items())
            
            for peer_name, peer_uri in peers_to_ping:
                try:
                    # Cria um proxy temporário para a chamada de heartbeat.
                    with Pyro5.api.Proxy(peer_uri) as peer_proxy:
                        peer_proxy.receive_heartbeat(self.name)
                except Pyro5.errors.CommunicationError:
                    print(f"[{self.name}] Falha de heartbeat para {peer_name}. Será removido pelo detector.")
    
    def _failure_detector_thread(self):
        """
        Thread que verifica periodicamente se algum peer falhou (não enviou heartbeat).
        """
        while True:
            time.sleep(HEARTBEAT_INTERVAL) 
            with self.state_lock:
                current_time = time.time()
                failed_peers = [] # Implementar lógica de população dos peers falhos.

                for peer_name in failed_peers:
                    print(f"[{self.name}] Timeout de heartbeat de {peer_name}. Considerado falho.")
                    self._remove_failed_peer(peer_name)

    def _handle_request_timeout(self, peer_name):
        """
        Callback chamado quando uma resposta a uma requisição de SC não chega a tempo.
        """
        with self.state_lock:
            # Verifica se o peer já não foi removido e se ainda estamos esperando por ele.
            if peer_name in self.active_peers and self.state == 'WANTED':
                print(f"[{self.name}] Timeout de resposta de {peer_name}. Considerado falho.")
                self._remove_failed_peer(peer_name)
                
                # Após remover o peer falho, verifica se já temos respostas suficientes dos restantes.
                if self.active_peers and self.reply_count == len(self.active_peers):
                    self.permission_granted.set()

    def _remove_failed_peer(self, peer_name):
        """
        Lógica centralizada para remover um peer considerado falho.
        Esta função deve ser chamada dentro de um lock.
        """
        if peer_name in self.active_peers:
            del self.active_peers[peer_name]
            if peer_name in self.last_heartbeat:
                del self.last_heartbeat[peer_name]
            
            # Remove quaisquer requisições pendentes deste peer.
            self.request_queue = [req for req in self.request_queue if req[1] != peer_name]
            
            print(f"[{self.name}] Peer {peer_name} removido da lista de peers ativos.")
            print(f"[{self.name}] Peers ativos agora: {list(self.active_peers.keys())}")

    # --- Configuração do PyRO e Loop Principal ---

    def setup_pyro(self):
        """
        Configura e inicia o servidor PyRO e conecta-se a outros peers.
        """
        # Inicia o daemon do PyRO em um thread separado.
        daemon = Pyro5.api.Daemon()
        uri = daemon.register(self, self.name)
        
        # Tenta localizar o servidor de nomes.
        try:
            ns = Pyro5.api.locate_ns(host=NAME_SERVER_HOST, port=NAME_SERVER_PORT)
        except Pyro5.errors.NamingError:
            print(f"[{self.name}] Servidor de Nomes não encontrado. Tentando iniciar...")
            # Lógica para iniciar o servidor de nomes se ele não estiver rodando.
            try:
                # Este comando inicia o servidor de nomes em um processo separado.
                subprocess.Popen(f"pyro5-ns -n {NAME_SERVER_HOST} -p {NAME_SERVER_PORT}", shell=True, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
                time.sleep(2) # Dá um tempo para o servidor iniciar.
                ns = Pyro5.api.locate_ns(host=NAME_SERVER_HOST, port=NAME_SERVER_PORT)
                print(f"[{self.name}] Servidor de Nomes iniciado com sucesso.")
            except Exception as e:
                print(f"[{self.name}] Falha ao iniciar o Servidor de Nomes: {e}")
                sys.exit(1)

        # Registra este peer no servidor de nomes.
        ns.register(self.name, uri)
        print(f"[{self.name}] Registrado no Servidor de Nomes com URI: {uri}")

        # Inicia o loop de eventos do daemon em um thread.
        daemon_thread = threading.Thread(target=daemon.requestLoop, daemon=True)
        daemon_thread.start()

        # Conecta-se aos outros peers.
        self._connect_to_peers(ns)

    def _connect_to_peers(self, ns):
        """
        Localiza e armazena proxies para todos os outros peers.
        """
        print(f"[{self.name}] Tentando se conectar a outros peers...")
        all_peer_names = PEER_NAMES.copy()
        all_peer_names.remove(self.name)

        for peer_name in all_peer_names:
            retries = 5
            while retries > 0:
                try:
                    peer_uri = ns.lookup(peer_name)
                    self.active_peers[peer_name] = peer_uri
                    self.last_heartbeat[peer_name] = time.time()
                    print(f"[{self.name}] Conectado com sucesso a {peer_name}.")
                    break
                except Pyro5.errors.NamingError:
                    # O outro peer pode ainda não ter se registrado. Tenta novamente.
                    print(f"[{self.name}] Aguardando {peer_name} se registrar...")
                    retries -= 1
                    time.sleep(2)
                except Exception as e:
                    print(f"[{self.name}] Erro ao conectar com {peer_name}: {e}")
                    break
        
        print(f"[{self.name}] Conexão inicial completa. Peers ativos: {list(self.active_peers.keys())}")

    def run(self):
        """
        Inicia os threads de background e o loop de interação com o usuário.
        """
        self.setup_pyro()
        
        # Inicia os threads para heartbeat e detecção de falhas.
        hb_sender = threading.Thread(target=self._heartbeat_sender_thread, daemon=True)
        hb_sender.start()
        
        failure_detector = threading.Thread(target=self._failure_detector_thread, daemon=True)
        failure_detector.start()

        print("\n" + "="*50)
        print(f"Peer {self.name} está totalmente operacional.")
        print("Pressione [Enter] para solicitar acesso à Seção Crítica.")
        print("Pressione [Ctrl+C] para sair.")
        print("="*50 + "\n")

        try:
            while True:
                input() # Aguarda o usuário pressionar Enter.
                with self.state_lock:
                    is_released = self.state == 'RELEASED'
                
                if is_released:
                    self.request_critical_section()
                else:
                    print(f"[{self.name}] Ação ignorada. Estado atual: {self.state}")
        except KeyboardInterrupt:
            print(f"\n[{self.name}] Encerrando...")

def main():
    if len(sys.argv) < 2 or sys.argv[1] not in PEER_NAMES:
        print(f"Uso: python peer.py <NomeDoPeer>")
        print(f"Nomes válidos: {PEER_NAMES}")
        sys.exit(1)
    
    peer_name = sys.argv[1]
    peer = Peer(peer_name)
    peer.run()

if __name__ == "__main__":
    main()