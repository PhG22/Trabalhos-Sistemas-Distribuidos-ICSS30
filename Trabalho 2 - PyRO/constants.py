# Nomes dos processos pares no sistema distribuído.
# Estes nomes são usados para registrar e localizar os objetos remotos no servidor de nomes do PyRO.
PEER_NAMES = ["PeerA","PeerB","PeerC","PeerD"]

# Configurações de rede para o servidor de nomes do PyRO.
NAME_SERVER_HOST = 'localhost'
NAME_SERVER_PORT = 9090

# Parâmetros de temporização para o mecanismo de tolerância a falhas.
# HEARTBEAT_INTERVAL: Frequência (em segundos) com que cada peer envia uma mensagem de "estou vivo".
HEARTBEAT_INTERVAL = 2.0  # rápido o suficiente para detectar falhas sem sobrecarregar o sistema com mensagens.

# HEARTBEAT_TIMEOUT: Tempo máximo (em segundos) de espera por um heartbeat de outro peer.
# Se um heartbeat não for recebido dentro deste período, o peer é considerado falho.
# Este valor deve ser maior que HEARTBEAT_INTERVAL para evitar falsos positivos.
# Usar 3x o intervalo (3 * 2.0s) permite que um peer perca até dois heartbeats consecutivos antes de ser considerado falho.   
HEARTBEAT_TIMEOUT = 6.0

# REQUEST_TIMEOUT: Tempo máximo (em segundos) que um peer aguarda por uma resposta
# a uma requisição para entrar na seção crítica. Se o tempo expirar, o peer de destino
# da requisição pode ser considerado falho.
# O timeout de uma requisição deve ser maior que o tempo máximo que um peer pode legitimamente ficar na seção crítica
REQUEST_TIMEOUT = 8.0  # CRITICAL_SECTION_DURATION (5s) + buffer (3s)

# Parâmetros de controle da seção crítica.
# CRITICAL_SECTION_DURATION: Tempo máximo (em segundos) que um peer pode permanecer
# dentro da seção crítica. Após este tempo, o recurso é liberado automaticamente.
CRITICAL_SECTION_DURATION = 5.0