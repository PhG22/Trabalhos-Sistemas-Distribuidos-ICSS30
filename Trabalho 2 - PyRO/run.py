import subprocess
import sys
import time
from constants import PEER_NAMES, NAME_SERVER_HOST, NAME_SERVER_PORT

def main():
    """
    Inicia o servidor de nomes do PyRO e todos os processos Peer em terminais separados.
    """
    commands = []
    
    # Comando para iniciar o servidor de nomes do PyRO
    ns_command = f"pyro5-ns -n {NAME_SERVER_HOST} -p {NAME_SERVER_PORT}"
    commands.append(ns_command)

    # Comandos para iniciar cada processo Peer
    for name in PEER_NAMES:
        # Garante que o comando para o peer seja uma lista, que é mais seguro para o subprocess
        peer_command = ["python", "peer.py", name]
        commands.append(peer_command)

    print("Iniciando o sistema distribuído...")
    print("Um terminal será aberto para o Servidor de Nomes e um para cada Peer.")
    print(f"Total de processos a serem iniciados: {len(commands)}")

    # A forma de abrir um novo terminal varia entre os sistemas operacionais
    if sys.platform == "win32": # Windows
        for i, cmd_list_or_str in enumerate(commands):
            title = "Name Server" if i == 0 else f"Peer {PEER_NAMES[i-1]}"
            # Se for uma lista, junta em uma string para o cmd
            cmd_str = " ".join(cmd_list_or_str) if isinstance(cmd_list_or_str, list) else cmd_list_or_str
            subprocess.Popen(f'start "{title}" cmd /k "{cmd_str}"', shell=True)
            time.sleep(0.5)
    elif sys.platform == "darwin": # macOS
        for i, cmd_list_or_str in enumerate(commands):
            title = "Name Server" if i == 0 else f"Peer {PEER_NAMES[i-1]}"
            cmd_str = " ".join(cmd_list_or_str) if isinstance(cmd_list_or_str, list) else cmd_list_or_str
            script = f'tell app "Terminal" to do script "echo {title}; {cmd_str}"'
            subprocess.run(["osascript", "-e", script])
            time.sleep(0.5)
    elif sys.platform.startswith("linux"): # Linux
        for i, cmd_list_or_str in enumerate(commands):
            title = "Name Server" if i == 0 else f"Peer {PEER_NAMES[i-1]}"
            cmd_str = " ".join(cmd_list_or_str) if isinstance(cmd_list_or_str, list) else cmd_list_or_str
            try:
                subprocess.Popen(['gnome-terminal', '--title', title, '--', 'bash', '-c', f'{cmd_str}; exec bash'])
            except FileNotFoundError:
                print("gnome-terminal não encontrado. Tentando com xterm...")
                try:
                    subprocess.Popen(['xterm', '-title', title, '-hold', '-e', f'bash -c "{cmd_str}; exec bash"'])
                except FileNotFoundError:
                    print("Erro: Não foi possível encontrar um emulador de terminal compatível (gnome-terminal ou xterm).")
                    print("Por favor, inicie os processos manualmente.")
                    return
            time.sleep(0.5)
    else:
        print(f"Sistema operacional '{sys.platform}' não suportado para lançamento automático.")
        print("Por favor, inicie os seguintes comandos manualmente em terminais separados:")
        for cmd in commands:
            cmd_str = " ".join(cmd) if isinstance(cmd, list) else cmd
            print(f"- {cmd_str}")

    print("\nSistema iniciado. Verifique as janelas de terminal abertas.")
    print("Para encerrar o sistema, feche todas as janelas de terminal manualmente.")

if __name__ == "__main__":
    main()