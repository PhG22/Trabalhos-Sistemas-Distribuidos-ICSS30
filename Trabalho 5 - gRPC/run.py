import sys
import time
import subprocess
import webbrowser

def start_service_terminal(command, title="Serviço"):
    """
    Inicia um novo processo em um terminal separado, compatível com múltiplos SOs.
    
    Args:
        command (str): O nome do arquivo Python a ser executado.
        title (str): O título da janela do terminal (para organização visual).
    """
    full_command = f"python {command}"
    
    try:
        if sys.platform == "win32":
            subprocess.Popen(f'start "{title}" cmd /k "{full_command}"', shell=True)
        elif sys.platform == "darwin": # MacOS
            script = f'tell app "Terminal" to do script "echo {title}; {full_command}"'
            subprocess.Popen(['osascript', '-e', script])
        else: # Linux
            try:
                # Tenta terminais comuns de Linux
                subprocess.Popen(['gnome-terminal', '--title', title, '--', 'bash', '-c', f"{full_command}; exec bash"])
            except FileNotFoundError:
                try:
                    subprocess.Popen(['xterm', '-T', title, '-e', f'bash -c "{full_command}; exec bash"'])
                except FileNotFoundError:
                     print(f"\n[AVISO] Não foi possível abrir {title} automaticamente.")

        print(f"[INFO] Terminal para '{title}' ({command}) solicitado.")

    except Exception as e:
        print(f"\n[AVISO] Falha ao abrir terminal para '{title}': {e}")

if __name__ == "__main__":
    print("--- Iniciando Microserviços do Sistema de Leilão (Arquitetura gRPC) ---")
    
    # Lista de tuplas (arquivo, titulo_da_janela)
    # A ordem de boot é importante para evitar erros de conexão imediata.
    services_to_launch = [
        ("api_gateway.py", "API Gateway (REST 5000 / gRPC Client)"),
        ("MS_leilao.py", "MS Leilão (gRPC 8001)"),
        ("MS_lance.py", "MS Lance (gRPC 8002)"),
        ("pagamento_externo.py", "MOCK Pagamento Externo (REST 8004)"),
        ("MS_pagamento.py", "MS Pagamento (gRPC 8003 / Webhook 8005)"),
    ]

    # Inicia cada serviço com um pequeno delay para o SO processar a abertura das janelas
    for service_script, title in services_to_launch:
        start_service_terminal(service_script, title)
        time.sleep(1) 

    print("-" * 70)
    print("[INFO] Todos os serviços de backend foram iniciados.")
    
    # Abre 3 abas do frontend no navegador padrão
    client_url = "http://localhost:5000"
    try:
        webbrowser.open(client_url)
        webbrowser.open(client_url)
        webbrowser.open(client_url)
    except Exception as e:
        pass # Falha silenciosa se não tiver navegador, o usuário pode abrir manual