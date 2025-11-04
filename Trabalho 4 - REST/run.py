import sys
import time
import subprocess
import webbrowser

def start_service_terminal(command, title="Serviço"):
    """
    Abre um novo terminal e executa um comando de serviço.
    Adaptado da sua função original start_client_terminal.
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
                # Tenta com gnome-terminal
                subprocess.Popen(['gnome-terminal', '--title', title, '--', 'bash', '-c', f"{full_command}; exec bash"])
            except FileNotFoundError:
                try:
                    # Tenta com xterm como fallback
                    subprocess.Popen(['xterm', '-T', title, '-e', f'bash -c "{full_command}; exec bash"'])
                except FileNotFoundError:
                     print(f"\n[AVISO] Não foi possível abrir {title} automaticamente.")
                     print(f"Por favor, abra um novo terminal e execute: {full_command}")

        print(f"[INFO] Terminal para '{title}' ({command}) solicitado.")

    except Exception as e:
        print(f"\n[AVISO] Falha ao abrir terminal para '{title}': {e}")
        print(f"Por favor, abra um novo terminal e execute: {full_command}")

if __name__ == "__main__":
    print("--- Iniciando Microserviços do Sistema de Leilão (Trabalho 4) ---")
    
    # Lista de serviços para iniciar. 
    # Cada um é um arquivo .py que roda seu próprio servidor FastAPI.
    services_to_launch = [
        ("api_gateway.py", "API Gateway (Porta 5000)"),
        ("MS_leilao.py", "MS Leilão (Porta 8001)"),
        ("MS_lance.py", "MS Lance (Porta 8002)"),
        ("pagamento_externo.py", "MOCK Pagamento Externo (Porta 8004)"),
        ("MS_pagamento.py", "MS Pagamento (Porta 8003)"),
    ]

    for service_script, title in services_to_launch:
        start_service_terminal(service_script, title)
        time.sleep(1) # Pequeno atraso para evitar sobrecarga

    print("-" * 70)
    print("[INFO] Todos os serviços de backend foram iniciados em seus próprios terminais.")
    print("[INFO] Você precisará de um broker RabbitMQ em execução.")
    
    # Abrir o cliente (navegador) automaticamente
    client_url = "http://localhost:5000"
    print(f"\n[INFO] Abrindo o frontend do cliente em: {client_url}")
    try:
        webbrowser.open(client_url)
        webbrowser.open(client_url)
        webbrowser.open(client_url)
    except Exception as e:
        print(f"[AVISO] Não foi possível abrir o navegador automaticamente: {e}")
        print(f"Por favor, abra manualmente: {client_url}")

    print("\n--- Gerenciador principal encerrado. Os serviços continuam em execução. ---")
    print("--- Você pode fechar este terminal. ---")