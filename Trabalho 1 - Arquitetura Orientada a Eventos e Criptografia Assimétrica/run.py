import sys
import time
import threading
import subprocess

from MS_leilao import MS_Auctions
from MS_lance import MSBid
from MS_notificacao import MS_Notifications

def start_client_terminal(user_id):
    """
    Abre um novo terminal e executa o script do cliente para um determinado user_id.
    Este trecho permanece o mesmo, pois o cliente é uma aplicação interativa separada.
    """
    command = f"python client.py {user_id}"
    
    try:
        if sys.platform == "win32":
            subprocess.Popen(f'start cmd /k "{command}"', shell=True)
        elif sys.platform == "darwin": # MacOS
            script = f'tell app "Terminal" to do script "{command}"'
            subprocess.Popen(['osascript', '-e', script])
        else: # Linux
            try:
                subprocess.Popen(['gnome-terminal', '--', 'bash', '-c', f"{command}; exec bash"])
            except FileNotFoundError:
                subprocess.Popen(['xterm', '-e', f'bash -c "{command}; exec bash"'])

        print(f"\n[INFO] Terminal do cliente para '{user_id}' solicitado.")

    except Exception:
        print(f"\n[AVISO] Não foi possível abrir um novo terminal automaticamente.")
        print(f"Por favor, abra um novo terminal e execute: {command}")

def run_service(service_instance):
    """
    Função wrapper para chamar o método run() de uma instância de serviço.
    Isso facilita o tratamento de exceções dentro do thread.
    """
    try:
        service_instance.run()
    except Exception as e:
        # Pega o nome da classe da instância para fornecer um log mais claro
        service_name = service_instance.__class__.__name__
        print(f"\n[ERRO FATAL] O serviço {service_name} encontrou um erro e foi encerrado: {e}")

if __name__ == "__main__":
    print("--- Iniciando Microserviços do Sistema de Leilão (Modo OOP com Threads) ---")

    try:
        # Instancia os objetos das classes de serviço
        auction_service = MS_Auctions()
        bid_service = MSBid()
        notification_service = MS_Notifications()

        services = [auction_service, bid_service, notification_service]
        service_threads = []

        # Cria e inicia um thread para cada serviço
        for service in services:
            # O thread é marcado como 'daemon' para que ele seja encerrado automaticamente
            # quando o programa principal (este script) for finalizado.
            thread = threading.Thread(target=run_service, args=(service,), daemon=True)
            thread.start()
            service_threads.append(thread)
            print(f"[OK] Serviço '{service.__class__.__name__}' iniciado em um thread separado.")
        
        print("-" * 70)
        print("\n[INFO] Todos os serviços de backend estão em execução.\n")

    except Exception as e:
        print(f"[ERRO CRÍTICO] Falha ao instanciar ou iniciar os serviços: {e}")
        print("--- Sistema não pôde ser iniciado. ---")
        sys.exit(1)


    try:
        # Mantém o script principal em execução para que os threads daemon não sejam encerrados
        while True:
            print("\n--- Gerenciador de Clientes ---")
            print("Digite um ID de usuário (ex: 'ana', 'bob') para iniciar um novo cliente.")
            user_input = input("Ou digite 'sair' para encerrar o sistema: ").strip()

            if user_input.lower() == 'sair':
                break
            
            if user_input:
                start_client_terminal(user_input)
            else:
                print("[AVISO] ID de usuário não pode ser vazio.")
    
    except (KeyboardInterrupt, EOFError):
        # A saída via Ctrl+C ou Ctrl+D encerrará o loop
        pass
    finally:
        print("\n--- Encerrando o Sistema de Leilão ---")
        # Não é necessário encerrar os threads explicitamente, pois são 'daemon'.
        # Eles serão finalizados quando o script principal sair.
        print("--- Sistema encerrado. ---")