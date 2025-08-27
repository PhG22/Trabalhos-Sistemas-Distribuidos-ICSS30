# Trabalho 1 - Arquitetura Orientada a Eventos e Criptografia Assimétrica

## Objetivo

Desenvolver um sistema de leilão composto por 3 **microsserviços**, cada um com uma responsabilidade específica. Os microsserviços vão se comunicar exclusivamente via filas de mensagens. O fluxo de dados é orquestrado por eventos através do **serviço de mensageria RabbitMQ** e do **protocolo AMQP** que garantem a sincronização entre as diferentes funcionalidades.

## Descrição dos Processos

### 1. RabbitMQ
- Middleware orientado a Mensagens (MOM – Message Oriented Middleware) responsável por organizar mensagens (eventos) em filas, onde os produtores (publishers) as enviam e os consumidores/assinantes (subscribers) as recebem.

### 2. Cliente (publisher e subscriber)
- Não se comunica diretamente com nenhum serviço, toda a comunicação é indireta através de filas de mensagens.

- Logo ao inicializar, atuará como consumidor recebendo eventos da fila **leilao_iniciado**. Os eventos recebidos contêm ID do leilão, descrição, data e hora de início e fim.

- Possui um par de chaves pública/privada. Publica lances na fila de mensagens **lance_realizado**. Cada lance contém: ID do leilão, ID do usuário, valor do lance. O cliente assina digitalmente cada lance com sua chave privada.

-  Ao dar um lance em um leilão, o cliente atuará como consumidor desse leilão, registrando interesse em receber notificações quando um novo lance for efetuado no leilão de seu interesse ou quando o leilão for encerrado. Por exemplo, se o cliente der um lance no leilão de ID 1, ele escutará a fila **leilao_1**.

### 3. MS Leilão (publisher)
- Mantém internamente uma lista pré-configurada (hardcoded) de leilões com: ID do leilão, descrição, data e hora de início e fim, status (ativo, encerrado).

- O leilão de um determinado produto deve ser iniciado quando o tempo definido para esse leilão for atingido. Quando um leilão começa, ele publica o evento na fila: **leilao_iniciado**.

- O leilão de um determinado produto deve ser finalizado quando o tempo definido para esse leilão expirar. Quando um leilão termina, ele publica o evento na fila: **leilao_finalizado**.

### 4. MS Lance (publisher e subscriber)
- Possui as chaves públicas de todos os clientes.

- Escuta os eventos das filas **lance_realizado**, **leilao_iniciado** e **leilao_finalizado**.

- Recebe lances de usuários (ID do leilão; ID do usuário, valor do lance) e checa a assinatura digital da mensagem utilizando a chave pública correspondente. Somente aceitará o lance se:
    - A assinatura for válida;
    - ID do leilão existir e se o leilão estiver ativo;
    - Se o lance for maior que o último lance registrado;

- Se o lance for válido, o MS Lance publica o evento na fila **lance_validado**.

- Ao finalizar um leilão, deve publicar na fila **leilao_vencedor**, informando o ID do leilão, o ID do vencedor do leilão e o valor negociado. O vencedor é o que efetuou o maior lance válido até o encerramento.

### 5. MS Notificação (publisher e subscriber)
- Escuta os eventos das filas **lance_validado** e **leilao_vencedor**.

- Publica esses eventos nas filas específicas para cada leilão, de acordo com o seu ID (**leilao_1**, **leilao_2**, ...), de modo que somente os consumidores interessados nesses leilões recebam as notificações correspondentes.