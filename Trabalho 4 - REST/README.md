# Trabalho 4 - REST

## Objetivo

Implementar uma aplicação web de Leilão e um sistema de pagamento.

## Descrição dos Processos

### 1. RabbitMQ
Middleware orientado a Mensagens (MOM – Message Oriented Middleware) responsável por organizar mensagens (eventos) em filas, onde os produtores  (publishers) as enviam e os consumidores/assinantes (subscribers) as recebem. Será utilizado para comunicação assíncrona entre os microsserviços.

### 2. Cliente (Frontend)
O **frontend** deve ser implementado em uma **linguagem diferente** daquela utilizada nos demais processos. O cliente interage exclusivamente com a aplicação do Leilão através do **API Gateway**, utilizando **requisições REST** e **conexões SSE** (Server-Sent Events).

- O cliente envia requisições REST para:
    - Criar leilões;

    - Consultar leilões ativos;

    - Efetuar lances;

    - Registrar interesse em notificações;

    - Cancelar interesse em notificações.

- O cliente recebe notificações via SSE, incluindo:
    - Novos lances válidos em leilões de interesse;

    - Encerramento de leilões e anúncio de vencedor;

    - Geração de link de pagamento;

    - Resultado do pagamento (aprovado ou recusado).
    
- O cliente vencedor do leilão recebe via SSE o link de pagamento gerado pelo sistema e pode acessá-lo para concluir a compra.

### 3. API Gateway (subscriber)
Responsável por atuar como ponto único de entrada para o frontend, expondo os endpoints REST e enviando requisições REST aos microsserviços internos. O API Gateway apenas consome eventos do RabbitMQ.

- Expõe a API REST para o cliente:
    - **Criar um leilão. O API gateway receberá os dados do leilão (nome do produto, descrição, valor inicial, data e hora de início e fim do leilão) e encaminhará ao MS Leilão via REST**;

    - **Consultar leilões ativos. O API gateway consultará o MS Leilão (REST) para retornar os dados (nome do produto, descrição, valor inicial ou último lance, data e hora de início e fim do leilão) dos leilões ativos aos clientes**;

    - Efetuar um lance. O API gateway receberá do cliente os dados do lance (ID do leilão, ID do usuário, valor do lance) e **enviará esses dados ao MS Lance via REST**;

    - **Registrar interesse em receber notificações (novos lances e vencedor) sobre um leilão**. O API gateway deve gerenciar os interesses ativos e os eventos gerados, a fim de enviar notificações apenas aos clientes interessados nesses eventos.

    - **Cancelar interesse em receber notificações (novos lances e vencedor) sobre um leilão**. O API gateway não deve mais enviar notificações aos clientes que cancelaram o registro de interesse em um leilão.

- **Responsável por manter conexões SSE com os clientes. Com base nos eventos consumidos lance_validado, lance_invalidado, leilao_vencedor, link_pagamento, status_pagamento, o API gateway enviará as seguintes notificações personalizadas (através de canais diferenciados pelo clienteID)**:
    - **Novo lance válido (aos clientes registrados)**;

    - **Lance inválido**;

    - **Vencedor do leilão (aos clientes registrados)**;

    - **Link de pagamento**;

    - **Pagamento aprovado ou Pagamento recusado**.

### 4. MS Leilão (publisher)
Responsável pela criação e gerenciamento dos leilões.

- **Recebe requisições REST do gateway para:**
    - **Criar leilões**.

    - **Consultar leilões**.

- Controla o ciclo de vida dos leilões:
    - Ao atingir a data/hora de início → publica leilao_iniciado.

    - Ao atingir a data/hora de término → publica leilao_finalizado.

### 5. MS Lance (publisher e subscriber)
Responsável por gerenciar os lances realizados nos leilões.
- Ele consome os eventos leilao_iniciado e leilao_finalizado.
- Recebe requisições REST do gateway contendo um lance (ID do leilão; ID do usuário, valor do lance). Somente aceita o lance se o leilão estiver ativo e se o lance for maior que o último lance registrado.
    - Se o lance for válido, o MS Lance publica lance_validado.
    - **Caso contrário, ele publicará o evento no lance_invalidado.**

- Quando o evento leilao_finalizado é recebido, o MS Lance determina o vencedor e publica leilao_vencedor, informando o ID do leilão, o ID do vencedor do leilão e o valor negociado.

### 6. MS Pagamento (publisher e subscriber)
- **Consome os eventos leilao_vencedor (ID do leilão, ID do vencedor, valor).**

- **Para cada evento consumido, ele fará uma requisição REST ao sistema externo de pagamentos enviando os dados do pagamento (valor, moeda, informações do cliente) e, então, receberá um link de pagamento que será publicado em link_pagamento.**

- **Ele define um endpoint que recebe notificações assíncronas do sistema externo indicando o status da transação (aprovada ou recusada). Com base nos eventos externos recebidos, ele publica eventos status_pagamento, para que o API Gateway notifique o cliente via SSE.**

### 7. Sistema de Pagamento Externo
**O sistema externo recebe requisições REST do MS Pagamento para iniciar a transação e retorna um link de pagamento. Após o processamento do pagamento, o sistema de pagamento externo envia uma notificação assíncrona via webhook (HTTP POST) ao endpoint configurado no MS Pagamento. Essa notificação inclui informações sobre o evento: ID da transação, status do pagamento (aprovado ou recusado), valor e dados do comprador.**