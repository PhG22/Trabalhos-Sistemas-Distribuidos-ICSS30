# Trabalho 5 - gRPC

## Objetivo

Alterar o Trabalho 4 com a modificação indicada abaixo.

## Modificação necessária

**Remover o RabbitMQ e o REST na comunicação entre os microsserviços e utilizar apenas gRPC**. Nesse caso, em vez de publicar mensagens em uma fila, as mensagens serão enviadas diretamente via gRPC para os microsserviços consumidores. Mantenher o uso de REST na comunicação entre: (I) o frontend e o API Gateway; e (II) o microsserviço de pagamento e o serviço externo;