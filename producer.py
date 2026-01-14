# Producer: envia mensagens para um tópico Kafka usando configs do .env
# Importação das bibliotecas necessárias
from confluent_kafka import Producer
import json
import random
import os
from dotenv import load_dotenv

# Carrega variáveis de ambiente do arquivo .env (se existir)
load_dotenv()

def delivery_report(err, msg):
    # Callback chamado após a tentativa de envio da mensagem.
    # - Se err != None: houve falha no envio
    # - Caso contrário: mensagem entregue com sucesso (mostra tópico e partição)
    if err is not None:
        print(f'Message delivery failed: {err}')
    else:
        print(f'Message delivered to {msg.topic()} [{msg.partition()}]')

# Cria o Producer com parâmetros/credenciais do Kafka vindos do .env
producer = Producer({
    'bootstrap.servers': os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'pkc-619z3.us-east1.gcp.confluent.cloud:9092'),
    'security.protocol': os.getenv('KAFKA_SECURITY_PROTOCOL', 'SASL_SSL'),
    'sasl.mechanisms': os.getenv('KAFKA_SASL_MECHANISMS', 'PLAIN'),
    'sasl.username': os.getenv('KAFKA_SASL_USERNAME'),
    'sasl.password': os.getenv('KAFKA_SASL_PASSWORD'),
    'session.timeout.ms': int(os.getenv('KAFKA_SESSION_TIMEOUT_MS', '45000')),
    'client.id': os.getenv('KAFKA_CLIENT_ID', 'ccloud-python-client-37a648e5-7873-47e5-ab3d-2b4676456816')
})

# Exemplo 1: envia uma mensagem simples (operation="insert") com id aleatório
message_simplified = {
    "id": random.randint(0, 3),
    "operation": "insert"
}

producer.produce(
    topic=os.getenv('KAFKA_TOPIC', 'meu_primeiro_topico'), # Tópico: deve ser o mesmo do consumer
    key=str(message_simplified['id']).encode('utf-8'),     # Chave (opcional), aqui o id como string
    value=json.dumps(message_simplified).encode('utf-8'),  # Corpo em JSON (bytes)
    callback=delivery_report                               # Mostra sucesso/erro do envio   
)

# Garante envio do que está em buffer antes de continuar
producer.flush()
# Exemplo 2: envia 3 mensagens (ids 0..2), com operation="insert" e um campo sequence
for i in range(3):
    message = {
        "id": i,
        "operation": 
        random.choice(['insert']),  #Mantém "insert" (a lista pode ter outras operações no futuro)
        "sequence": i
    }
    producer.produce(
        topic=os.getenv('KAFKA_TOPIC', 'meu_primeiro_topico'), # Tópico onde a mensagem será publicada (deve bater com o consumer)
        key=str(message['id']).encode('utf-8'),                # Chave opcional (aqui usamos o id); ajuda no particionamento
        value=json.dumps(message).encode('utf-8'),             # Corpo da mensagem em JSON (convertido para bytes)
        callback=delivery_report                               # Callback para relatar sucesso/erro no envio
    )
# Aguarda finalizar o envio de tudo antes de encerrar o programa
producer.flush()