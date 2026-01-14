from confluent_kafka import Consumer
from services.query import query_data
import json
import os
from dotenv import load_dotenv

# Carrega variáveis de ambiente do arquivo .env (se existir)
load_dotenv()

consumer = Consumer({
    'bootstrap.servers': os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'pkc-619z3.us-east1.gcp.confluent.cloud:9092'),
    'security.protocol': os.getenv('KAFKA_SECURITY_PROTOCOL', 'SASL_SSL'),
    'sasl.mechanisms': os.getenv('KAFKA_SASL_MECHANISMS', 'PLAIN'),
    'sasl.username': os.getenv('KAFKA_SASL_USERNAME'),
    'sasl.password': os.getenv('KAFKA_SASL_PASSWORD'),
    'group.id': os.getenv('KAFKA_GROUP_ID', 'grupo-python-consumer-9'),
    'auto.offset.reset': os.getenv('KAFKA_AUTO_OFFSET_RESET', 'earliest')
})

consumer.subscribe(['meu_primeiro_topico'])

print("📡 Aguardando mensagens do Kafka...")

try:
    while True:
        msg = consumer.poll(1.0)

        if msg is None:
            continue

        if msg.error():
            print(f"Erro: {msg.error()}")
            continue

        key = msg.key().decode('utf-8') if msg.key() else None
        value = json.loads(msg.value().decode('utf-8'))

        query_data(
            id=value.get('id'),
            operation=value.get('operation'),
            sequence=value.get('sequence')
        )

except KeyboardInterrupt:
    print("Encerrando consumer...")

finally:
    consumer.close()
