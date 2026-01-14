from confluent_kafka import Producer
import json
import random
import os
from dotenv import load_dotenv

# Carrega variáveis de ambiente do arquivo .env (se existir)
load_dotenv()

def delivery_report(err, msg):
    if err is not None:
        print(f'Message delivery failed: {err}')
    else:
        print(f'Message delivered to {msg.topic()} [{msg.partition()}]')

producer = Producer({
    'bootstrap.servers': os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'pkc-619z3.us-east1.gcp.confluent.cloud:9092'),
    'security.protocol': os.getenv('KAFKA_SECURITY_PROTOCOL', 'SASL_SSL'),
    'sasl.mechanisms': os.getenv('KAFKA_SASL_MECHANISMS', 'PLAIN'),
    'sasl.username': os.getenv('KAFKA_SASL_USERNAME'),
    'sasl.password': os.getenv('KAFKA_SASL_PASSWORD'),
    'session.timeout.ms': int(os.getenv('KAFKA_SESSION_TIMEOUT_MS', '45000')),
    'client.id': os.getenv('KAFKA_CLIENT_ID', 'ccloud-python-client-37a648e5-7873-47e5-ab3d-2b4676456816')
})

message_simplified = {
    "id": random.randint(0, 3),
    "operation": "insert"
}

producer.produce(
    topic=os.getenv('KAFKA_TOPIC', 'meu_primeiro_topico'),
    key=str(message_simplified['id']).encode('utf-8'),
    value=json.dumps(message_simplified).encode('utf-8'),
    callback=delivery_report
)

producer.flush()
for i in range(3):
    message = {
        "id": i,
        "operation": 
        random.choice(['insert']),
        "sequence": i
    }
    producer.produce(
        topic=os.getenv('KAFKA_TOPIC', 'meu_primeiro_topico'),
        key=str(message['id']).encode('utf-8'),
        value=json.dumps(message).encode('utf-8'),
        callback=delivery_report
    )

producer.flush()