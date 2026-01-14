# Consumer: recebe mensagens do Kafka e chama o serviço que consulta o Postgres

from confluent_kafka import Consumer            # Cliente Kafka para consumir mensagens
from services.query import query_data           # Função que acessa o banco
import json                                    # Para converter texto JSON em dict
import os
from dotenv import load_dotenv                 # Para carregar variáveis do .env

# Carrega as variáveis do arquivo .env (usuário, senha, etc.)
load_dotenv()

# Cria o consumidor com as configurações do Kafka
consumer = Consumer({
    'bootstrap.servers': os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'pkc-619z3.us-east1.gcp.confluent.cloud:9092'),  # endereço do cluster
    'security.protocol': os.getenv('KAFKA_SECURITY_PROTOCOL', 'SASL_SSL'),  # usa conexão segura
    'sasl.mechanisms': os.getenv('KAFKA_SASL_MECHANISMS', 'PLAIN'),         # tipo de autenticação
    'sasl.username': os.getenv('KAFKA_SASL_USERNAME'),                      # usuário (do .env)
    'sasl.password': os.getenv('KAFKA_SASL_PASSWORD'),                      # senha (do .env)
    'group.id': os.getenv('KAFKA_GROUP_ID', 'grupo-python-consumer-11'),    # nome do grupo de leitura
    'auto.offset.reset': os.getenv('KAFKA_AUTO_OFFSET_RESET', 'earliest')   # se grupo novo, começa do início
})

# Tópico que vamos ouvir (precisa ser o mesmo que o producer envia)
consumer.subscribe(['meu_primeiro_topico'])

print("📡 Aguardando mensagens do Kafka...")

try:
    while True:
        # Espera até 1 segundo por uma mensagem
        msg = consumer.poll(1.0)

        if msg is None:
            continue  # não chegou nada, tenta de novo

        if msg.error():
            print(f"Erro: {msg.error()}")  # problema ao ler do Kafka
            continue

        # Converte a chave e o valor da mensagem
        key = msg.key().decode('utf-8') if msg.key() else None
        value = json.loads(msg.value().decode('utf-8'))  # transforma JSON em dict

        # Chama o serviço que acessa o Postgres com os dados da mensagem
        query_data(
            id=value.get('id'),
            operation=value.get('operation'),
            sequence=value.get('sequence')
        )

except KeyboardInterrupt:
    print("Encerrando consumer...")  # permite parar com Ctrl+C

finally:
    consumer.close()  # fecha conexões com o Kafka
