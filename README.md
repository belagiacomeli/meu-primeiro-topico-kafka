## Visão Geral

- Projeto em Python que demonstra integração com Apache Kafka (Confluent Cloud) e PostgreSQL.
- O `producer` publica mensagens em um tópico Kafka; o `consumer` lê essas mensagens e consulta um registro no banco baseado no `id` recebido.
- Segredos e configurações são carregados de variáveis de ambiente via `.env` usando `python-dotenv`.

## Arquitetura

- **Producer**: envia mensagens para o tópico configurado. Veja [producer.py](producer.py).
- **Consumer**: consome mensagens do tópico e chama o serviço de consulta ao banco. Veja [consumer.py](consumer.py).
- **Serviço de consulta**: realiza `SELECT` no PostgreSQL. Veja [services/query.py](services/query.py).
- **Banco (Docker)**: PostgreSQL provisionado via [docker-compose.yaml](docker-compose.yaml).

## Pré-requisitos

- Python 3.14+ instalado.
- Docker instalado (para subir PostgreSQL via Compose).
- Conta e credenciais válidas no Confluent Cloud (Kafka).

## Configuração de Ambiente

- Crie/edite o arquivo `.env` na raiz do projeto com suas credenciais e parâmetros (já existe um modelo):
	- Kafka: `KAFKA_BOOTSTRAP_SERVERS`, `KAFKA_SECURITY_PROTOCOL`, `KAFKA_SASL_MECHANISMS`, `KAFKA_SASL_USERNAME`, `KAFKA_SASL_PASSWORD`, `KAFKA_GROUP_ID`, `KAFKA_AUTO_OFFSET_RESET`, `KAFKA_SESSION_TIMEOUT_MS`, `KAFKA_CLIENT_ID`, `KAFKA_TOPIC`.
	- Postgres: `DB_HOST`, `DB_NAME`, `DB_USER`, `DB_PASSWORD`, `DB_PORT`.
- O arquivo `.env` já está ignorado pelo Git em [.gitignore](.gitignore).

## Subir o Banco com Docker

- Na pasta do projeto, execute:

```bash
docker-compose up -d
```

- Isso iniciará um container Postgres com usuário, senha e banco conforme [docker-compose.yaml](docker-compose.yaml).

## Criar Tabela e Dados de Exemplo (Opcional)

- Para testar a consulta do `consumer`, crie a tabela esperada (`vendas_sephora`) e insira alguns registros. Exemplo (ajuste conforme necessário):

```sql
CREATE TABLE IF NOT EXISTS vendas_sephora (
	id SERIAL PRIMARY KEY,
	data_venda DATE NOT NULL,
	marca TEXT,
	categoria TEXT,
	produto TEXT,
	qtd INTEGER,
	preco_unitario NUMERIC(10,2),
	valor_total NUMERIC(10,2)
);

INSERT INTO vendas_sephora (data_venda, marca, categoria, produto, qtd, preco_unitario, valor_total)
VALUES ('2025-01-01', 'MarcaX', 'SkinCare', 'Serum A', 2, 50.00, 100.00);
```

## Instalação de Dependências

```bash
pip install .
```

## Execução

- Recomenda-se iniciar primeiro o `consumer` para observar as mensagens que serão produzidas:

```bash
python consumer.py
```

- Em outro terminal, execute o `producer` para enviar mensagens ao tópico:

```bash
python producer.py
```

## Notas Importantes

- Atualize o `.env` com suas credenciais reais de Kafka e Postgres antes de rodar.
- O `consumer` consultará a tabela `vendas_sephora` por `id` e registrará se encontrou o item.
- Em ambientes Windows, execute os comandos acima em Prompt de Comando ou PowerShell.

