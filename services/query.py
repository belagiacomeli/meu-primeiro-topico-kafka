# Serviço de consulta ao Postgres:
# - Lê as credenciais do .env
# - Abre conexão com o banco
# - Executa a operação pedida (hoje: SELECT por id quando operation="insert")

import psycopg2
import json
from datetime import date
import os
from dotenv import load_dotenv

# Carrega as variáveis do arquivo .env (DB_HOST, DB_NAME, etc.)
load_dotenv()

def get_db_connection():
    """
    Cria e retorna uma conexão com o Postgres usando variáveis do .env.
    """
    conn = psycopg2.connect(
        host=os.getenv("DB_HOST", "localhost"),   # endereço do banco
        database=os.getenv("DB_NAME", "meu_banco"),  # nome do banco
        user=os.getenv("DB_USER", "admin"),       # usuário
        password=os.getenv("DB_PASSWORD"),        # senha (vem do .env)
        port=os.getenv("DB_PORT", "5432")         # porta (padrão 5432)
    )
    return conn

def processar_operacao(id_registro, operation, sequence):
    """
    Decide o que fazer com base na operação.
    - insert: faz um SELECT pelo id na tabela vendas_sephora.
    - outras operações: ainda não implementadas aqui.
    """
    conn = get_db_connection()
    cursor = conn.cursor()
    
    print(f"Consultando dados para ID: {id_registro}, Operação: {operation}, Sequência: {sequence}")
    
    try:
        if operation == "insert":
            # Consulta 1 registro pelo ID
            sql = """
                SELECT id, data_venda, marca, categoria, produto, qtd, preco_unitario, valor_total
                FROM vendas_sephora
                WHERE id = %s
            """
            cursor.execute(sql, (id_registro,))   # executa o SELECT com o parâmetro id
            row = cursor.fetchone()               # pega a primeira linha do resultado

            if row is None:
                # Não encontrou o id na tabela
                print(f"⚠️ Nenhum registro encontrado para ID: {id_registro}")
                return {"id": id_registro, "status": "not_found"}

            # Monta um dict com nomes de coluna -> valores
            colnames = [desc[0] for desc in cursor.description]
            record = dict(zip(colnames, row))
            print(f"✅ Registro encontrado: {record}")
            return {"status": "found", "record": record}

        else:
            # Qualquer operação diferente de "insert" cai aqui (não tratada ainda)
            print(f"Operação desconhecida: {operation}")
            return None

    except Exception as e:
        # Se der erro, desfaz a transação e avisa
        print(f"Erro ao acessar o banco: {e}")
        conn.rollback()
        return None
        
    finally:
        # Sempre fecha cursor e conexão, mesmo com erro
        cursor.close()
        conn.close()

def query_data(id: int, operation: str, sequence: int = None):
    """
    Função simples chamada pelo consumer para executar a operação no banco.
    """
    result = processar_operacao(id, operation, sequence)
    if result:
        return result
    return None
import psycopg2
import json
from datetime import date
import os
from dotenv import load_dotenv

# Carrega variáveis de ambiente do arquivo .env (se existir)
load_dotenv()

# 1. Configuração da Conexão (Baseado no seu Docker Compose)
def get_db_connection():
    conn = psycopg2.connect(
        host=os.getenv("DB_HOST", "localhost"),
        database=os.getenv("DB_NAME", "meu_banco"),
        user=os.getenv("DB_USER", "admin"),
        password=os.getenv("DB_PASSWORD"),
        port=os.getenv("DB_PORT", "5432")
    )
    return conn

# 2. Sua função adaptada
def processar_operacao(id_registro, operation, sequence):
    conn = get_db_connection()
    cursor = conn.cursor()
    
    print(f"Consultando dados para ID: {id_registro}, Operação: {operation}, Sequência: {sequence}")
    
    try:
        if operation == "insert":
            # Ao invés de inserir, realizamos um SELECT de um único registro
            sql = """
                SELECT id, data_venda, marca, categoria, produto, qtd, preco_unitario, valor_total
                FROM vendas_sephora
                WHERE id = %s
            """

            cursor.execute(sql, (id_registro,))
            row = cursor.fetchone()

            if row is None:
                print(f"⚠️ Nenhum registro encontrado para ID: {id_registro}")
                return {"id": id_registro, "status": "not_found"}

            # Monta um dicionário usando os nomes das colunas retornados
            colnames = [desc[0] for desc in cursor.description]
            record = dict(zip(colnames, row))
            print(f"✅ Registro encontrado: {record}")
            return {"status": "found", "record": record}

        else:   
            print(f"Operação desconhecida: {operation}")
            return None

    except Exception as e:
        print(f"Erro ao acessar o banco: {e}")
        conn.rollback() # Desfaz se der erro
        return None
        
    finally:
        cursor.close()
        conn.close()

def query_data(id: int, operation: str, sequence: int = None):
    """
        Consulta dados no banco com base no ID e operação fornecidos.
    """
    result = processar_operacao(id, operation, sequence)
    if result:  
        return result
    return None