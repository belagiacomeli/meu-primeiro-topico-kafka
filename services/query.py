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