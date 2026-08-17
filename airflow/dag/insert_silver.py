import logging
import psycopg2
import pandas as pd
import psycopg2.extras as extras



# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

"""
   Função para se conectar ao banco de dados PostgreSQL a partir do arquivo de variáveis (.env). 
   Caso não obtenha sucesso, retorna um erro.
   
   Args:
        None.

   Returns:
        Retorna a conexão com o banco PostgreSQL.
"""

def connect_to_db()-> psycopg2.connect:
    print("Conectando ao PostgreSQL")
    try:
        conn = psycopg2.connect(
            host="db",
            port="5432",
            dbname="db",
            user="db_user",
            password="db_password"
        )
        return(conn)
    except psycopg2.Error as e:
        print(f"Erro ao tentar se conectar ao PostgreSQL: {e}")
        raise

"""
    Função para criar o schema 'localiza' e a tabela bronze, caso ainda não existam.

    Args:
        conn:psycopg2.connect Conexão com a base de dados PostgreSQL.
    
    Return: 
        None.
"""


def create_table(conn : psycopg2.connect)-> None:
    print("Criando o schema e a tabela silver, caso ainda não exista.")
    try:
        cursor = conn.cursor()
        cursor.execute("""
            DROP TABLE IF EXISTS localiza.silver_ingest;
            CREATE TABLE IF NOT EXISTS localiza.silver_ingest AS
            WITH NomeDaCTE AS (
            SELECT 
               timestamp,
               sending_address,
               receiving_address,
               amount,
               transaction_type,
               CASE
                         WHEN location_region = '0' THEN 'Oceania'
                         ELSE location_region
                     END AS location_region_tratado,
               ip_prefix,
               login_frequency,
               session_duration,
               purchase_pattern,
               age_group,
               risk_score,
               anomaly
            FROM localiza.bronze_ingest
         )
         SELECT * FROM NomeDaCTE;
            """)
        conn.commit()
        print("Schema e tabela silver criados com sucesso!")
    except psycopg2.Error as e:
        print(f"Falha ao criar a tabela silver: {e}")
    pass

def cria_df(file_path:str)-> pd.DataFrame:
    return pd.read_csv(file_path)


def main():
    
    conn = connect_to_db()
    create_table(conn)
    try:
        conn = connect_to_db()
        create_table(conn)
    except Exception as e:
        print(f"Ocorreu um erro: {e}")
    finally:
        if 'conn' in locals():
            conn.close()
            print("Fechando a conexão")
main()