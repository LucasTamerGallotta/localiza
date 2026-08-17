import logging
import psycopg2
import os
import json
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
    print("Criando o schema e a tabela, caso ainda não exista.")
    try:
        cursor = conn.cursor()
        cursor.execute("""
            CREATE SCHEMA IF NOT EXISTS localiza;
            DROP TABLE IF EXISTS localiza.bronze_ingest;
            CREATE TABLE IF NOT EXISTS localiza.bronze_ingest
            (
                timestamp DATE,
                sending_address TEXT,
                receiving_address TEXT,
                amount DOUBLE PRECISION,
                transaction_type TEXT,
                location_region TEXT,
                ip_prefix DOUBLE PRECISION,
                login_frequency BIGINT,
                session_duration BIGINT,
                purchase_pattern TEXT,
                age_group TEXT,
                risk_score DOUBLE PRECISION,
                anomaly TEXT);
            """)
        conn.commit()
        print("Schema e tabela bronze criados com sucesso!")
    except psycopg2.Error as e:
        print(f"Falha ao criar a tabela bronze: {e}")
    pass


# Estabelecendo a conexão via psycopg2

def insert_dataframe(conn, dataframe, table_name):
    #Convertendo as linhas do Dataframe em uma tupla
    tuples = [tuple(x) for x in dataframe.to_numpy()]
    
    # Recuperando o nome das colunas
    cols = ','.join(list(dataframe.columns))
    
    # Setando o SQL que será executado
    query = f"INSERT INTO {table_name} ({cols}) VALUES %s"
    
    cursor = conn.cursor()
    try:
        # Inserindo os dados
        extras.execute_values(cursor, query, tuples)
        conn.commit()
        print(f"Successfully inserted {len(dataframe)} rows.")
    except (Exception, psycopg2.DatabaseError) as error:
        print(f"Error: {error}")
        conn.rollback()
    finally:
        cursor.close()


def cria_df(file_path:str)-> pd.DataFrame:
    return pd.read_csv(file_path)


def main():
    try:
        conn = connect_to_db()
        create_table(conn)
        df_bronze = pd.read_csv("/usr/local/airflow/dataset/df_fraud_credit.csv")
        # df_bronze = cria_df('./dataset/df_fraud_credit.csv')

        df_bronze = df_bronze[df_bronze['risk_score'].str.lower() != 'none']
        df_bronze['risk_score'] = pd.to_numeric(df_bronze['risk_score'], errors='ignore')
        df_bronze = df_bronze[df_bronze['amount'].str.lower() != 'none']
        df_bronze['amount'] = pd.to_numeric(df_bronze['amount'], errors='ignore')
        df_bronze['timestamp'] = pd.to_datetime(df_bronze['timestamp'], unit='s')
        
        insert_dataframe(conn, df_bronze,"localiza.bronze_ingest")
    except Exception as e:
        print(f"Ocorreu um erro: {e}")
    finally:
        if 'conn' in locals():
            conn.close()
            print("Fechando a conexão")

main()