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
    print("Criando o schema e a tabela gold1, caso ainda não exista.")
    try:
        cursor = conn.cursor()
        cursor.execute("""
            DROP TABLE IF EXISTS localiza.gold_ingest;
            CREATE TABLE IF NOT EXISTS localiza.gold1_ingest AS
            
            WITH NomeDaCTE AS (
            SELECT location_region_tratado,
                                AVG(risk_score) AS media_risk_score
                         FROM localiza.silver_ingest
                         GROUP BY location_region_tratado
                         ORDER BY media_risk_score DESC
            )
            SELECT * FROM NomeDaCTE;
            """)
        conn.commit()
        print("Schema e tabela gold criado com sucesso!")
    except psycopg2.Error as e:
        print(f"Falha ao criar a tabela gold: {e}")
    pass


def create_table2(conn : psycopg2.connect)-> None:
    print("Criando o schema e a tabela gold1, caso ainda não exista.")
    try:
        cursor = conn.cursor()
        cursor.execute("""
            DROP TABLE IF EXISTS localiza.gold2_ingest;
            CREATE TABLE IF NOT EXISTS localiza.gold2_ingest AS
            WITH ranked_sales AS (
                SELECT 
                    receiving_address,
                    amount,
                    timestamp,
                    ROW_NUMBER() OVER (
                        PARTITION BY receiving_address 
                        ORDER BY timestamp DESC
                    ) AS _rn
                FROM localiza.silver_ingest
                WHERE transaction_type = 'sale'
            )
            SELECT 
                receiving_address,
                amount,
                timestamp
            FROM ranked_sales
            WHERE _rn = 1
            ORDER BY amount DESC
            LIMIT 3;
            """)
        conn.commit()
        print("Schema e tabela gold criado com sucesso!")
    except psycopg2.Error as e:
        print(f"Falha ao criar a tabela gold: {e}")
    pass

def cria_df(file_path:str)-> pd.DataFrame:
    return pd.read_csv(file_path)


def main():
    try:
        conn = connect_to_db()
        create_table(conn)
        create_table2(conn)

    except Exception as e:
        print(f"Ocorreu um erro: {e}")
    finally:
        if 'conn' in locals():
            # conn.close()
            print("Fechando a conexão")
main()