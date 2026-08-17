import logging
import boto3
from botocore.client import Config
import pandas as pd
import io
import datetime
from bronze_ingest_pandas import ingest_landing_bronze
import numpy as np



# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def create_s3_cliente() -> boto3.client:
    logger.info("Criando a conexão com o Minio...")
    return boto3.client(
        's3',
        endpoint_url='http://localhost:9001',  # Access host from container
        aws_access_key_id='minioadmin',
        aws_secret_access_key='minioadmin',
        config=Config(signature_version='s3v4'),
        region_name='us-east-1'
    )


def ingest_csv_to_minio(path_file) -> pd.DataFrame:
      logger.info("Lendo o arquivo df_fraud_credit.csv do diretório ./dataset")
      return pd.read_csv(path_file)

def ingest_silver(df_bronze:pd.DataFrame,file_name:str,s3_client)-> pd.DataFrame:

    df_silver = df_bronze.copy()

    df_bronze['location_region_tratada'] = np.where(
        df_bronze['location_region'] == '0', 'Oceania', df_bronze['location_region']
    )

    # Aplicando DISTINCT para remover linhas duplicadas
    df_silver = df_silver.drop_duplicates()

    print(f"A quantidade total de registros na camada Silver é de {df_silver.count()}")

    logger.info("Criando a tabela silver com os devidos tratamentos ...")
    logger.info("Inserindo o arquivo {file_name} tratado no silver-bucket")


    parquet_buffer = io.BytesIO()
    df_silver.to_parquet(parquet_buffer, index=False)

    s3_client.put_object(
        Bucket='silver-bucket',
        Key=file_name,
        Body=parquet_buffer.getvalue(),
    )
    return df_silver


def call_ingest():
      s3_client = create_s3_cliente()
      df_bronze = ingest_csv_to_minio("/usr/local/airflow/dataset/df_fraud_credit.csv")
      today = datetime.datetime.now()
      file_name = today.strftime("fraud_credit_%Y%m%d.parquet")
      ingest_silver(df_bronze, file_name, s3_client)
      

call_ingest()

