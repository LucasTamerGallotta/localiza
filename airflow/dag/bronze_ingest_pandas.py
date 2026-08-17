import logging
import boto3
from botocore.client import Config
import pandas as pd
import io
import datetime

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


# #Upload to MinIO Landing e bronze
def ingest_landing_bronze(df_bronze: pd.DataFrame,file_name:str, s3_client:boto3.client) -> None:

    parquet_buffer = io.BytesIO()
    df_bronze.to_parquet(parquet_buffer, index=False)

    logger.info(f"Inserindo o arquivo {file_name} no landing-bucket")
    s3_client.put_object(
        Bucket='landing-bucket',
        Key=file_name,
        Body=parquet_buffer.getvalue(),
    )

    logger.info(f"Inserindo o arquivo {file_name} no bronze-bucket")
    s3_client.put_object(
        Bucket='bronze-bucket',
        Key=file_name,
        Body=parquet_buffer.getvalue(),
    )
    pass


def call_ingest():
    #   file_path = "./dataset/df_fraud_credit.csv"
      s3_client = create_s3_cliente()
      df_bronze = ingest_csv_to_minio("/usr/local/airflow/dataset/df_fraud_credit.csv")
      today = datetime.datetime.now()
      file_name = today.strftime("fraud_credit_%Y%m%d.parquet")
    #   ingest_landing_bronze(df_bronze, file_name, s3_client)

call_ingest()


