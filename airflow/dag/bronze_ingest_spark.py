import logging
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, upper, current_timestamp, lit
from pyspark.sql import DataFrame
from datetime import datetime
import boto3
from botocore.client import Config
import pandas as pd
import io
import datetime

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def create_spark_session() -> SparkSession.builder:
    logger.info("Criando o SparkSession...")
    """Create Spark session with MinIO S3 configuration"""
    return SparkSession.builder \
        .appName("MinIO Data Processing") \
        .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000") \
        .config("spark.hadoop.fs.s3a.access.key", "minioadmin") \
        .config("spark.hadoop.fs.s3a.secret.key", "minioadmin") \
        .config("spark.hadoop.fs.s3a.path.style.access", "true") \
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
        .config("spark.hadoop.fs.s3a.aws.credentials.provider", "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider") \
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false") \
        .config("spark.sql.adaptive.enabled", "true") \
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
        .getOrCreate()

def create_s3_cliente() -> boto3.client:
    logger.info("Criando a conexão com o Minio...")
    return boto3.client(
        's3',
        endpoint_url='http://localhost:9000',  # Access host from container
        aws_access_key_id='minioadmin',
        aws_secret_access_key='minioadmin',
        config=Config(signature_version='s3v4'),
        region_name='us-east-1'
    )


def ingest_csv_to_minio(path_file,spark) -> DataFrame:
    logger.info("Lendo o arquivo df_fraud_credit.csv do diretório ./dataset")
    df_bronze = spark.read \
        .option("header", "true") \
        .option("inferSchema", "true") \
        .csv(path_file)
    return df_bronze

# #Upload to MinIO Landing e bronze
def ingest_landing_bronze(df_bronze: DataFrame,file_name:str, s3_client:boto3.client) -> None:

    logger.info("Criando a tabela temporaria do bronze ...")
    print(f"A quantidade total de registros na camada Bronze é de {df_bronze.count()} .")

    df_bronze_w = df_bronze.toPandas()
    parquet_buffer = io.BytesIO()
    df_bronze_w.to_parquet(parquet_buffer, index=False)

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
    
    #Estabelecendo conexões
    spark = create_spark_session()
    s3_client = create_s3_cliente()
    df_bronze = ingest_csv_to_minio("./dataset",spark)

    today = datetime.datetime.now()
    file_name = today.strftime("fraud_credit_%Y%m%d.parquet")

    ingest_landing_bronze(df_bronze,file_name,s3_client)


call_ingest()


