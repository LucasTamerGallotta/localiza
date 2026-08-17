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

def ingest_gold(df_silver:DataFrame, spark, s3_client:boto3.client) -> None:
    today = datetime.datetime.now()

    df_gold1 = df_silver.query ("""
                        SELECT location_region_tratada,
                               AVG(risk_score) AS media_risk_score
                        FROM silver
                        GROUP BY location_region_tratada
                        ORDER BY media_risk_score DESC
                        """)

    print(f"A quantidade total de registros da camada Gold primeira solução 1 é de {df_gold1.count()}")
    logger.info("Criando o gold_1.parquet com a resolução da primeira tabela")

    parquet_buffer = io.BytesIO()
    df_gold1.to_parquet(parquet_buffer, index=False)

    logger.info("Inserindo o arquivo gold_1 com a resolução da primeira tabela no gold-bucket")
    s3_client.put_object(
        Bucket='gold-bucket',
        Key=f"gold_1_{today.strftime('%Y%m%d')}.parquet",
        Body=parquet_buffer.getvalue(),
    )

    df_filtered = df_silver[df_silver["transaction_type"] == "sale"].copy()
    
    # Converte o timestamp se necessário e ordena decrescente para simular o ROW_NUMBER()
    df_filtered["timestamp"] = pd.to_datetime(df_filtered["timestamp"], unit="s")
    df_sorted = df_filtered.sort_values(
        by=["receiving_address", "timestamp"], ascending=[True, False]
    )
    
    # Mantém apenas a primeira linha por grupo (equivalente a _rn = 1)
    ranked_sales = df_sorted.drop_duplicates(
        subset=["receiving_address"], keep="first"
    )
    
    # Seleciona colunas, arredonda, adiciona _created_at e pega o top 3 por amount
    result = ranked_sales.assign(
        amount=ranked_sales["amount"].round(2),
        _created_at=pd.Timestamp.now(),
    )[["receiving_address", "amount", "timestamp", "_created_at"]]
    
    df_gold2 = result.sort_values(by="amount", ascending=False).head(3)

    print(f"A quantidade total de registros da camada Gold primeira solução 2 é de {df_gold2.count()}")
    logger.info("Criando o gold_1.parquet com a resolução da primeira tabela")


    parquet_buffer = io.BytesIO()
    df_gold2.to_parquet(parquet_buffer, index=False)


    s3_client.put_object(
        Bucket='gold-bucket',
        Key=f"gold_2_{today.strftime('%Y%m%d')}.parquet",
        Body=parquet_buffer.getvalue(),
    )
    pass



def call_ingest():
    
    #Estabelecendo conexões
    spark = create_spark_session()
    s3_client = create_s3_cliente()

    # f_silver = ingest_silver(df_bronze, file_name, spark,s3_client)
    # ingest_gold(df_silver, file_name, spark,s3_client)

    today = datetime.datetime.now()
    file_name = today.strftime("fraud_credit_%Y%m%d.parquet")


call_ingest()


