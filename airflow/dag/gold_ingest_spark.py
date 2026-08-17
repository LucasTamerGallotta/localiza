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

    df_silver.createOrReplaceTempView("silver")
    df_gold1 = spark.sql("""
                        SELECT location_region_tratada,
                               AVG(risk_score) AS media_risk_score
                        FROM silver
                        GROUP BY location_region_tratada
                        ORDER BY media_risk_score DESC
                        """)

    print(f"A quantidade total de registros da camada Gold primeira solução 1 é de {df_gold1.count()}")
    logger.info("Criando o gold_1.parquet com a resolução da primeira tabela")

    df_gold1_w = df_gold1.toPandas()
    parquet_buffer = io.BytesIO()
    df_gold1_w.to_parquet(parquet_buffer, index=False)

    logger.info("Inserindo o arquivo gold_1 com a resolução da primeira tabela no gold-bucket")
    s3_client.put_object(
        Bucket='gold-bucket',
        Key=f"gold_1_{today.strftime('%Y%m%d')}.parquet",
        Body=parquet_buffer.getvalue(),
    )

    df_gold2 = spark.sql("""
        WITH ranked_sales AS (
        SELECT 
            receiving_address,
            amount,
            FROM_UNIXTIME(timestamp) timestamp,
            ROW_NUMBER() OVER (
                PARTITION BY receiving_address 
                ORDER BY `timestamp` DESC
            ) AS _rn
        FROM 
            silver
        WHERE 
            transaction_type = 'sale'
    )
        SELECT 
            receiving_address,
            ROUND(amount, 2) AS amount,
            `timestamp`,
            CURRENT_TIMESTAMP() AS _created_at
        FROM 
            ranked_sales
        WHERE 
            _rn = 1
        ORDER BY 
            amount DESC
        LIMIT 3""")

    print(f"A quantidade total de registros da camada Gold primeira solução 2 é de {df_gold2.count()}")
    logger.info("Criando o gold_1.parquet com a resolução da primeira tabela")


    df_gold2_w = df_gold2.toPandas()
    parquet_buffer = io.BytesIO()
    df_gold2_w.to_parquet(parquet_buffer, index=False)


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


