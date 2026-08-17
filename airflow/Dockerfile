# ── Stage 2: Airflow with Spark client ────────────────────────────────────────
FROM apache/airflow:2.9.3-python3.11

USER root

RUN apt-get update && apt-get install -y --no-install-recommends procps && \
    rm -rf /var/lib/apt/lists/*

# Bring in Spark binaries and JARs from stage 1 (no separate download)
COPY --from=spark-with-jars /opt/spark /opt/spark
COPY --from=spark-with-jars /opt/java  /opt/java

ENV SPARK_HOME=/opt/spark
ENV JAVA_HOME=/opt/java/openjdk
ENV PATH=${PATH}:${SPARK_HOME}/bin

USER airflow



COPY ./dataset/df_fraud_credit.csv /usr/local/airflow/dataset/df_fraud_credit.csv

COPY requirements.txt /tmp/airflow-requirements.txt
RUN pip install --no-cache-dir -r /tmp/airflow-requirements.txt
RUN pip install psycopg2
