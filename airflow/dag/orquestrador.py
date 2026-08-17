from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator
from datetime import datetime, timedelta


def self_main_bronze_callable():
    from insert_bronze import main
    return main()

def self_main_silver_callable():
    from insert_silver import main
    return main()


def self_main_gold_callable():
    from insert_gold import main
    return main()

default_args = {
    'description':'Uma DAG de oesquestração',
    'depends_on_past': False,
    'start_date': datetime(2026, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
}


def main_dag():
    dag = DAG(
        dag_id="Ingestão_de_Dados",
        default_args=default_args,
        schedule=timedelta(minutes=10)
     )
    with dag:
        ingestao_bronze = PythonOperator(
            task_id="ingest_landing_bronze",
            python_callable=self_main_bronze_callable
        )
    with dag:
        ingestao_silver = PythonOperator(
            task_id="ingest_silver",
            python_callable=self_main_silver_callable    
        )
    ingestao_bronze >> ingestao_silver
    
    with dag:
        ingestao_gold = PythonOperator(
            task_id="ingest_gold",
            python_callable=self_main_gold_callable
        )
    ingestao_bronze >> ingestao_silver >> ingestao_gold
    


main_dag_instance = main_dag()
