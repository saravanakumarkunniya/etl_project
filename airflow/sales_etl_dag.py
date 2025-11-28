from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
from src.etl_job import etl_process

default_args = {
    "owner": "airflow",
    "start_date": datetime(2025, 11, 28),
    "retries": 1
}

with DAG("sales_etl_dag", default_args=default_args, schedule_interval="@daily", catchup=False) as dag:
    run_etl = PythonOperator(
        task_id="run_etl_job",
        python_callable=etl_process
    )
