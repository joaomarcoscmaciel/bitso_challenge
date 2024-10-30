import sys
import os

# Add the project root to the Python path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
from etl.main import run_etl_pipeline

default_args = {
    'owner': 'admin',
    'depends_on_past': False,
    'start_date': datetime(2024, 10, 30),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    'daily_incremental_etl',
    default_args=default_args,
    description='Daily ETL for incremental updates',
    schedule_interval='@daily',
    catchup=False,
) as dag:

    run_incremental_etl = PythonOperator(
        task_id='run_incremental_etl',
        python_callable=run_etl_pipeline,
    )

    run_incremental_etl
