from datetime import datetime, timedelta
from crud_count.advanced_app import main
from airflow import DAG
from airflow.operators.python import PythonOperator


DAG_ID = 'vk_test'

default_args = {
    'owner': 'vmaksutova',
    'depends_on_past': False,
    'start_date': datetime(2024, 9, 22),
    'retries': 2,
    'retry_delay': timedelta(minutes=15),
    'execution_timeout': timedelta(minutes=60)
}

dag = DAG(
    dag_id=DAG_ID,
    catchup=False,
    default_args=default_args,
    schedule_interval='0 4 * * *',
    dagrun_timeout=timedelta(minutes=30),
    tags=['vk']
)

task_count = PythonOperator(
    python_callable=main,
    dag=dag,
    task_id='crud_count',
)

task_count