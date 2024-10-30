import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator
from transform import transform

default_args={
    'owner': 'iqbal',
    'start_date': None,
    'retries': 0
}

dag = DAG(
    'local_ingest_transform',
    default_args=default_args,

)

ingest_transform = PythonOperator(
    task_id='ingest_transform',
    python_callable=transform,
    dag=dag
)

ingest_transform