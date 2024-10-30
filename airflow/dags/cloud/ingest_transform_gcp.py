import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator
from ingest_transform_gcp import transform_gcp

default_args = {
    'owner': 'iqbal',
    'start_date': datetime.datetime(2024, 8, 27),  # Set a valid start date
    'retries': 0,
}

dag = DAG(
    'ingest_transform_gcp',
    default_args=default_args,
    schedule_interval=None,  # Adjust this if you want to schedule it periodically
)

ingest_transform = PythonOperator(
    task_id='ingest_transform',
    python_callable=transform_gcp,
    dag=dag
)

ingest_transform
