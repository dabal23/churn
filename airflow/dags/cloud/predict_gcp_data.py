import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator
from predict_gcp import predict_on_gcp

default_args = {
    'owner': 'iqbal',
    'start_date': None,  # Set a valid start date
    'retries': 0,
}

dag = DAG(
    'predict_gcp',
    default_args=default_args,
    schedule_interval=None,  # Adjust this if you want to schedule it periodically
)

predict_gcp = PythonOperator(
    task_id='predict_gcp',
    python_callable=predict_on_gcp,
    dag=dag
)

predict_gcp
