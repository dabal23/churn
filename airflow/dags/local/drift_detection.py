from airflow import DAG
from google.cloud import bigquery
import datetime
from airflow.operators.python import PythonOperator
import pandas as pd
from data_drift_check import drift_detection



default_args = {
    'owner':'iqbal',
    'start_date':None,
    'retries':0
}

dag = DAG(
    'data_drift_detection',
    default_args=default_args
)

detect_drift = PythonOperator(
    task_id = 'drift_detection',
    python_callable=lambda: drift_detection('local'),
    dag=dag
)





detect_drift