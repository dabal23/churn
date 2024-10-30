from airflow import DAG
import datetime
from airflow.operators.python import PythonOperator
from google.cloud import bigquery
from data_drift_check import drift_detection, upload_to_bigquery




default_args = {
    'owner':'iqbal',
    'start_date':None,
    'retries':0
}

dag = DAG(
    'data_drift_detection_gcp',
    default_args=default_args
)

detect_drift = PythonOperator(
    task_id = 'drift_detection_gcp',
    python_callable=lambda: drift_detection('gcp'),
    dag=dag
)


detect_drift 