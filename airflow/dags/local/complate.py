import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator
from transform import transform
from predict_local import predict
from data_drift_check import drift_detection


default_args = {
    'owner':'iqbal',
    'start_date':None,
    'retries':0
}

dag = DAG(
    'churn_dag',
    default_args = default_args
)

transform_data = PythonOperator(
    task_id = 'transform',
    python_callable=transform,
    dag=dag
)

predict_data = PythonOperator(
    task_id='prediction',
    python_callable=predict,
    dag=dag
)

detect_drift = PythonOperator(
    task_id = 'drift_detection',
    python_callable=lambda: drift_detection('local'),
    dag=dag
)

transform_data >> predict_data >> detect_drift