import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator
from predict_local import predict

default_args = {
    'owner': 'iqbal',
    'start_date': None,
    'retries': 0
}

dag = DAG(
    'local_predict_local_1',
    default_args=default_args
)

prediction = PythonOperator(
    task_id='prediction',
    python_callable=predict,
    dag=dag
)

prediction