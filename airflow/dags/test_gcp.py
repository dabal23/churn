from airflow import DAG
from airflow.providers.google.cloud.sensors.gcs import GCSObjectExistenceSensor
from airflow.utils.dates import days_ago

# Define default arguments for the DAG
default_args = {
    'start_date': days_ago(1),
}

# Define the DAG
with DAG(
    dag_id='gcs_folder_check_dag',
    default_args=default_args,
    schedule_interval=None,  # This DAG runs on demand
    catchup=False,
) as dag:

    # Task to check if a folder (prefix) exists in a GCS bucket
    check_gcs_folder = GCSObjectExistenceSensor(
        task_id='check_gcs_folder',
        bucket='telco_churn_data',  # Replace with your GCS bucket name
        object='transformed/',  # Replace with the folder/prefix you want to check
        google_cloud_conn_id='gcp',  # Replace if you have a different connection ID
        timeout=10,  # Optional: Set timeout for the sensor (in seconds)
        poke_interval=10,  # Optional: How often to check (in seconds)
    )

    # You can add more tasks here

    # Set task dependencies (if needed)
    check_gcs_folder