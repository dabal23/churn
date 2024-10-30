from airflow import DAG
from airflow.providers.google.cloud.operators.bigquery import BigQueryCheckOperator
from airflow.utils.dates import days_ago
from airflow.utils.dates import days_ago

# Define the default arguments for the DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': days_ago(1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
}

# Define the DAG
with DAG(
    dag_id='check_bigquery_connection',
    default_args=default_args,
    description='A DAG to check BigQuery connection',
    schedule_interval=None,  # Set to None for manual trigger
    catchup=False,
) as dag:

    # Define the BigQuery Check Operator
    check_bigquery_connection = BigQueryCheckOperator(
        task_id='check_bigquery_connection',
        sql='SELECT 1',  # A simple query to test connectivity
        use_legacy_sql=False,  # Ensure you use standard SQL
        gcp_conn_id='gcp',  # Use the default connection ID or specify your own
    )

    check_bigquery_connection
