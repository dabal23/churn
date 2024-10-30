from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator
from airflow.utils.dates import days_ago

# Default arguments
default_args = {
    'owner': 'airflow',
    'start_date': None,
    'retries': 0,
}

# Define a Python function to generate the SQL insert query
def create_insert_query(**context):
    # Your Python dictionary (could also be fetched from an external source)
    data = [
        {"name": "John Doe", "age": 28, "city": "New York"},
        {"name": "Jane Smith", "age": 34, "city": "Los Angeles"},
        {"name": "Sam Johnson", "age": 45, "city": "Chicago"},
    ]

    # Create SQL insert statements from the dictionary
    rows = [
        f"('{item['name']}', {item['age']}, '{item['city']}')" 
        for item in data
    ]
    
    # Complete SQL query for BigQuery INSERT
    insert_query = f"""
        INSERT INTO `encoded-axis-430602-s0.churnDrift.test` (name, age, city)
        VALUES {', '.join(rows)}
    """
    
    # Push the query to XCom
    context['ti'].xcom_push(key='insert_query', value=insert_query)

# Define the DAG
with DAG(
    dag_id='insert_dict_into_bigquery',
    default_args=default_args,
    schedule_interval=None,  # This DAG will run manually or with a trigger
    catchup=False,
    tags=['bigquery', 'insert'],
) as dag:

    # Task to generate the SQL insert query
    generate_insert_query = PythonOperator(
        task_id='generate_insert_query',
        python_callable=create_insert_query,
    )

    # Task to insert the data into BigQuery
    insert_data_to_bq = BigQueryInsertJobOperator(
        task_id='insert_data_to_bq',
        configuration={
            "query": {
                "query": "{{ ti.xcom_pull(task_ids='generate_insert_query', key='insert_query') }}",
                "useLegacySql": False,
            }
        },
        gcp_conn_id='gcp'
    )

    # Set task dependencies
    generate_insert_query >> insert_data_to_bq
