from google.cloud import storage
from airflow.providers.google.cloud.hooks.gcs import GCSHook
import pandas as pd
import pickle
import re
import io

def get_gcs_client():
    hook = GCSHook(gcp_conn_id='gcp')
    credentials = hook.get_credentials()
    client = storage.Client(credentials=credentials, project=hook.project_id)
    return client

def load_encoder(client, bucket_name):
    bucket = client.get_bucket(bucket_name)
    encoder_blob = bucket.blob('artefacts/encoder.pkl')
    encoder_data = encoder_blob.download_as_string()
    encoder = pickle.loads(encoder_data)
    return encoder

def load_data(client, bucket_name):
    bucket = client.get_bucket(bucket_name)
    prefix = 'datas/transformed/'
    blobs = bucket.list_blobs(prefix=prefix)
    files = [blob.name for blob in blobs if re.search(r'transformed_(\d+)\.csv', blob.name)]

    if files:
        count_list = [int(re.search(r'transformed_(\d+)\.csv', f).group(1)) for f in files]
        counter = max(count_list) + 1
        df_path = f'datas/raw/raw_data_{counter}.csv'
    else:
        df_path = 'datas/raw/raw_data_1.csv'
        counter = 1

    return counter, df_path

def transform_gcp():
    print('get GCS client')
    client = get_gcs_client()

    print('load encoder')
    bucket_name = 'telco_churn_data'
    encoder = load_encoder(client, bucket_name)

    print('load data')
    counter, df_path = load_data(client, bucket_name)
    print(df_path)
    
    # Load data from GCS
    bucket = client.get_bucket(bucket_name)
    blob = bucket.blob(df_path)
    data = blob.download_as_string()
    df = pd.read_csv(io.BytesIO(data))
    
    print('data loaded')
    print('transform data')
    df.replace(encoder, inplace=True)
    
    print('save data')

    # Save the transformed data back to GCS
    transformed_blob = bucket.blob(f'datas/transformed/transformed_{counter}.csv')
    output_buffer = io.StringIO()
    df.to_csv(output_buffer, index=False)
    transformed_blob.upload_from_string(output_buffer.getvalue())
    
    print('data saved')
