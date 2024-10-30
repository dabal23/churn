import evidently
import pandas as pd
from evidently.report import Report
# from evidently.metric_preset import DataDriftPreset
from evidently.metrics import ColumnDriftMetric, DatasetDriftMetric
from evidently import ColumnMapping
from google.cloud import storage
from airflow.providers.google.cloud.hooks.gcs import GCSHook
from airflow.providers.google.cloud.hooks.bigquery import BigQueryHook
from google.cloud import bigquery
import os
import re
import io
import pickle

# load data >> report
def load_ref():
    path = './datas/reference'
    files = os.listdir(path)
    print(files)
    count_data = [int(re.search(r'reference_data_(\d+)\.csv',f).group(1)) for f in files]
    counter = max(count_data)
    ref_path = f'./datas/reference/reference_data_{counter}.csv'
    df = pd.read_csv(ref_path)
    return df

def load_curr():
    path = './datas/prediction'
    files = os.listdir(path)
    count_data = [int(re.search(r'prediction_(\d+)\.csv',f).group(1)) for f in files]
    counter = max(count_data)
    pred_path = f'./datas/prediction/prediction_{counter}.csv'
    df = pd.read_csv(pred_path)
    return df, counter

def load_encoder():
    with open('./artefacts/encoder.pkl', 'rb') as file:
        encoder = pickle.load(file)
    
    reverse_encoder = {col:{i:j for j,i in mapping.items()} for col, mapping in encoder.items()}
    return reverse_encoder

# gcp
def get_gcs_client():
    hook = GCSHook(gcp_conn_id='gcp')
    credentials = hook.get_credentials()
    client = storage.Client(credentials=credentials, project=hook.project_id)
    return client

def load_ref_gcp(client,bucket_name):
    bucket = client.get_bucket(bucket_name)
    prefix = 'datas/reference/'
    blobs = bucket.list_blobs(prefix=prefix)
    files = [blob.name for blob in blobs if re.search(r'reference_data_(\d+)\.csv', blob.name)]
    print(files)
    count_list = [int(re.search(r'reference_data_(\d+)\.csv',f).group(1)) for f in files]
    print(count_list)
    counter = max(count_list)
    ref_path = f'datas/reference/reference_data_{counter}.csv'
    blob = bucket.blob(ref_path)
    data = blob.download_as_bytes()
    df = pd.read_csv(io.BytesIO(data))
    print('ref loaded')
    return df

def load_curr_gcp(client,bucket_name):
    bucket = client.get_bucket(bucket_name)
    prefix = 'datas/prediction/'
    blobs = bucket.list_blobs(prefix=prefix)
    files = [blob.name for blob in blobs if re.search(r'prediction_(\d+)\.csv', blob.name)]
    count_list = [int(re.search(r'prediction_(\d+)\.csv',f).group(1)) for f in files]
    counter = max(count_list)
    curr_path = f'datas/prediction/prediction_{counter}.csv'
    blob = bucket.blob(curr_path)
    data = blob.download_as_bytes()
    df = pd.read_csv(io.BytesIO(data))
    print('curr loaded')
    return df, counter

def upload_to_bigquery(data, project_id, dataset_id, table_id):
    hook = BigQueryHook(gcp_conn_id='gcp')
    credentials = hook.get_credentials()
    client = bigquery.Client(credentials=credentials, project=hook.project_id)
    table_ref = f'{project_id}.{dataset_id}.{table_id}'

    errors = client.insert_rows_json(table_ref, data)
    if errors:
        print(f'Error uploading to BigQuery: {errors}')
    else:
        print('Data uploaded successfully')


def drift_detection(mode):

    print(mode)
    if mode == 'local':
        ref_data = load_ref()
        curr_data, counter = load_curr()
    elif mode == 'gcp':
        client = get_gcs_client()
        bucket_name = 'telco_churn_data'
        ref_data = load_ref_gcp(client,bucket_name)
        curr_data, counter = load_curr_gcp(client,bucket_name)
    else:
        print('mode not recognize')


    print('load data')
    ref = ref_data
    curr = curr_data
    encoder = load_encoder()

    print('preprocessing')
    ref.drop('CustomerID',axis=1,inplace=True)
    ref.rename(columns={'Churn':'predict'},inplace=True)

    print('drift detection start')
    curr_data.replace(encoder,inplace=True)

    col_map = ColumnMapping(
        target=None,
        prediction='predict'
    )

    report = Report(metrics=[
        ColumnDriftMetric(column_name='predict'),
        DatasetDriftMetric()
    ])

    report.run(reference_data=ref,current_data=curr,column_mapping=col_map)

    drift_score = report.as_dict()['metrics'][0]['result']['drift_score']
    drift_detected = report.as_dict()['metrics'][0]['result']['drift_detected']
    dataset_drift = report.as_dict()['metrics'][1]['result']['dataset_drift'] 
    print(drift_score, drift_detected, dataset_drift)
    data = [{
        'id': counter,
        'drift_score':drift_score,
        'drift_status':drift_detected,
        'dataset_drift':dataset_drift
    }]
    
       # Upload data to BigQuery
    upload_to_bigquery(
        data=data,
        project_id='encoded-axis-430602-s0',      
        dataset_id='churnDrift',      
        table_id='drift'           
    )

    return data


 
