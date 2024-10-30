from google.cloud import storage
from airflow.providers.google.cloud.hooks.gcs import GCSHook
import pandas as pd
import pickle
import re
import io
from xgboost import XGBClassifier

def get_gcs_client():
    hook = GCSHook(gcp_conn_id='gcp')
    credentials = hook.get_credentials()
    client = storage.Client(credentials=credentials, project=hook.project_id)
    return client

def model_path(client, bucket_name):
    bucket = client.get_bucket(bucket_name)
    prefix = 'artefacts/model/'
    blobs = bucket.list_blobs(prefix=prefix)
    files = [blob.name for blob in blobs if re.search(r'model_xgb_(\d+)\.pkl', blob.name)]
    count_list = [int(re.search(r'model_xgb_(\d+)\.pkl',f).group(1)) for f in files]
    counter = max(count_list)
    model_path = f'artefacts/model/model_xgb_{counter}.pkl'
    print(files)
    return model_path

def data_path(client,bucket_name):
    bucket = client.get_bucket(bucket_name)
    prefix = 'datas/transformed/'
    blobs = bucket.list_blobs(prefix=prefix)
    files = [blob.name for blob in blobs if re.search(r'transformed_(\d+)\.csv', blob.name)]
    print(files)
    count_list = [int(re.search(r'transformed_(\d+)\.csv',f).group(1)) for f in files]
    print(files)
    counter = max(count_list)
    data_path = f'datas/transformed/transformed_{counter}.csv'
    return data_path

def prediction_path(client, bucket_name):
    bucket = client.get_bucket(bucket_name)
    prefix = 'datas/prediction'
    blobs = bucket.list_blobs(prefix=prefix)
    files = [blob.name for blob in blobs if re.search(r'prediction_(\d+)\.csv', blob.name)]
    if files:
        count_list = [int(re.search(r'prediction_(\d+)\.csv',f).group(1)) for f in files]
        counter = max(count_list)
    else:
        counter = 0
    return counter


def predict_on_gcp():
    print('get gcs client')
    client = get_gcs_client()

    bucket_name = 'telco_churn_data'
    model_path_val = model_path(client, bucket_name)
    data_path_val = data_path(client, bucket_name)
    predict_counter = prediction_path(client, bucket_name)

    print('load model')
    bucket = client.get_bucket(bucket_name)
    blob = bucket.blob(model_path_val)
    data = blob.download_as_bytes()
    model = pickle.load(io.BytesIO(data))

    print('load data')
    bucket = client.get_bucket(bucket_name)
    blob = bucket.blob(data_path_val)
    data = blob.download_as_bytes()
    df = pd.read_csv(io.BytesIO(data))

    print('delete customer id')
    df.drop(['CustomerID','Churn'],axis=1, inplace=True)
    df['predict'] = model.predict(df)

    prediction_blob = bucket.blob(f'datas/prediction/prediction_{predict_counter}.csv')
    with io.BytesIO() as output:
        df.to_csv(output, index=False)
        output.seek(0)
        prediction_blob.upload_from_file(output, content_type='text/csv')

    
    print(f'Predictions saved to data/prediction/prediction_{predict_counter}.csv')

