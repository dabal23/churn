import streamlit as st
import pandas as pd
from google.cloud import storage
import io

from google.cloud import storage

# Path to your service account key file
key_path = "../airflow/other/encoded-axis-430602-s0-d2bc2db2ceda.json"

# Initialize the Google Cloud Storage client with the service account key
client = storage.Client.from_service_account_json(key_path)

def read_data_from_gcs(bucket_name, file_path):
    bucket = client.get_bucket(bucket_name)
    blob = bucket.blob(file_path)
    data = blob.download_as_bytes()
    return data

# Streamlit app
def main():
    st.title('Data Dashboard')

    # Define your bucket and file path
    bucket_name = 'your-bucket-name'
    file_path = 'path/to/your/data.csv'  # Adjust the path and file type accordingly

    # Read data from GCS
    data = read_data_from_gcs('telco_churn_data', 'datas/prediction/prediction_0.csv')
    
    # Load data into a DataFrame
    df = pd.read_csv(io.BytesIO(data))  # Adjust if using a different format

    # Show the head of the data
    st.write('Head of the data:')
    st.write(df.head())

if __name__ == '__main__':
    main()