import pandas as pd
import pickle
import os
import re
from xgboost import XGBClassifier
from sklearn import train_test_split



# load
def check_model():
    path = './artefacts/model'
    files = os.listdir(path)
    count_list = [int(re.search(r'model_xgb_(\d+)\.pkl', f).group(1)) for f in files]
    counter = max(count_list)
    model_path = f'./artefacts/model/model_xgb_{counter}.pkl'
    return model_path

# load latest curr data
def load_transformed_data():
    path = './datas/prediction'
    files = os.listdir(path)
    count_list = [int(re.search(r'prediction(\d+)\.csv',f).group(1)) for f in files]
    counter = max(count_list)
    df_path = f'./datas/prediction/prediction{counter}.csv'
    print('fetch the data')
    df = pd.read_csv(df_path)
    return df

# load latest ref data
def load_reff_data():
    path = './datas/reference'
    files = os.listdir(path)
    count_list = [int(re.search(r'reference_data_(\d+)\.csv',f).group(1)) for f in files]
    counter = max(count_list)
    df_path = f'./datas/prediction/reference{counter}.csv'
    print('fetch the data')
    df = pd.read_csv(df_path)
    return df

# combine
def new_cuur_data():
    reff = load_reff_data()
    curr = load_transformed_data()

    comb = pd.concat([reff,curr], ignore_index=True)
    return comb

# preprocessing
X = df.drop
X_train, X_test, y_train, y_test = train_test_split(X,y,test_size=.2,random_state=1)