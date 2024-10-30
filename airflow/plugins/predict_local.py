import pandas as pd
import pickle
import os
import re
from xgboost import XGBClassifier

# open reverse encoder
with open('artefacts/reverse_encoder.pkl', 'rb') as file:
        reverse_encoder = pickle.load(file)

def load_model():
    path = './artefacts/model'
    files = os.listdir(path)
    count_list = [int(re.search(r'model_xgb_(\d+)\.pkl', f).group(1)) for f in files]
    counter = max(count_list)
    model_path = f'./artefacts/model/model_xgb_{counter}.pkl'
    return model_path

def load_transformed_data():
    path = './datas/transformed'
    files = os.listdir(path)
    count_list = [int(re.search(r'transformed_(\d+)\.csv',f).group(1)) for f in files]
    counter = max(count_list)
    df_path = f'./datas/transformed/transformed_{counter}.csv'
    return df_path

def prediction_path():
    path = './datas/prediction'
    files = os.listdir(path)
    if files:
        count_list = [int(re.search(r'prediction_(\d+)\.csv',f).group(1)) for f in files]
        counter = max(count_list)+1
        predict_path = f'./datas/prediction/prediction_{counter}.csv'

    else:
        predict_path = './datas/prediction/prediction_0.csv'

    return predict_path

def predict():
    print('retrieve the model')
    print(load_model())
    with open(load_model(), 'rb') as file:
        model = pickle.load(file)

    print('fetch the data')
    df = pd.read_csv(load_transformed_data())

    print('delete customer id')
    df.drop(['CustomerID','Churn'],axis=1, inplace=True)

    print('make prediction')
    df['predict'] = model.predict(df)

    print('reverse encoding')
    df.replace(reverse_encoder,inplace=True)

    print('save prediction')
    df.to_csv(prediction_path(), index=False)

    print('prediction saved')




