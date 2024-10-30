import pandas as pd
import pickle
import os
import re

with open('artefacts/encoder.pkl', 'rb') as file:
        encoder = pickle.load(file)

def load_data():
    path = './datas/transformed'
    files = os.listdir(path)
    print(files)

    if files:
        count_list = [int(re.search(r'transformed_(\d+)\.csv',f).group(1)) for f in files]
        counter = max(count_list)+1
        df_path = f'./datas/raw/raw_data_{counter}.csv'

    else:
        df_path = './datas/raw/raw_data_1.csv'
        counter = 1

    return counter, df_path

def transform():
    print('load data')
    
    counter, df_path = load_data()
    print(df_path)
    df = pd.read_csv(df_path)
    print('data loaded')
    print('transform data')
    df.replace(encoder, inplace=True)
    print('save data')
    df.to_csv(f'./datas/transformed/transformed_{counter}.csv',index=False)
    print('data saved')