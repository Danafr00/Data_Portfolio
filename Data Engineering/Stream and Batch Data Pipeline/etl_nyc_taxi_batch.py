import time
from datetime import datetime
import pytz
from airflow.decorators import dag, task
from airflow.utils.task_group import TaskGroup
from airflow.models import DAG
import pandas as pd
import pandas_gbq
import glob
from bs4 import BeautifulSoup
import requests
import numpy as np
import urllib.parse
from google.cloud import bigquery
from google.oauth2 import service_account


# Define DAG function
@task()
def extract_transform():
    # Get CSV files list from a folder
    path = '/home/dana123/python_code/nyc_taxi_project/kafka_nyc_taxi_data'
    csv_files = glob.glob(path + "/*.json")

    # Read each CSV file into DataFrame
    # This creates a list of dataframes
    df_list = (pd.read_json(file, lines=True) for file in csv_files)

    # Concatenate all DataFrames
    big_df   = pd.concat(df_list, ignore_index=True)
    
    big_df["data_date"] = datetime.now(pytz.timezone("Asia/Jakarta")).date().strftime('%Y-%m-%d')
    big_df.insert(0, 'id', range(1, 1 + len(big_df)))
    big_df['id'] = big_df['id'].astype('str')
    big_df['uid'] = big_df['data_date'] + '_' + big_df['id'].astype(str)

    big_df['data_date']= big_df['data_date'].str.strip()
    big_df['uid']= big_df['uid'].str.strip()

    df_dict = big_df.to_dict('dict')
    return df_dict

# Define DAG function
@task()
def load(df_dict):
    credentials = service_account.Credentials.from_service_account_file(r"/home/dana123/airflow/latihan-345909-89e4eb39e2b1.json")
    project_id = 'latihan-345909'
    table_id = 'latihan-345909.nyc_taxi.yellow_taxi_data_stream'
    client = bigquery.Client(credentials=credentials, project=project_id)

    df = pd.DataFrame.from_dict(df_dict)
    df['tpep_pickup_datetime'] = df['tpep_pickup_datetime'].apply(pd.to_datetime)
    df['tpep_dropoff_datetime'] = df['tpep_dropoff_datetime'].apply(pd.to_datetime)
    df['data_date'] = df['data_date'].apply(pd.to_datetime)

    job = client.load_table_from_dataframe(df, table_id)
    job.result()
    print("There are {0} rows added/changed".format(len(df)))


# Declare Dag
with DAG(dag_id='etl_nyc_taxi',
         schedule_interval="0 0 * * *",
         start_date=datetime(2023, 11, 25),
         catchup=False,
         tags=['etl_nyc_taxi'])\
        as dag:
    source_transform_data = extract_transform()
    load_data = load(source_transform_data)

    source_transform_data >> load_data


