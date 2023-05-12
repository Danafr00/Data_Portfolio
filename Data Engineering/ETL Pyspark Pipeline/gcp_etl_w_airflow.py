import time
from datetime import datetime
import pytz
from airflow.decorators import dag, task
from airflow.utils.task_group import TaskGroup
from airflow.models import DAG
import pandas as pd
import pandas_gbq
from bs4 import BeautifulSoup
import requests
import numpy as np
import urllib.parse
from google.cloud import bigquery
from google.oauth2 import service_account


# Define DAG function
@task()
def extract(pages):
    headers = ({
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/104.0.5112.81 Safari/537.36 Edg/104.0.1293.54'})

    address = []
    beds = []
    baths = []
    areas = []
    prices = []

    web = ['https://www.trulia.com/NY/New_York/',
            'https://www.trulia.com/CA/Los_Angeles/',
            'https://www.trulia.com/IL/Chicago/',
            'https://www.trulia.com/AZ/Phoenix/',
            'https://www.trulia.com/NV/Las_Vegas/']

    for h in web:
        for i in range(1, pages + 1):
            website = requests.get(h + str(i) + '_p/', headers=headers)

            soup = BeautifulSoup(website.content, 'html.parser')

            result = soup.find_all('div',
                            {'data-testid': 'property-card-details'})

            result_update = [k for k in result if k.has_attr('data-testid')]

            for result in result_update:

                try:
                    address.append(result.find('div', {'data-testid': 'property-address'}).get_text())
                except:
                    address.append("n/a")

                try:
                    beds.append(result.find('div', {'data-testid': 'property-beds'}).get_text())
                except:
                    beds.append("n/a")

                try:
                    baths.append(result.find('div', {'data-testid': 'property-baths'}).get_text())
                except:
                    baths.append("n/a")

                try:
                    areas.append(result.find('div', {'data-testid': 'property-floorSpace'}).get_text())
                except:
                    areas.append("n/a")

                try:
                    prices.append(result.find('div', {'data-testid': 'property-price'}).get_text())
                except:
                    prices.append("n/a")

    real_estate_new = pd.DataFrame(list(zip(address, beds, baths, areas, prices)), columns=['Address', 'Beds', 'Baths', 'Area', 'Price'])
    df_dict = real_estate_new.to_dict('dict')
    return df_dict

# Define DAG function
@task()
def transform(df_dict):
    df = pd.DataFrame.from_dict(df_dict)
    df['Beds'] = df['Beds'].str.lower()
    df['Beds'] = df['Beds'].apply(lambda x: x.strip('bd'))
    df['Beds'] = df['Beds'].str.strip()
    df['Baths'] = df['Baths'].apply(lambda x: x.strip('ba'))
    df['Baths'] = df['Baths'].str.strip()
    df['Price'] = df['Price'].apply(lambda x: x.strip('$'))
    df['Price'] = df['Price'].apply(lambda x: x.replace(",", ""))
    df['Price'] = df['Price'].apply(lambda x: x.replace("+", ""))
    df['Price'] = df['Price'].apply(lambda x: x.replace(".0", ""))
    df['Price'] = df['Price'].str.strip()
    df['Area'] = df['Area'].apply(lambda x: x.replace(" sqft", ""))
    df['Area'] = df['Area'].str.strip()
    df['Address']= df['Address'].str.strip()
    df['Street'] = df['Address'].apply(lambda x: x.split(',')[0])
    df['District'] = df['Address'].apply(lambda x: x.split(',')[1])
    df['City'] = df['Address'].apply(lambda x: x.split(',')[2].split(' ')[1])
    df['Zip_Code'] = df['Address'].apply(lambda x: x.split(',')[2].split(' ')[2])

    df['Area'] = df['Area'].str.replace(',', '')
    df['Area'] = df['Area'].str.split(" ", n=1, expand=True)[0]

    df = df.where(df.Area != r'n/a')
    df = df.where(df.Area != r'nan')
    df = df.where(df.Area != r'')
    df = df.where(df.Baths != r'n/')
    df = df.where(df.Baths != r'')
    df = df.where(df.Beds != r'studio')
    df = df.where(df.Beds != r'')
    df = df.where(df.Price != r'')
    df = df.drop_duplicates()
    df = df.dropna()

    df['Area'] = df['Area'].astype('int64')
    df['Beds'] = df['Beds'].astype('int64')
    df['Baths'] = df['Baths'].astype('int64')
    df['Price'] = df['Price'].astype('int64')
    df['data_date'] = datetime.now(pytz.timezone("Asia/Jakarta")).date().strftime('%Y-%m-%d')
    df.insert(0, 'id', range(1, 1 + len(df)))
    df['id'] = df['id'].astype('str')
    df['uid'] = df['data_date'] + '_' + df['id'].astype(str)

    df['data_date']= df['data_date'].str.strip()
    df['uid']= df['uid'].str.strip()

    df_dict_clean = df.to_dict('dict')
    return df_dict_clean

# Define DAG function
@task()
def load(df_dict_clean):
    credentials = service_account.Credentials.from_service_account_file(r"your_path.json")
    project_id = 'latihan-345909'
    table_id = 'latihan-345909.real_estate.trulia'
    client = bigquery.Client(credentials=credentials, project=project_id)

    sql = """
    SELECT *
    FROM latihan-345909.real_estate.trulia
    """

    df = pd.DataFrame.from_dict(df_dict_clean)
    df['data_date'] = df['data_date'].apply(pd.to_datetime)
    bq_df = client.query(sql).to_dataframe()

    changes = df[~df.apply(tuple, 1).isin(bq_df.apply(tuple, 1))]
    job = client.load_table_from_dataframe(changes, table_id)
    job.result()
    print("There are {0} rows added/changed".format(len(changes)))


# Declare Dag
with DAG(dag_id='etl_gcp',
         schedule_interval="0 0 * * *",
         start_date=datetime(2023, 5, 9),
         catchup=False,
         tags=['etl_gcp'])\
        as dag:
    src_data = extract(7)
    transform_src_data = transform(src_data)
    load_data = load(transform_src_data)

    src_data >> transform_src_data >> load_data


