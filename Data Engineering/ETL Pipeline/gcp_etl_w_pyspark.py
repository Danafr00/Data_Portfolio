import time
from datetime import datetime
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
    try:
        real_estate_new = pd.DataFrame(columns=['Address', 'Beds', 'Baths', 'Area', 'Price'])
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

                result = soup.find_all('li',
                                       {
                                           'class': 'Grid__CellBox-sc-144isrp-0 SearchResultsList__WideCell-b7y9ki-2 jiZmPM'})

                result_update = [k for k in result if k.has_attr('data-testid')]

                for result in result_update:

                    try:
                        address.append(result.find('div', {'data-testid': 'property-address'}).get_text())
                    except:
                        address.append('n/a')

                    try:
                        beds.append(result.find('div', {'data-testid': 'property-beds'}).get_text())
                    except:
                        beds.append('n/a')

                    try:
                        baths.append(result.find('div', {'data-testid': 'property-baths'}).get_text())
                    except:
                        baths.append('n/a')

                    try:
                        areas.append(result.find('div', {'data-testid': 'property-floorSpace'}).get_text())
                    except:
                        areas.append('n/a')

                    try:
                        prices.append(result.find('div', {'data-testid': 'property-price'}).get_text())
                    except:
                        prices.append('n/a')

                for j in range(len(address)):
                    real_estate_new = real_estate_new.append(
                        {'Address': address[j],
                         'Beds': beds[j],
                         'Baths': baths[j],
                         'Area': areas[j],
                         'Price': prices[j]},
                        ignore_index=True)
        df_dict = real_estate_new.to_dict('dict')
        return df_dict

    except Exception as e:
        print("Data extract error: " + str(e))


# Define DAG function
@task()
def transform(df_dict):
    try:
        df = pd.DataFrame.from_dict(df_dict)
        df['Beds'] = df['Beds'].apply(lambda x: x.strip('bd'))
        df['Baths'] = df['Baths'].apply(lambda x: x.strip('ba'))
        df['Price'] = df['Price'].apply(lambda x: x.strip('$'))
        df['Price'] = df['Price'].apply(lambda x: x.replace(",", ""))
        df['Price'] = df['Price'].apply(lambda x: x.replace("+", ""))
        df['Area'] = df['Area'].apply(lambda x: x.replace(" sqft", ""))

        df['Street'] = df['Address'].apply(lambda x: x.split(',')[0])
        df['District'] = df['Address'].apply(lambda x: x.split(',')[1])
        df['City'] = df['Address'].apply(lambda x: x.split(',')[2].split(' ')[1])
        df['Zip_Code'] = df['Address'].apply(lambda x: x.split(',')[2].split(' ')[2])

        df['Area'] = df['Area'].str.replace(',', '')
        df['Area'] = df['Area'].str.split(" ", n=1, expand=True)[0]

        df = df.where(df.Area != r'n/a')
        df = df.where(df.Baths != r'n/')
        df = df.where(df.Price != r'')
        df = df.drop_duplicates()
        df = df.dropna()

        df['Area'] = df['Area'].astype('int')
        df['Baths'] = df['Baths'].astype('int')
        df['Price'] = df['Price'].astype('float64')

        df_dict_clean = df.to_dict('dict')
        return df_dict_clean

    except Exception as e:
        print("Data transform error: " + str(e))


# Define DAG function
@task()
def load(df_dict_clean):
    try:
        credentials = service_account.Credentials.from_service_account_file(r"/opt/airflow/dags/latihan-345909-d057684ecb42.json")
        project_id = 'latihan-345909'
        table_id = 'latihan-345909.real_estate.test_1'
        client = bigquery.Client(credentials=credentials, project=project_id)

        sql = """
       SELECT *
       FROM real_estate.test_1
       """

        df = pd.DataFrame.from_dict(df_dict_clean)
        bq_df = client.query(sql).to_dataframe()

        changes = df[~df.apply(tuple, 1).isin(bq_df.apply(tuple, 1))]
        job = client.load_table_from_dataframe(changes, table_id)
        job.result()
        print("There are {0} rows added/changed".format(len(changes)))

    except Exception as e:
        print("Data load error: " + str(e))


# Declare Dag
with DAG(dag_id='gcp_etl_w_pyspark',
         schedule_interval="0 0 * * *",
         start_date=datetime(2022, 10, 9),
         catchup=False,
         tags=['etl_gcp'])\
        as dag:
    src_data = extract(3)
    transform_src_data = transform(src_data)
    load_data = load(transform_src_data)

    src_data >> transform_src_data >> load_data


