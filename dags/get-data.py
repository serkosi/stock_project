from airflow import DAG
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator
from airflow.operators.python import PythonOperator
from datetime import datetime
import requests
import os
import json

with open('../../secs/stock_project/apikey.json', 'r') as file:
    data = json.load(file)
apikey = data['apikey']

def fetch_data_from_api():
    response = requests.get(f'https://www.alphavantage.co/query?function=TIME_SERIES_DAILY&symbol=MBG.DEX&outputsize=compact&apikey={apikey}')
    data = response.json()
    return data

def transform_data(x):
    data_rows_init = []
    for i in range(len(x['Time Series (Daily)'])):
        data_rows_init.append(dict({'date':list(x['Time Series (Daily)'].keys())[i]}, **list(x['Time Series (Daily)'].values())[i]))
    data = []
    for item in data_rows_init:
        item['open'] = item.pop('1. open')
        item['high'] = item.pop('2. high')
        item['low'] = item.pop('3. low')
        item['close'] = item.pop('4. close')
        item['volume'] = item.pop('5. volume')
        data.append(item)
    return data

# Set the environment variable for Google Cloud credentials
os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = 'firstapp-a03fb-f6359c8ba41b.json'

with DAG('api_to_bigquery', start_date=datetime(2023, 1, 1), schedule_interval='@daily') as dag:
    
    fetch_data = PythonOperator(
        task_id='fetch_data',
        python_callable=fetch_data_from_api
    )
    
    transform_data = PythonOperator(
        task_id='transform_data',
        python_callable=transform_data,
        op_args=[fetch_data.output]
    )
    
    load_to_bigquery = BigQueryInsertJobOperator(
        task_id='load_to_bigquery',
        configuration={
            "query": {
                "query": """
                    INSERT INTO `firstapp-a03fb.XETRA.compact` 
                    (date, open, high, low, close, volume) 
                    VALUES (@date, @open, @high, @low, @close, @volume)
                """,
                "useLegacySql": False,
                "queryParameters": [
                    {
                        "name": "date",
                        "parameterType": {"type": "DATE"},
                        "parameterValue": {"value": "{{ task_instance.xcom_pull(task_ids='transform_data')[0]['date'] }}"}
                    },
                    {
                        "name": "open",
                        "parameterType": {"type": "FLOAT"},
                        "parameterValue": {"value": "{{ task_instance.xcom_pull(task_ids='transform_data')[0]['open'] }}"}
                    },
                    {
                        "name": "high",
                        "parameterType": {"type": "FLOAT"},
                        "parameterValue": {"value": "{{ task_instance.xcom_pull(task_ids='transform_data')[0]['high'] }}"}
                    },
                    {
                        "name": "low",
                        "parameterType": {"type": "FLOAT"},
                        "parameterValue": {"value": "{{ task_instance.xcom_pull(task_ids='transform_data')[0]['low'] }}"}
                    },
                    {
                        "name": "close",
                        "parameterType": {"type": "FLOAT"},
                        "parameterValue": {"value": "{{ task_instance.xcom_pull(task_ids='transform_data')[0]['close'] }}"}
                    },
                    {
                        "name": "volume",
                        "parameterType": {"type": "INT64"},
                        "parameterValue": {"value": "{{ task_instance.xcom_pull(task_ids='transform_data')[0]['volume'] }}"}
                    }
                ]
            }
        },
        gcp_conn_id='google_cloud_default'
    )
    
    fetch_data >> transform_data >> load_to_bigquery
