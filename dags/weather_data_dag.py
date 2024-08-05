from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import requests
import pandas as pd
import json
import os

API_KEY = os.getenv('API_KEY')
CITY = 'London'
API_URL = f'http://api.openweathermap.org/data/2.5/weather?q={CITY}&appid={API_KEY}'
RAW_DATA_PATH = '/tmp/weather_data.json'
PROCESSED_DATA_PATH = '/tmp/processed_weather_data.csv'
PARQUET_DATA_PATH = '/tmp/weather.parquet'


def fetch_weather_data():    
    response = requests.get(API_URL)
    data = response.json()
    directory = os.path.dirname(RAW_DATA_PATH)

    if not os.path.exists(directory):
        os.makedirs(directory)

    with open(RAW_DATA_PATH, 'w') as f:
        json.dump(data, f)


def process_weather_data():
    with open(RAW_DATA_PATH, 'r') as f:
        data = json.load(f)

    main_data = data['main']
    weather_data = {
        'timestamp': datetime.now(),
        'temperature_k': main_data['temp'],
        'pressure': main_data['pressure'],
        'humidity': main_data['humidity'],
        'temperature_c': main_data['temp'] - 273.15
    }
    
    df = pd.DataFrame([weather_data])
    df.to_csv(PROCESSED_DATA_PATH, index=False)

def save_to_parquet():
    df_new = pd.read_csv(PROCESSED_DATA_PATH)
    
    if os.path.exists(PARQUET_DATA_PATH):
        df_existing = pd.read_parquet(PARQUET_DATA_PATH)
        
        df_combined = pd.concat([df_existing, df_new], ignore_index=True)
    else:
        df_combined = df_new
    
    df_combined.to_parquet(PARQUET_DATA_PATH, index=False)

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'retries': 1,
}

with DAG(
    'weather_data_pipeline',
    default_args=default_args,
    description='A simple weather data pipeline',
    schedule_interval='0 0 * * *',
    start_date=datetime(2024, 8, 1),
    catchup=False
) as dag:
    
    fetch_data = PythonOperator(
        task_id='fetch_weather_data',
        python_callable=fetch_weather_data,
    )

    process_data = PythonOperator(
        task_id='process_weather_data',
        python_callable=process_weather_data,
    )

    save_data = PythonOperator(
        task_id='save_to_parquet',
        python_callable=save_to_parquet,
    )

    fetch_data >> process_data >> save_data
