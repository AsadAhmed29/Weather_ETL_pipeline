from airflow import DAG
from airflow.providers.http.hooks.http import HttpHook
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.decorators import task
from airflow.utils.dates import days_ago
import json
import requests

latitude = '45.4299'
longitude = '10.98444'

postgres_conn = 'postgres_default'
api_conn = 'open_meteo_connection'

default_args = {
    'owner' : 'Asad',
    'start_date' : days_ago(1),
    'email' : 'asad.nedian29@gmail.com',
    'email_on_success' : False,
    'email_on_failure' : True
}

with DAG(dag_id = 'etl_weather_pipeline', 
         default_args = default_args,
         schedule_interval = '@hourly',
         catchup = False
          ) as dags :

    @task()
    def extract_weather_data():
        """ Used to extract weather data from Open Meteo API """

        httphook = HttpHook(http_conn_id = api_conn , method = 'GET')
        #https://api.open-meteo.com/v1/forecast?latitude=45.4299&longitude=10.98444&current_weather=true
        endpoint = f'v1/forecast?latitude={latitude}&longitude={longitude}&current_weather=true'

        response = httphook.run(endpoint)
        
        if response.status_code == 200:
            return response.json()
        else  :
            raise Exception (f'failed to fetch weather data : {response.status_code}')

    @task()
    def transform_weather_data(json_weather_data):
        current_weather = json_weather_data['current_weather']
        transformed_weather_data = {
            'latitude' : latitude,
            'longitude' : longitude,
            'temperature' : current_weather['temperature'],
            'wind speed' : current_weather['windspeed'],
            'wind direction' : current_weather['winddirection']
        }

        return transformed_weather_data

    @task
    def load_transformed_data(transformed_weather_data):
        postgres_hook = PostgresHook(postgres_conn_id = postgres_conn)
        conn = postgres_hook.get_conn()
        cursor = conn.cursor()

        cursor.execute("""
        CREATE TABLE IF NOT EXISTS weather_data
        
        (Latitude FLOAT,
        Longitude FLOAT,
        Temperature FLOAT,
        Windspeed FLOAT,
        Winddirection FLOAT,
        timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
        """)

        cursor.execute("""
        INSERT INTO weather_data(Latitude, Longitude , Temperature , Windspeed , Winddirection )
        VALUES(%s,%s,%s,%s,%s)
        """,(
            transformed_weather_data['latitude'],
            transformed_weather_data['longitude'],
            transformed_weather_data['temperature'],
            transformed_weather_data['wind speed'],
            transformed_weather_data['wind direction']
        ))

        conn.commit()
        cursor.close()


## DAG Worflow- ETL Pipeline
    json_weather_data= extract_weather_data()
    transformed_data=transform_weather_data(json_weather_data)
    load_transformed_data(transformed_data)
