from airflow import DAG
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.decorators import task
from datetime import datetime, timedelta
import pandas as pd
import requests

# Coordinates
LATITUDE = '-6.2016026'
LONGITUDE = '106.8162208'
POSTGRES_CONN_ID = 'postgres_default'
API_KEY = 'your_openweather_api_key'

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2024, 10, 1)
}

with DAG(
    dag_id='pollution_etl_pipeline_openweather',
    default_args=default_args,
    schedule_interval='@daily',
    catchup=False
) as dag:
    
    @task()
    def extract_pollution_data():
        """Extract historical air pollution data from OpenWeather API."""
        url = "http://api.openweathermap.org/data/2.5/air_pollution/history"
        start_date = datetime(2024, 10, 1)
        end_date = datetime(2024, 11, 20)
        start_timestamp = int(start_date.timestamp())
        end_timestamp = int(end_date.timestamp())
        
        params = {
            "lat": LATITUDE,
            "lon": LONGITUDE,
            "start": start_timestamp,
            "end": end_timestamp,
            "appid": API_KEY
        }
        
        response = requests.get(url, params=params)
        response.raise_for_status()
        data = response.json()
        
        hourly_data = {
            "date": [],
            "pm10": [],
            "pm2_5": []
        }
        
        for entry in data['list']:
            timestamp = entry['dt']
            date = datetime.utcfromtimestamp(timestamp).strftime('%Y-%m-%d %H:%M:%S')
            pm10 = entry['components']['pm10']
            pm2_5 = entry['components']['pm2_5']
            
            hourly_data['date'].append(date)
            hourly_data['pm10'].append(pm10)
            hourly_data['pm2_5'].append(pm2_5)
        
        return hourly_data

    @task()
    def transform_pollution_data(hourly_data):
        """Transform the extracted pollution data."""
        # Convert to DataFrame and then to records for database insertion
        df = pd.DataFrame(data=hourly_data)
        df['date'] = pd.to_datetime(df['date'])
        # Convert to dict with orient='records' and convert datetime to string
        records = df.to_dict('records')
        for record in records:
            record['date'] = record['date'].strftime('%Y-%m-%d %H:%M:%S')
        return records

    @task()
    def load_pollution_data(transformed_data):
        """Load transformed pollution data into PostgreSQL."""
        pg_hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
        conn = pg_hook.get_conn()
        cursor = conn.cursor()

        try:
            # Create table if it doesn't exist
            cursor.execute("""
            CREATE TABLE IF NOT EXISTS pollution_data (
                id SERIAL PRIMARY KEY,
                date TIMESTAMP,
                pm10 FLOAT,
                pm2_5 FLOAT
            );
            """)

            # Insert transformed data into the table
            for record in transformed_data:
                cursor.execute("""
                INSERT INTO pollution_data 
                    (date, pm10, pm2_5)
                VALUES 
                    (%s, %s, %s)
                """, (
                    record['date'],
                    record['pm10'],
                    record['pm2_5']
                ))

            conn.commit()
            
        except Exception as e:
            conn.rollback()
            raise e
        
        finally:
            cursor.close()
            conn.close()

    @task()
    def log_completion():
        """Log the completion of the ETL process."""
        print("Pollution ETL pipeline completed successfully")
        return True

    # Define the DAG workflow
    pollution_data = extract_pollution_data()
    transformed_data = transform_pollution_data(pollution_data)
    load_pollution_data(transformed_data)
    log_completion()
