from airflow import DAG
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.decorators import task
from datetime import datetime
import pandas as pd
import openmeteo_requests
import requests_cache
from retry_requests import retry

# Coordinates
LATITUDE = '-6.2016026'
LONGITUDE = '106.8162208'
POSTGRES_CONN_ID = 'postgres_default'

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2024, 10, 1)
}

with DAG(
    dag_id='pollution_etl_pipeline',
    default_args=default_args,
    schedule_interval='@daily',
    catchup=False
) as dag:
    
    @task()
    def extract_pollution_data():
        """Extract historical air pollution data from Open-Meteo API."""
        # Setup the Open-Meteo API client with cache and retry on error
        cache_session = requests_cache.CachedSession('.cache', expire_after=3600)
        retry_session = retry(cache_session, retries=5, backoff_factor=0.2)
        openmeteo = openmeteo_requests.Client(session=retry_session)

        url = "https://air-quality-api.open-meteo.com/v1/air-quality"
        params = {
            "latitude": LATITUDE,
            "longitude": LONGITUDE,
            "hourly": ["pm10", "pm2_5"],
            "start_date": "2024-10-01",
            "end_date": "2024-11-20"
        }
        
        responses = openmeteo.weather_api(url, params=params)
        response = responses[0]
        
        # Process hourly data
        hourly = response.Hourly()
        hourly_pm10 = hourly.Variables(0).ValuesAsNumpy()
        hourly_pm2_5 = hourly.Variables(1).ValuesAsNumpy()

        # Verify arrays have same length before creating date range
        if len(hourly_pm10) != len(hourly_pm2_5):
            raise ValueError("PM10 and PM2.5 arrays must have the same length")
        
        # Create date range using the length of the PM data
        date_range = pd.date_range(
            start="2024-10-01",
            periods=len(hourly_pm10),  # Use the actual length of data
            freq="H"
        )
        
        hourly_data = {
            "date": [d.strftime('%Y-%m-%d %H:%M:%S') for d in date_range],  # Convert to string format
            "pm10": hourly_pm10.tolist(),
            "pm2_5": hourly_pm2_5.tolist()
        }
        
        # Verify dictionary values have same length
        lengths = {key: len(value) for key, value in hourly_data.items()}
        if len(set(lengths.values())) != 1:
            raise ValueError(f"Inconsistent array lengths: {lengths}")
            
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
                pm2_5 FLOAT,
                latitude FLOAT,
                longitude FLOAT,
                timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );
            """)

            # Insert transformed data into the table
            for record in transformed_data:
                cursor.execute("""
                INSERT INTO pollution_data 
                    (date, pm10, pm2_5, latitude, longitude)
                VALUES 
                    (%s, %s, %s, %s, %s)
                """, (
                    record['date'],
                    record['pm10'],
                    record['pm2_5'],
                    float(LATITUDE),
                    float(LONGITUDE)
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