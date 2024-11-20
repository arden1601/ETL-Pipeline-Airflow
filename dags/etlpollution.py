from airflow import DAG
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.decorators import task
from datetime import datetime
import pandas as pd
import openmeteo_requests
import requests_cache
from retry_requests import retry
import requests
from datetime import datetime, timedelta
import pytz

# Coordinates
LATITUDE = '-6.2016026'
LONGITUDE = '106.8162208'
POSTGRES_CONN_ID = 'postgres_default'
WEATHERBIT_API_KEY = '58491fb1764d49c9ad020c7275ea18f5'

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2024, 10, 19)
}

with DAG(
    dag_id='pollution_etl_pipeline',
    default_args=default_args,
    schedule_interval='@daily',
    catchup=False
) as dag:
    
    @task()
    def extract_pollution_data():
        """Extract historical air pollution data from Open-Meteo and Weatherbit APIs."""
        # Setup the Open-Meteo API client with cache and retry on error
        cache_session = requests_cache.CachedSession('.cache', expire_after=3600)
        retry_session = retry(cache_session, retries=5, backoff_factor=0.2)
        openmeteo = openmeteo_requests.Client(session=retry_session)

        # Open-Meteo API call
        url = "https://air-quality-api.open-meteo.com/v1/air-quality"
        params = {
            "latitude": LATITUDE,
            "longitude": LONGITUDE,
            "hourly": ["pm10", "pm2_5"],
            "start_date": "2024-10-19",
            "end_date": "2024-11-18"
        }
        
        responses = openmeteo.weather_api(url, params=params)
        response = responses[0]
        
        # Process hourly data from Open-Meteo
        hourly = response.Hourly()
        hourly_pm10 = hourly.Variables(0).ValuesAsNumpy()
        hourly_pm2_5 = hourly.Variables(1).ValuesAsNumpy()

        date_range = pd.date_range(
            start="2024-10-19",
            periods=len(hourly_pm10),
            freq="H"
        )
        
        openmeteo_data = {
            "date": [d.strftime('%Y-%m-%d %H:%M:%S') for d in date_range],
            "pm10": hourly_pm10.tolist(),
            "pm2_5": hourly_pm2_5.tolist()
        }

        # Weatherbit API call
        weatherbit_url = "https://api.weatherbit.io/v2.0/history/airquality"
        start_date = datetime(2024, 10, 19)
        end_date = datetime(2024, 11, 19)
        
        params = {
            "lat": LATITUDE,
            "lon": LONGITUDE,
            "start_date": start_date.strftime("%Y-%m-%d"),
            "end_date": end_date.strftime("%Y-%m-%d"),
            "tz": "local",
            "key": WEATHERBIT_API_KEY
        }
        
        response = requests.get(weatherbit_url, params=params)
        response.raise_for_status()
        data = response.json()
        
        jakarta_tz = pytz.timezone('Asia/Jakarta')
        
        weatherbit_data = {
            "date": [],
            "co": [],
            "no2": []
        }
        
        for entry in data['data']:
            local_time = datetime.strptime(entry['timestamp_local'], '%Y-%m-%dT%H:%M:%S')
            local_time = jakarta_tz.localize(local_time)
            formatted_date = local_time.strftime('%Y-%m-%d %H:%M:%S')
            
            weatherbit_data['date'].append(formatted_date)
            weatherbit_data['co'].append(float(entry['co']))  # Convert to float
            weatherbit_data['no2'].append(float(entry['no2']))  # Convert to float
            
        return {'openmeteo': openmeteo_data, 'weatherbit': weatherbit_data}

    @task()
    def transform_pollution_data(data):
        """Transform the extracted pollution data from both sources."""
        # Transform Open-Meteo data
        df_openmeteo = pd.DataFrame(data['openmeteo'])
        df_openmeteo['date'] = pd.to_datetime(df_openmeteo['date'])
        
        # Transform Weatherbit data
        df_weatherbit = pd.DataFrame(data['weatherbit'])
        df_weatherbit['date'] = pd.to_datetime(df_weatherbit['date'])
        
        # Ensure numeric columns
        df_weatherbit['co'] = pd.to_numeric(df_weatherbit['co'], errors='coerce')
        df_weatherbit['no2'] = pd.to_numeric(df_weatherbit['no2'], errors='coerce')
        
        # Fill NaN values with 0 or appropriate default value
        df_weatherbit = df_weatherbit.fillna(0)
        
        # Merge the dataframes
        df_merged = pd.merge(df_openmeteo, df_weatherbit, on='date', how='outer')
        
        # Convert datetime to string in desired format
        df_merged['date'] = df_merged['date'].dt.strftime('%Y-%m-%d %H:%M:%S')
        
        return df_merged.to_dict('records')

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
                co FLOAT,
                no2 FLOAT
            );
            """)

            # Insert transformed data into the table
            for record in transformed_data:
                cursor.execute("""
                INSERT INTO pollution_data 
                    (date, pm10, pm2_5, co, no2)
                VALUES 
                    (%s, %s, %s, %s, %s)
                """, (
                    record['date'],
                    record['pm10'],
                    record['pm2_5'],
                    record['co'],
                    record['no2']
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