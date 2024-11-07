# %% Import and default arguments
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.http.sensors.http import HttpSensor
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.utils.dates import days_ago
import requests
import json

# List of cities to collect weather data for
cities = ["Bangkok", "Tokyo", "New York", "London", "Paris"]

# Define default arguments for the DAG
default_args = {
    "owner": "airflow",
    "start_date": days_ago(1),
    "retries": 1,
}

# %% DAG Definition
with DAG(
    "weather_data_etl_pipeline",
    default_args=default_args,
    schedule_interval="@daily",
    catchup=False,
) as dag:

    # 1. Check if the API is available
    check_api = HttpSensor(
        task_id="check_weather_api",
        http_conn_id="weather_api",
        endpoint="current.json?key=a7b78630443c40f8b1b135649240311&q=Bangkok",  # Test one city
        poke_interval=5,
        timeout=20,
    )

    # 2. Extract weather data for multiple cities
    def extract_weather_data_for_cities():
        results = []
        for city in cities:
            try:
                # Replace 'YOUR_API_KEY' with your actual API key
                response = requests.get(
                    f"http://api.weatherapi.com/v1/current.json?key=a7b78630443c40f8b1b135649240311&q={city}"
                )
                response.raise_for_status()  # Raise an error for unsuccessful status codes
                data = response.json()
                results.append(data)
            except requests.exceptions.RequestException as e:
                print(f"Error fetching data for city {city}: {e}")
        return results

    extract_weather_data = PythonOperator(
        task_id="extract_weather_data",
        python_callable=extract_weather_data_for_cities,
    )

    # %% 3. Transform the data: Calculate daily averages, handle missing data, and convert units
    def transform_weather_data(ti):
        raw_data = ti.xcom_pull(task_ids="extract_weather_data")

        if not raw_data:  # Check if raw_data is empty or None
            raise ValueError("No data found in XCom for 'extract_weather_data' task.")

        # Aggregate and transform data for each city
        transformed_data = []
        for data in raw_data:
            transformed_city_data = {
                "city": data["location"]["name"],
                "temperature": data["current"]["temp_c"],
                "humidity": data["current"]["humidity"],
                "weather": data["current"]["condition"]["text"],
                "timestamp": data["current"]["last_updated_epoch"],
            }
            transformed_data.append(transformed_city_data)

        # Calculate daily averages for reporting
        daily_averages = {
            "temperature": sum(item["temperature"] for item in transformed_data) / len(transformed_data),
            "humidity": sum(item["humidity"] for item in transformed_data) / len(transformed_data),
        }

        # Push the transformed data and daily summary to XCom
        ti.xcom_push(key="transformed_data", value=transformed_data)
        ti.xcom_push(key="daily_averages", value=daily_averages)

    transform_data = PythonOperator(
        task_id="transform_data",
        python_callable=transform_weather_data,
    )

    # %% 4. Load data into PostgreSQL
    load_data = PostgresOperator(
        task_id="load_data_to_postgresql",
        postgres_conn_id="postgres_default",
        sql="""
        CREATE TABLE IF NOT EXISTS weather_data_analyze (
            id SERIAL PRIMARY KEY,
            city VARCHAR(50),
            temperature FLOAT NOT NULL,
            humidity INT NOT NULL,
            weather VARCHAR(255) NOT NULL,
            timestamp TIMESTAMP NOT NULL
        );

        {% set records = ti.xcom_pull(task_ids='transform_data', key='transformed_data') %}
        {% if records %}
        INSERT INTO weather_data_analyze (city, temperature, humidity, weather, timestamp)
        VALUES 
        {% for record in records %}
        ('{{ record.city }}', {{ record.temperature }}, {{ record.humidity }}, '{{ record.weather }}', TO_TIMESTAMP({{ record.timestamp }}))
        {% if not loop.last %}, {% endif %}
        {% endfor %}
        ;
        {% else %}
        -- No data to insert
        {% endif %}
    """,
    )

    # %% Generate a Summary Report
    def generate_report(ti):
        daily_averages = ti.xcom_pull(task_ids="transform_data", key="daily_averages")
        report = f"Weather Data Summary:\nDaily Average Temperature: {daily_averages['temperature']} °C\nDaily Average Humidity: {daily_averages['humidity']}%"
        print(report)
        return report

    report_task = PythonOperator(
        task_id="generate_report",
        python_callable=generate_report,
    )

    # Define task dependencies
    check_api >> extract_weather_data >> transform_data >> load_data >> report_task
