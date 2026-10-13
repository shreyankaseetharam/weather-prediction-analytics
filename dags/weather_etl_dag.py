"""
Weather Analytics ETL DAG

Stage 1:
- Reads cities and date settings from Airflow Variables
- Extracts daily weather data from Open-Meteo
- Validates the API response
"""

from datetime import datetime, timedelta

import requests

from airflow import DAG
from airflow.decorators import task
from airflow.models import Variable


API_URL = "https://api.open-meteo.com/v1/forecast"


with DAG(
    dag_id="weather_etl",
    description="Extract daily weather data for Seattle and Phoenix",
    start_date=datetime(2026, 10, 1),
    schedule="0 7 * * *",
    catchup=False,
    default_args={
        "owner": "shreyanka",
        "retries": 2,
        "retry_delay": timedelta(minutes=2),
    },
    tags=["weather", "open-meteo", "etl"],
) as dag:

    @task
    def get_cities():
        """Read city configuration from an Airflow Variable."""
        return Variable.get(
            "weather_cities",
            deserialize_json=True,
        )

    @task
    def extract_weather(city: dict):
        """Extract daily weather data for one city."""
        params = {
            "latitude": city["latitude"],
            "longitude": city["longitude"],
            "daily": ",".join(
                [
                    "temperature_2m_max",
                    "temperature_2m_min",
                    "temperature_2m_mean",
                    "apparent_temperature_mean",
                    "precipitation_sum",
                    "rain_sum",
                    "precipitation_hours",
                    "relative_humidity_2m_mean",
                    "wind_speed_10m_max",
                    "weather_code",
                ]
            ),
            "past_days": int(
                Variable.get("weather_past_days", default_var=60)
            ),
            "forecast_days": int(
                Variable.get("weather_forecast_days", default_var=7)
            ),
            "timezone": "auto",
        }

        response = requests.get(
            API_URL,
            params=params,
            timeout=30,
        )

        response.raise_for_status()

        response_data = response.json()

        return {
            "city": city["name"],
            "latitude": response_data["latitude"],
            "longitude": response_data["longitude"],
            "timezone": response_data["timezone"],
            "daily": response_data["daily"],
        }

    @task
    def validate_weather_data(payload: dict):
        """Check that the API returned the expected daily fields."""
        required_fields = [
            "time",
            "temperature_2m_max",
            "temperature_2m_min",
            "temperature_2m_mean",
            "apparent_temperature_mean",
            "precipitation_sum",
            "rain_sum",
            "precipitation_hours",
            "relative_humidity_2m_mean",
            "wind_speed_10m_max",
            "weather_code",
        ]

        daily_data = payload["daily"]

        missing_fields = [
            field for field in required_fields
            if field not in daily_data
        ]

        if missing_fields:
            raise ValueError(
                f"Missing fields for {payload['city']}: {missing_fields}"
            )

        row_count = len(daily_data["time"])

        print(
            f"Successfully extracted {row_count} daily rows "
            f"for {payload['city']}."
        )

        print(
            f"Date range: {daily_data['time'][0]} "
            f"to {daily_data['time'][-1]}"
        )

        return {
            "city": payload["city"],
            "row_count": row_count,
            "first_date": daily_data["time"][0],
            "last_date": daily_data["time"][-1],
        }

    cities = get_cities()

    extracted_weather = extract_weather.expand(city=cities)

    validate_weather_data.expand(payload=extracted_weather)