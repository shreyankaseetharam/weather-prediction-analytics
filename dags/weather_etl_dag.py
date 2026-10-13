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
from datetime import date, datetime, timedelta

from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook


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

        @task
    def load_weather_to_snowflake(payload: dict):
        """Load one city's weather data into Snowflake idempotently."""

        hook = SnowflakeHook(
            snowflake_conn_id="snowflake_weather"
        )

        daily = payload["daily"]
        city = payload["city"]

        rows = []

        for index, weather_date_text in enumerate(daily["time"]):
            weather_date = datetime.strptime(
                weather_date_text,
                "%Y-%m-%d",
            ).date()

            rows.append(
                (
                    city,
                    payload["latitude"],
                    payload["longitude"],
                    weather_date,
                    daily["temperature_2m_max"][index],
                    daily["temperature_2m_min"][index],
                    daily["temperature_2m_mean"][index],
                    daily["apparent_temperature_mean"][index],
                    daily["precipitation_sum"][index],
                    daily["rain_sum"][index],
                    daily["precipitation_hours"][index],
                    daily["relative_humidity_2m_mean"][index],
                    daily["wind_speed_10m_max"][index],
                    daily["weather_code"][index],
                    weather_date > date.today(),
                )
            )

        connection = hook.get_conn()
        cursor = connection.cursor()

        try:
            cursor.execute("BEGIN")

            cursor.execute(
                """
                DELETE FROM WEATHER_DB.RAW.WEATHER_DAILY
                WHERE CITY = %s
                  AND WEATHER_DATE BETWEEN %s AND %s
                """,
                (
                    city,
                    rows[0][3],
                    rows[-1][3],
                ),
            )

            cursor.executemany(
                """
                INSERT INTO WEATHER_DB.RAW.WEATHER_DAILY (
                    CITY,
                    LATITUDE,
                    LONGITUDE,
                    WEATHER_DATE,
                    TEMP_MAX_C,
                    TEMP_MIN_C,
                    TEMP_MEAN_C,
                    APPARENT_TEMP_MEAN_C,
                    PRECIPITATION_MM,
                    RAIN_MM,
                    PRECIPITATION_HOURS,
                    HUMIDITY_MEAN_PCT,
                    WIND_SPEED_MAX_KMH,
                    WEATHER_CODE,
                    IS_FORECAST
                )
                VALUES (
                    %s, %s, %s, %s, %s,
                    %s, %s, %s, %s, %s,
                    %s, %s, %s, %s, %s
                )
                """,
                rows,
            )

            connection.commit()

            print(
                f"Loaded {len(rows)} rows for {city} "
                "into WEATHER_DB.RAW.WEATHER_DAILY."
            )

        except Exception as error:
            connection.rollback()
            raise RuntimeError(
                f"Snowflake load failed for {city}; "
                "transaction rolled back."
            ) from error

        finally:
            cursor.close()
            connection.close()

    cities = get_cities()

    extracted_weather = extract_weather.expand(city=cities)

    validate_weather_data.expand(payload=extracted_weather)
    load_weather_to_snowflake.expand(payload=extracted_weather)