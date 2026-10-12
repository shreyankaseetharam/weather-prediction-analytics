# weather-prediction-analytics

# Weather Prediction Analytics

This project builds an automated weather analytics pipeline using the Open-Meteo API, Apache Airflow, Snowflake, dbt, and a BI dashboard.

The project compares weather conditions in Seattle and Phoenix.

## Pipeline

Open-Meteo API  
→ Airflow ETL  
→ Snowflake RAW schema  
→ dbt transformations  
→ Snowflake ANALYTICS schema  
→ BI dashboard

## Planned Metrics

- Average temperature
- Feels-like temperature
- Humidity
- Rolling rainfall
- Rainy-day count
- Temperature range
- Extreme heat days
- Forecast and observed weather comparison

## Project Structure

- `dags/`: Airflow DAG files
- `dbt/`: dbt models, tests, and snapshots
- `docs/`: screenshots and documentation
- `docker-compose.yaml`: local Airflow setup
