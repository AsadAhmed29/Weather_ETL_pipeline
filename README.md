# ETL Weather Pipeline

This project implements an ETL (Extract, Transform, Load) pipeline that retrieves weather data from the Open Meteo API, processes it, and stores it in a PostgreSQL database running in a Docker container.

## Features

- **Extraction**: Fetches current weather data based on latitude and longitude using HTTP requests.
- **Transformation**: Parses and structures the weather data for storage.
- **Loading**: Inserts the transformed data into a PostgreSQL database.

## Technologies Used

- **Apache Airflow**: For orchestrating the ETL workflow.
- **PostgreSQL**: As the database for storing weather data.
- **Docker**: To containerize PostgreSQL.
- **Python**: For writing the ETL logic.

## Setup Instructions

1. Clone the repository.
2. Set up PostgreSQL in a Docker container.
3. Configure Airflow using Astro.
4. Run the DAG in Airflow to execute the ETL process.
