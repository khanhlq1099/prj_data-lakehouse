# Setup

## Virtual Environment

`python -m venv venv`
`source venv/bin/activate`

## Airflow 

`curl -LfO 'https://airflow.apache.org/docs/apache-airflow/2.7.1/docker-compose.yaml'`
`mkdir -p ./dags ./logs ./plugins`
`echo -e "AIRFLOW_UID=$(id -u)" > .env`
`AIRFLOW_UID=50000`
`docker compose up airflow-init`

## Spark

## Build Airflow Image from Dockerfile
`docker build -t apache-airflow:customize .`
`docker-compose up -d --build`