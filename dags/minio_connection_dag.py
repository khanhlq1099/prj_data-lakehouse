from datetime import datetime, timedelta

from airflow import DAG
from airflow.providers.amazon.aws.sensors.s3 import S3KeySensor


default_args = {
    'owner': 'jqk',
}

with DAG(
    dag_id='minio_connection_dag',
    start_date=datetime(2024,1,1),
    schedule_interval='@once',
    default_args=default_args,
    catchup=False
) as dag:
    task = S3KeySensor(
        task_id='sensor_minio_s3',
        bucket_name='stock',
        bucket_key='bronze/stock_price_data/2024/2024_07/2024_07_01.csv',
        aws_conn_id='minio-conn',
    )
    task