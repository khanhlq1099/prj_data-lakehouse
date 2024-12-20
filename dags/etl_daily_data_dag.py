from airflow import DAG
from airflow.utils.dates import days_ago
from datetime import datetime
from airflow.operators.python import PythonOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.operators.dummy import DummyOperator
from airflow.operators.bash import BashOperator

spark_conf = {
    "spark.hadoop.fs.s3a.aws.credentials.provider": "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider",
    "spark.sql.extensions": "io.delta.sql.DeltaSparkSessionExtension",
    "spark.sql.catalog.spark_catalog": "org.apache.spark.sql.delta.catalog.DeltaCatalog",
    "spark.hadoop.fs.s3a.endpoint": f"http://minio:9000",
    "spark.hadoop.fs.s3a.access.key": "xdHe7t4BYpfcZPWal8ho",
    "spark.hadoop.fs.s3a.secret.key": "zrSbj3ShgJqQHmLMAAse86VahptuzNex44BHDH4g",  
    "spark.hadoop.fs.s3a.path.style.access": "true",
    "spark.hadoop.fs.s3a.connection.ssl.enabled": "false",
}

def extract_data():
    from src.modules.service import extract_stock_data
    from src.config.period import PERIOD_TYPE 
    # extract_stock_data(period_type=PERIOD_TYPE.TODAY,extract=datetime.today().date())
    extract_stock_data(period_type=PERIOD_TYPE.PERIOD,from_date=datetime(2024,10,4),to_date=datetime(2024,10,4))

default_args ={
    'owner': 'Khanh - Lam Quoc'
}

with DAG(
    dag_id='ETL_Stock_Price_Data',
    # schedule_interval='0 9,11,13,15 * * *',
    schedule_interval=None,
    default_args=default_args,
    # description='Daily Extract Stock Data',
    start_date=days_ago(1),
    tags=["example"],
    catchup=False,
) as dag:
    start_task = DummyOperator(task_id='start_task',dag=dag)
    extract = PythonOperator(
        task_id = 'bronze',
        python_callable=extract_data,
        dag=dag
    )
    sleep_1= BashOperator(task_id = "sleep_1",bash_command='sleep 3')

    # transform_silver = SparkSubmitOperator(
    #     task_id='bronze_to_silver',
    #     conn_id='spark-conn',
    #     application='spark/app/silver/incremental_load.py',
    #     jars='spark/resources/jars/*',
    #     conf=spark_conf,
    #     verbose=False,
    #     dag=dag
    # )

    end_task = DummyOperator(task_id = 'end_task',trigger_rule="all_success",dag=dag)
# start_task >> extract >> sleep_1 >> transform_silver >> end_task
    extract