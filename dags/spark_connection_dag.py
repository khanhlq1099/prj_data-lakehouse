from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from datetime import datetime
import socket

MINIO_IP_ADDRESS = socket.gethostbyname("minio")

spark_conf = {
    "spark.hadoop.fs.s3a.impl": "org.apache.hadoop.fs.s3a.S3AFileSystem",
    "spark.hadoop.fs.s3a.endpoint": f"http://{MINIO_IP_ADDRESS}:9000",
    "spark.hadoop.fs.s3a.access.key": "xdHe7t4BYpfcZPWal8ho",
    "spark.hadoop.fs.s3a.secret.key": "zrSbj3ShgJqQHmLMAAse86VahptuzNex44BHDH4g",  
    "spark.hadoop.fs.s3a.path.style.access": "true",
    "spark.hadoop.fs.s3a.connection.ssl.enabled": "false",
    "spark.sql.extensions":"io.delta.sql.DeltaSparkSessionExtension",
    # "spark.sql.catalog.spark_catalog":"org.apache.spark.sql.delta.catalog.DeltaCatalog",
    "spark.hadoop.fs.s3a.aws.credentials.provider": "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider",
    "spark.driver.memory": "1g",  # Adjust as per your requirement
    "spark.executor.memory": "1g",  # Adjust as per your requirement
    "spark.executor.instances": "1",  # Adjust as per your requirement
}
dag = DAG(
    dag_id="Spark_connection_dag",
    default_args={
        "owner":'JQK',
        "start_date":datetime(2024,1,1)
    },
    schedule_interval="@once"
)

spark_job= SparkSubmitOperator(
    task_id='submit_job',
    conn_id='spark-conn',
    application='spark/app/spark_test.py',
    packages='org.apache.hadoop:hadoop-aws:3.3.4,io.delta:delta-core_2.12:2.0.0,org.apache.spark:spark-hadoop-cloud_2.13:3.3.4',
    conf=spark_conf,
    verbose = 1,
    dag=dag
)

spark_job