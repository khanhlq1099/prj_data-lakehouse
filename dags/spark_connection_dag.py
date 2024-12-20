from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from datetime import datetime


spark_conf = {
    "spark.hadoop.fs.s3a.impl": "org.apache.hadoop.fs.s3a.S3AFileSystem",
    "spark.hadoop.fs.s3a.aws.credentials.provider": "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider",
    "spark.sql.extensions": "io.delta.sql.DeltaSparkSessionExtension",
    "spark.sql.catalog.spark_catalog": "org.apache.spark.sql.delta.catalog.DeltaCatalog",
    "spark.delta.logStore.class":"org.apache.spark.sql.delta.storage.S3SingleDriverLogStore",
    "spark.hadoop.fs.s3a.endpoint": "http://localhost:9000",
    "spark.hadoop.fs.s3a.access.key": "xdHe7t4BYpfcZPWal8ho",
    "spark.hadoop.fs.s3a.secret.key": "zrSbj3ShgJqQHmLMAAse86VahptuzNex44BHDH4g",  
    "spark.hadoop.fs.s3a.path.style.access": "true",
    "spark.hadoop.fs.s3a.connection.ssl.enabled": "false",
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
    application='spark/app/silver/incremental_load.py',
    jars='spark/resources/jars/*',
    # conf=spark_conf,
    verbose=False,
    dag=dag
)

spark_job