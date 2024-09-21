from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from datetime import datetime

minio_conf = {
    "spark.hadoop.fs.s3a.impl": "org.apache.hadoop.fs.s3a.S3AFileSystem",
    "spark.hadoop.fs.s3a.endpoint": "http://minio:9000",
    "spark.hadoop.fs.s3a.access.key": "a0S54PCmAg5BKGQMxhna",
    "spark.hadoop.fs.s3a.secret.key": "LyMqjfIAhYhurDWW3aCDg4fXUP7lL73fltXrbcqN",  
    "spark.hadoop.fs.s3a.path.style.access": "true",
    "spark.hadoop.fs.s3a.connection.ssl.enabled": "false",
    "spark.sql.extensions":"io.delta.sql.DeltaSparkSessionExtension",
    "spark.sql.catalog.spark_catalog":"org.apache.spark.sql.delta.catalog.DeltaCatalog",
    "spark.driver.memory": "1g",  # Adjust as per your requirement
    "spark.executor.memory": "1g",  # Adjust as per your requirement
    "spark.executor.instances": "1",  # Adjust as per your requirement
    # "spark.driver.port":"7077",
    # "spark.driver.host":"localhost",
    # "spark.driver.bindAddress":"0.0.0.0",
    # "spark.master": "spark://spark:7077",
    # "spark.sql.catalog.data.s3.endpoint": "http://minio:9000",
    # "spark.sql.catalog.data.warehouse":"s3a://stock/",
}
jar_home="/spark/resources"

app_jars=f'{jar_home}/jars/hadoop-cloud-storage-3.3.4.jar,{jar_home}/jars/aws-java-sdk-bundle-1.12.262.jar'
driver_class_path=f'{jar_home}/jars/hadoop-cloud-storage-3.3.4.jar:{jar_home}/jars/aws-java-sdk-bundle-1.12.262.jar'

dag = DAG(
    dag_id="Spark_testing",
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
    jars=app_jars,
    driver_class_path=driver_class_path,
    packages='org.apache.hadoop:hadoop-aws:3.3.4,io.delta:delta-core_2.12:2.0.0,org.apache.spark:spark-hadoop-cloud_2.13:3.3.4',
    conf=minio_conf,
    verbose = 1,
    dag=dag
)

spark_job