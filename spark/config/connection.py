from pyspark.sql import SparkSession
from delta import *

class spark_connection:
    # Create a SparkSession
    builder = SparkSession.builder.appName("Data Lakehouse") \
        # .master("local[*]") \
        # .config("spark.hadoop.fs.s3a.aws.credentials.provider", "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider") \
        # .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
        # .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")

    spark = configure_spark_with_delta_pip(builder).getOrCreate()
    # Get the SparkContext from the SparkSession
    sc = spark.sparkContext
    # Set the MinIO access key, secret key, endpoint, and other configurations
    # sc._jsc.hadoopConfiguration().set("fs.s3a.access.key", "xdHe7t4BYpfcZPWal8ho")
    # sc._jsc.hadoopConfiguration().set("fs.s3a.secret.key", "zrSbj3ShgJqQHmLMAAse86VahptuzNex44BHDH4g")
    # sc._jsc.hadoopConfiguration().set("fs.s3a.endpoint", "http://localhost:9000")
    # sc._jsc.hadoopConfiguration().set("fs.s3a.path.style.access", "true")
    # sc._jsc.hadoopConfiguration().set("fs.s3a.connection.ssl.enabled", "false")

bucket = 'stock'

conn = spark_connection.spark