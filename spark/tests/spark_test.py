from pyspark.sql import SparkSession    
from delta import *
import socket

MINIO_IP_ADDRESS = socket.gethostbyname("localhost")

# Create a SparkSession
builder = SparkSession.builder \
    .appName("Test Spark on Airflow") \
    .config("spark.hadoop.fs.s3a.access.key", "xdHe7t4BYpfcZPWal8ho") \
    .config("spark.hadoop.fs.s3a.secret.key", "zrSbj3ShgJqQHmLMAAse86VahptuzNex44BHDH4g") \
    .config("spark.hadoop.fs.s3a.path.style.access", "true") \
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
    .config("spark.hadoop.fs.s3a.endpoint", f"http://{MINIO_IP_ADDRESS}:9000") \
    .config("spark.hadoop.fs.s3a.aws.credentials.provider", "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    

# from delta import configure_spark_with_delta_pip

spark = configure_spark_with_delta_pip(builder).getOrCreate()
# spark = builder.getOrCreate()
print("--- Spark version - Check 1 --- " + spark.version)

df = spark.read.option("header", "true") \
        .csv(f"s3a://stock/bronze/stock_price_data/2024/2024_07/2024_07_01.csv")
# df.tail(5)K
df.show()
spark.stop()
# deltaTable = DeltaTable.forPath(spark, "s3a://stock/gold/stock_price")
# deltaTable.detail()
# try:
#     print("--- Spark version --- " + spark.version)
#     df = spark.read.option("header", "true") \
#         .csv(f"s3a://stock/bronze/stock_price_data/2024/2024_07/2024_07_01.csv")
#     df.show()
# except Exception as e:
#     print(f"General Exception: {e}")
# finally:
#     spark.stop()
