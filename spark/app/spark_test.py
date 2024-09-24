from pyspark.sql import SparkSession    

import socket

MINIO_IP_ADDRESS = socket.gethostbyname("minio")

# Create a SparkSession
spark = SparkSession.builder \
    .appName("Test Spark on Airflow") \
    .getOrCreate()

# spark = builder.getOrCreate()
print("--- Spark version - Check 1 --- " + spark.version)

df = spark.read.option("header", "true") \
        .csv(f"s3a://stock/bronze/stock_price_data/2024/2024_07/2024_07_01.csv")
# df.tail(5)K
df.show()
spark.stop()

    # .config("spark.hadoop.fs.s3a.access.key", "xdHe7t4BYpfcZPWal8ho") \
    # .config("spark.hadoop.fs.s3a.secret.key", "zrSbj3ShgJqQHmLMAAse86VahptuzNex44BHDH4g") \
    # .config("spark.hadoop.fs.s3a.path.style.access", "true") \
    # .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
    # .config("spark.hadoop.fs.s3a.endpoint", f"http://{MINIO_IP_ADDRESS}:9000") \
    # .config("spark.hadoop.fs.s3a.aws.credentials.provider", "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider") \