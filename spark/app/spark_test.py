from pyspark.sql import SparkSession    
from pyspark.sql import functions as fn
from delta import *
import pandas as pd

builder = SparkSession.builder.appName("Data Lakehouse") \
      .config("spark.hadoop.fs.s3a.aws.credentials.provider", "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider") \
      .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
      .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \

spark = configure_spark_with_delta_pip(builder).getOrCreate()
# spark = builder.getOrCreate()
sc = spark.sparkContext
# Set the MinIO access key, secret key, endpoint, and other configurations
sc._jsc.hadoopConfiguration().set("fs.s3a.access.key", "xdHe7t4BYpfcZPWal8ho")
sc._jsc.hadoopConfiguration().set("fs.s3a.secret.key", "zrSbj3ShgJqQHmLMAAse86VahptuzNex44BHDH4g")
sc._jsc.hadoopConfiguration().set("fs.s3a.endpoint", "http://minio:9000")
sc._jsc.hadoopConfiguration().set("fs.s3a.path.style.access", "true")
sc._jsc.hadoopConfiguration().set("fs.s3a.connection.ssl.enabled", "false")
bucket = "stock"

print("--- Spark version --- " + spark.version)

df = spark.read.option('header',True).load(f"s3a://{bucket}/silver/stock_price").sort(fn.desc('ngay'),fn.desc('version'))

df = df.selectExpr('ma as symbols','ngay as date','gia_dong_cua as close ', \
          'gia_mo_cua as open','gia_cao_nhat as max_price','gia_thap_nhat as min_price').toPandas()

df['date'] = pd.to_datetime(df['date'], format="%Y-%m-%d")
df = df.astype({'close': float, 'open': float, 'max_price': float, 'min_price': float})

# df.show()
# print(df.dtypes)
print(df.tail(5))

spark.stop()
