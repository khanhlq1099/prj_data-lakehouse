from pyspark.sql import SparkSession
# from duckdb.experimental.spark.sql import SparkSession
from delta import *
from pyspark.sql import functions as fn
from pyspark.sql.types import IntegerType,DateType
import pandas as pd

class spark_connection:
    # Create a SparkSession
    builder = SparkSession.builder.appName("Data Lakehouse") \
        .master('local[*]') \
        .config("spark.hadoop.fs.s3a.aws.credentials.provider", "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider") \
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
        .config("spark.hadoop.fs.s3a.endpoint", "http://localhost:9000") \
        .config("spark.hadoop.fs.s3a.access.key", "xdHe7t4BYpfcZPWal8ho") \
        .config("spark.hadoop.fs.s3a.secret.key", "zrSbj3ShgJqQHmLMAAse86VahptuzNex44BHDH4g") \
        .config("spark.hadoop.fs.s3a.path.style.access", "true") \
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false") 

    spark = configure_spark_with_delta_pip(builder).getOrCreate()
bucket = 'stock'

spark = spark_connection.spark

df = spark.read.option('header',True).load(f"s3a://{bucket}/gold/stock_price").sort(fn.desc('ngay'),fn.desc('version'))

df = df.selectExpr('ma as symbols','ngay as date','gia_dong_cua as close ', \
          'gia_mo_cua as open','gia_cao_nhat as max_price','gia_thap_nhat as min_price').toPandas()

df['date'] = pd.to_datetime(df['date'], format="%Y-%m-%d")
df = df.astype({'close': float, 'open': float, 'max_price': float, 'min_price': float})

# df.to_csv("src/data/stock_price.csv")
import duckdb as db 

# df.info
conn = db.connect("lakehouse.duckdb", read_only = False)
# Note: duckdb.sql connects to the default in-memory database connection
# conn.sql("CREATE TABLE stock_price AS SELECT * FROM df").show()
# conn.sql("SHOW ALL TABLES").show()