from pyspark.sql.functions import concat,lit,desc
# import spark.config.connection as cn
from datetime import date,datetime
from typing import Optional
import src.config.storage as s3

# spark_conn = cn.spark_connection.spark
from delta import *
from pyspark.sql import SparkSession

# Create a SparkSession

builder = SparkSession.builder.appName("Data Lakehouse") \
    .config("spark.hadoop.fs.s3a.aws.credentials.provider", "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .config("spark.debug.maxToStringFields","100")
spark = configure_spark_with_delta_pip(builder).getOrCreate()

# spark = builder.getOrCreate()
sc = spark.sparkContext
# # Set the MinIO access key, secret key, endpoint, and other configurations
sc._jsc.hadoopConfiguration().set("fs.s3a.access.key", "xdHe7t4BYpfcZPWal8ho")
sc._jsc.hadoopConfiguration().set("fs.s3a.secret.key", "zrSbj3ShgJqQHmLMAAse86VahptuzNex44BHDH4g")
sc._jsc.hadoopConfiguration().set("fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
sc._jsc.hadoopConfiguration().set("fs.s3a.endpoint", "http://minio:9000")
sc._jsc.hadoopConfiguration().set("fs.s3a.path.style.access", "true")
sc._jsc.hadoopConfiguration().set("fs.s3a.connection.ssl.enabled", "false")

bucket = 'stock'

# spark.stop()
def manual_incremental_load(extract_date: Optional[date] = None):
    if extract_date is None: 
        extract_date = datetime.today().date()

    extract_month = extract_date.strftime("%Y_%m")
    extract_year = extract_date.strftime("%Y")
    
    # Read csv file
    df = spark.read.option("header", "true") \
        .csv(f"s3a://{bucket}/bronze/stock_price_data/{extract_year}/{extract_month}/{extract_date}.csv")

    # Generate key to check matching 
    key = df.select(concat('ma',lit('@'),'ngay')).dropDuplicates().first()[0]

    # Get key in delta table
    keys = spark.read.option('header',True).load(f"s3a://{bucket}/silver/stock_price").select('key','version')

    # Check matching -> bool, False means already, True means not yet
    check_matching_key = keys.filter(keys.key.contains(key)).isEmpty()
    if check_matching_key: 
        df.withColumns({'version':lit(1),'key':concat('ma',lit('@'),'ngay')}) \
        .write.format("delta").mode('overwrite').save(f"s3a://{bucket}/silver/stock_price")
    else: df.withColumns({'version':lit(keys.filter(keys.key == key).sort(desc('version')).first()['version']+1),'key':lit(key)}) \
        .write.format("delta").mode('overwrite').save(f"s3a://{bucket}/silver/stock_price")

def auto_incremental_load():
    # current_date = datetime.today().strftime("%Y-%m-%d 0:0:0")
    current_date = datetime(2024,10,4).strftime("%Y-%m-%d 0:0:0")
    s3_client = s3.MinIO_S3_client.s3
    params = {'Bucket': 'stock', 'Prefix': 'bronze/stock_price_data'}
    list_objects = s3_client.get_paginator('list_objects_v2').paginate(**params)
    # Get list new file by last modified date
    new_files = list_objects.search(f"Contents[?to_string(LastModified)>='\"{current_date}\"'].Key")

    for key_data in new_files:
        df = spark.read.csv(f"s3a://{bucket}/{key_data}",header=True)

        # Generate key to check matching 
        key = df.select(concat('ma',lit('@'),'ngay')).dropDuplicates().first()[0]
        # print(key)

    # Get key in delta table
    keys = spark.read.option('header',True).load(f"s3a://{bucket}/silver/stock_price").select('key','version')
    # print(keys.head(5))
    # print(keys.filter(keys.key == key).sort(desc('version')).first()['version'])

    # Check matching -> bool, False means already, True means not yet
    check_matching_key = keys.filter(keys.key.contains(key)).isEmpty()
    # print(type(df.toPandas()))
    # if check_matching_key: 
    #     df.withColumns({'version':lit(1),'key':concat('ma',lit('@'),'ngay')}) \
    #     .write.partitionBy("ma").format("delta").mode('overwrite').save(f"s3a://{bucket}/silver/test")
    # else: df.withColumns({'version':lit(keys.filter(keys.key == key).sort(desc('version')).first()['version']+1),'key':lit(key)}) \
    #     .write.partitionBy("ma").format("delta").mode('overwrite').save(f"s3a://{bucket}/silver/test")

    df_from_silver_layer = spark.read.option('header',True).load(f"s3a://{bucket}/silver/test").sort(desc('ngay'),desc('version'))

    df_from_silver_layer.show()

try: 
    print("--- Spark version --- " + spark.version)
    
    auto_incremental_load()

    print("Done.")
finally:
    spark.stop()