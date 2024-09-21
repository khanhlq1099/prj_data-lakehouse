from pyspark.sql.functions import concat,lit,desc
import config.connection as cn
from datetime import date,datetime
from typing import Optional
from src.config.storage import minio_local

spark_conn = cn.spark_connection.spark
bucket = 'stock'

def manual_incremental_load(extract_date: Optional[date] = None):
    if extract_date is None: 
        extract_date = datetime.today().date()

    extract_month = extract_date.strftime("%Y_%m")
    extract_year = extract_date.strftime("%Y")
    
    # Read csv file
    df = spark_conn.read.option("header", "true") \
        .csv(f"s3a://{bucket}/bronze/stock_price_data/{extract_year}/{extract_month}/{extract_date}.csv")

    # Generate key to check matching 
    key = df.select(concat('ma',lit('@'),'ngay')).dropDuplicates().first()[0]

    # Get key in delta table
    keys = spark_conn.read.option('header',True).load(f"s3a://{bucket}/silver/stock_price").select('key','version')

    # Check matching -> bool, False means already, True means not yet
    check_matching_key = keys.filter(keys.key.contains(key)).isEmpty()
    if check_matching_key: 
        df.withColumns({'version':lit(1),'key':concat('ma',lit('@'),'ngay')}) \
        .write.format("delta").mode('overwrite').save(f"s3a://{bucket}/silver/stock_price")
    else: df.withColumns({'version':lit(keys.filter(keys.key == key).sort(desc('version')).first()['version']+1),'key':lit(key)}) \
        .write.format("delta").mode('overwrite').save(f"s3a://{bucket}/silver/stock_price")

def auto_incremental_load():
    current_date = datetime.today().strftime("%Y-%m-%d %0:%0:%0")

    s3_client = minio_local.s3
    params = {'Bucket': 'stock', 'Prefix': 'bronze/stock_price_data'}
    list_objects = s3_client.get_paginator('list_objects_v2').paginate(**params)
    # Get list new file by last modified date
    new_files = list_objects.search(f"Contents[?to_string(LastModified)>='\"{current_date}\"'].Key")

    for key_data in new_files:
        df = spark_conn.read.csv(f"s3a://{bucket}/{key_data}",header=True)

        # Generate key to check matching 
        key = df.select(concat('ma',lit('@'),'ngay')).dropDuplicates().first()[0]
        # print(key)

    # Get key in delta table
    keys = spark_conn.read.option('header',True).load(f"s3a://{bucket}/silver/stock_price").select('key','version')
    # print(keys.filter(keys.key == key).sort(desc('version')).first()['version'])

    # Check matching -> bool, False means already, True means not yet
    check_matching_key = keys.filter(keys.key.contains(key)).isEmpty()
    if check_matching_key: 
        df.withColumns({'version':lit(1),'key':concat('ma',lit('@'),'ngay')}) \
        .write.format("delta").mode('overwrite').save(f"s3a://{bucket}/silver/stock_price")
    else: df.withColumns({'version':lit(keys.filter(keys.key == key).sort(desc('version')).first()['version']+1),'key':lit(key)}) \
        .write.format("delta").mode('overwrite').save(f"s3a://{bucket}/silver/stock_price")
