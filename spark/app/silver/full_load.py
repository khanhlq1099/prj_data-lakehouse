from pyspark.sql.functions import desc,asc,concat,lit
import config.connection as cn

''' Full load all data from stock_price_data folder 
    - Generate key = ma + @ + ngay
    - Generate version - default value = 1
'''
def full_load():
    spark_conn = cn.spark_connection.spark
    bucket = 'stock'
    # Read all csv files and sort by desc(ngay), asc(ma)
    df = spark_conn.read.option("header", "true") \
        .csv(f"s3a://{bucket}/bronze/stock_price_data/*/*/*.csv").sort(desc(col='ngay'),asc('ma'))

    # Combine and write csv files to delta table
    df.withColumns({'version':lit(1),'key':concat('ma',lit('@'),'ngay')}) \
        .write.format("delta").mode("overwrite").save(f"s3a://{bucket}/silver/stock_price")

full_load()

cn.spark_connection.spark.stop()