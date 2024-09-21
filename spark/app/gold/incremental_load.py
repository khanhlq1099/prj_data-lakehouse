from pyspark.sql.functions import desc,first
import config.connection as cn

spark_conn = cn.spark_connection.spark
bucket = 'stock'

def incremental_load_gold_layer():
    df_from_silver_layer = spark_conn.read.option('header',True).load(f"s3a://{bucket}/silver/stock_price").sort(desc('ngay'),desc('version'))

    df_from_silver_layer.show()

    # Read the latest version
    latest_version_df = df_from_silver_layer.orderBy(desc("version")).groupBy("ngay","ma").agg(first("version").alias("version"))
    '''
        +----------+---+-------+
    |      ngay| ma|version|
    +----------+---+-------+
    |2024-07-02|TCB|      3|
    |2024-07-02|HPG|      3|
    |2024-07-02|VHM|      3|
    |2024-07-02|POW|      3|
    |2024-07-02|FPT|      3|
    |2024-07-01|FPT|      1|
    |2024-07-01|VHM|      1|
    |2024-07-01|TCB|      1|
    |2024-07-01|HPG|      1|
    |2024-07-01|POW|      1|
    '''

    # df_to_gold_layer = df_from_silver_layer.join(latest_version_df,['ma','ngay','version'])
    '''
    +---+----------+-------+------------+--------------+----------------+--------------------+-----------------------+--------------------+------------------------+---------------------+----------+------------+-------------+--------------+
| ma|      ngay|version|gia_dong_cua|gia_dieu_chinh|gia_tri_thay_doi|  phan_tram_thay_doi|gd_khop_lenh_khoi_luong|gd_khop_lenh_gia_tri|gd_thoa_thuan_khoi_luong|gd_thoa_thuan_gia_tri|gia_mo_cua|gia_cao_nhat|gia_thap_nhat|           key|
+---+----------+-------+------------+--------------+----------------+--------------------+-----------------------+--------------------+------------------------+---------------------+----------+------------+-------------+--------------+
|TCB|2024-07-02|      3|       22900|          NULL|             250|0.011000000000000001|                9248300|              212.21|                       0|                    0|     22850|       23200|        22650|TCB@2024-07-02|
|HPG|2024-07-02|      3|       28700|          NULL|             350|              0.0123|               21258400|              606.72|                  120222|                 3.45|     28350|       28850|        28250|TCB@2024-07-02|
|VHM|2024-07-02|      3|       38150|          NULL|             450|0.011899999999999999|                5422700|              205.74|                       0|                    0|     37750|       38350|        37550|TCB@2024-07-02|
|POW|2024-07-02|      3|       14500|          NULL|              50|-0.00340000000000...|                9441100|               137.1|                       1|                    0|     14700|       14700|        14400|TCB@2024-07-02|
|FPT|2024-07-02|      3|      128000|          NULL|             600|-0.00469999999999...|                6720600|              873.66|                  839401|               109.69|    128600|      132100|       127500|TCB@2024-07-02|
|FPT|2024-07-01|      1|      129000|          NULL|            -500|             -0.0115|                3334900|              430.68|                       0|                    0|    130400|      130400|       128500|FPT@2024-07-01|
|VHM|2024-07-01|      1|       37250|          NULL|             400|             -0.0106|                2270300|               84.79|                       0|                    0|     37650|       37700|        37100|VHM@2024-07-01|
|TCB|2024-07-01|      1|       22750|          NULL|             600|-0.02569999999999...|                3647200|               83.51|                       0|                    0|     23100|       23100|        22750|TCB@2024-07-01|
|HPG|2024-07-01|      1|       28150|          NULL|             150|             -0.0053|                6119500|              172.67|                       0|                    0|     28300|       28500|        28050|HPG@2024-07-01|
|POW|2024-07-01|      1|       14500|          NULL|             400|             -0.0268|                6014600|               87.63|                       0|                    0|     14800|       14900|        14400|POW@2024-07-01|
    '''
    # df_to_gold_layer.show()
    # df_to_gold_layer.write.format("delta").mode('append').save(f"s3a://{bucket}/gold/stock_price")

incremental_load_gold_layer()