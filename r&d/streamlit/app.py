import streamlit as st,pandas as pd
from pyspark.sql import SparkSession,functions as fn
from delta import *
import plotly.express as px

class spark_connection:
    # Create a SparkSession
    builder = SparkSession.builder.appName("Data Lakehouse") \
        .master('local[*]') \
        .config("spark.hadoop.fs.s3a.aws.credentials.provider", "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider") \
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
        .config("spark.hadoop.fs.s3a.access.key", "khanhlq10") \
        .config("spark.hadoop.fs.s3a.secret.key", "khanhlq10") \
        .config("spark.hadoop.fs.s3a.path.style.access", "true") \
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
        .config("spark.hadoop.fs.s3a.endpoint", "http://localhost:9000") \

    spark = configure_spark_with_delta_pip(builder).getOrCreate()

conn = spark_connection.spark

df = conn.read.option('header',True).load(f"s3a://stock/gold/stock_price").sort(fn.desc('ngay'),fn.desc('version'))

df = df.selectExpr('ma as symbols','ngay as date','gia_dong_cua as close ', \
          'gia_mo_cua as open','gia_cao_nhat as max_price','gia_thap_nhat as min_price').toPandas()

df['date'] = pd.to_datetime(df['date'], format="%Y-%m-%d")
df = df.astype({'close': float, 'open': float, 'max_price': float, 'min_price': float})

# print(df.dtypes)
series = df['symbols'].drop_duplicates()
symbol = series.tolist()
# print(symbol)
# df.info()
# df.set_index('date',inplace=True)
df_tcb = df[(df['symbols'] == 'TCB') & (df['date'] >= '2024-01-02')]

close = df_tcb['close'] / 1000
st.set_page_config(layout="wide", page_title='Stock Dashboard', page_icon="📈")
# st.title('Drop Down Menu')
# st.selectbox(label='Ticker',options=symbol)

fig = px.line(data_frame=df_tcb,x=df_tcb['date'],y=close,title='TCB')
st.plotly_chart(figure_or_data=fig)

# ------ layout setting---------------------------
window_selection_c = st.sidebar.container() # create an empty container in the sidebar
window_selection_c.markdown("## Insights") # add a title to the sidebar container
sub_columns = window_selection_c.columns(2) #Split the container into two columns for start and end date

tickers = symbol
SYMB = window_selection_c.selectbox("Ticker", tickers)

conn.stop()