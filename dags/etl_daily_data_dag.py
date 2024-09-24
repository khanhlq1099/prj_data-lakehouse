from airflow import DAG
from airflow.utils.dates import days_ago
from datetime import datetime
from airflow.operators.python import PythonOperator

def extract_daily():
    from src.modules.service import extract_stock_data
    from src.config.period import PERIOD_TYPE 
    # extract_stock_data(period_type=PERIOD_TYPE.TODAY,extract=datetime.today().date())
    extract_stock_data(period_type=PERIOD_TYPE.PERIOD,from_date=datetime(2024,9,23),to_date=datetime(2024,9,23))

default_args ={
    'owner': 'Khanh - Lam Quoc'
}

with DAG(
    dag_id='ETL_Stock_Price_Data',
    # schedule_interval='0 9,11,13,15 * * *',
    schedule_interval=None,
    default_args=default_args,
    # description='Daily Extract Stock Data',
    start_date=days_ago(1),
    tags=["example"],
    catchup=False,
) as dag:
    extract = PythonOperator(
          task_id = 'extract',
          python_callable=extract_daily,
          dag=dag
    )
extract