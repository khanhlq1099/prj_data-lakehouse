from datetime import datetime, date
from typing import Optional

import src.modules.crawler as crawler
from src.config.driver import setup_driver_local_mode,setup_driver_selenium_mode
from src.config.period import PERIOD_TYPE
from src.library import datetime_helper

from selenium import webdriver
from webdriver_manager.chrome import ChromeDriverManager
from selenium.webdriver.chrome.service import Service as ChromeService

import io
from src.config.storage import upload_df_to_s3
import pandas as pd


def extract_stock_data(period_type:PERIOD_TYPE,extract: Optional[date] = None,
                       from_date: Optional[date] = None, to_date: Optional[date] = None):

    print(f"---Task: ETL Daily Stock Price Data---")
    start_time = datetime.now()

    symbols = "TCB,HPG,FPT,POW,VHM"
    # symbols='TCB'
    symbols = symbols.split(",")
    print(symbols)

    if period_type != PERIOD_TYPE.PERIOD:
        from_date, to_date = datetime_helper.calc_period_range(extract=extract,period_type=period_type)
    if period_type == PERIOD_TYPE.PERIOD:
        from_date = from_date
        to_date = to_date

    if not ( from_date and to_date):
        return

    driver = setup_driver_selenium_mode()
    # driver = setup_driver_local_mode()
    all_rows = []
    try:
        for symbol in symbols:
            all_rows.extend(crawler.extract_daily_symbol_price_data(symbol=symbol, from_date= from_date, to_date= to_date, driver=driver))

        df = pd.DataFrame(all_rows)
        df.columns = ["ma","ngay","gia_dong_cua","gia_dieu_chinh","gia_tri_thay_doi","phan_tram_thay_doi",
                    "gd_khop_lenh_khoi_luong","gd_khop_lenh_gia_tri","gd_thoa_thuan_khoi_luong","gd_thoa_thuan_gia_tri",
                    "gia_mo_cua","gia_cao_nhat","gia_thap_nhat"]
        
        if df.shape[0] >=1 and df.shape[0] <= 5:
            print(df)
            # df.to_csv('utils/data.csv', index=False)
            upload_df_to_s3(df=df,extract=from_date)

        elif df.shape[0] > 5:
            grouped_df = df.groupby('ngay')
            for key, item in grouped_df:
                sub_df = grouped_df.get_group(key)
                upload_df_to_s3(df=sub_df,extract=key)

        else: print("Missing data.")
    except Exception as e:
        print(e)

    driver.quit()   

    end_time = datetime.now()
    print(f"From date: {from_date} - To date: {to_date}")
    print(f"Start at: {start_time.time()} - End at: {end_time.time()} \nDuration all symbols: {end_time - start_time}")


# driver = setup_driver_local_mode()
# extract_stock_data(period_type=PERIOD_TYPE.PERIOD,from_date=datetime(2020,2,1),to_date=datetime(2020,2,29))
# extract_stock_data(period_type=PERIOD_TYPE.TODAY,extract=datetime(2023,10,31))