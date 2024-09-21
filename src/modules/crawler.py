import pandas as pd
from datetime import date,datetime
from src.library import handling_helper
import re

from selenium import webdriver
from webdriver_manager.chrome import ChromeDriverManager
from selenium.webdriver.chrome.service import Service as ChromeService
from selenium.webdriver.chrome.options import Options
from time import sleep,time

from bs4 import BeautifulSoup
from selenium.webdriver.remote.webelement import WebElement

from selenium.webdriver.common.by import By

def extract_daily_symbol_price_data(symbol: str, from_date: date, to_date: date, driver: webdriver.Chrome = None) -> pd.DataFrame:

    date_key = from_date.strftime("%d/%m/%Y") + ' - ' + to_date.strftime("%d/%m/%Y")
    page_index = 1
    url = f"https://s.cafef.vn/Lich-su-giao-dich-{symbol}-{page_index}.chn"

    if driver is None:
        # Set up Driver
        chrome_options = webdriver.ChromeOptions()
        # chrome_options.binary_location = "/Users/lamquockhanh10/VSCodeProjects/kpim_stock/stock_etl/stock/lib/chromedriver"
        chrome_options.add_argument("--headless")
        chrome_options.add_argument('--no-sandbox')
        chrome_options.add_argument("--disable-setuid-sandbox")
        chrome_options.add_argument('--disable-dev-shm-usage') 
        chrome_options.add_argument("--window-size=1920x1080")
        chrome_options.add_argument('--disable-gpu')
        driver = webdriver.Chrome(service=ChromeService(ChromeDriverManager().install()), options=chrome_options)
        # driver = webdriver.Remote("http://selenium:4444/wd/hub",options=chrome_options)

    driver.implicitly_wait(10)
    driver.set_page_load_timeout(60)
    driver.get(url)
    driver.find_element(By.XPATH, '//*[@id="date-inp-disclosure"]').send_keys(date_key)
    
    sleep(1)
    driver.find_element(By.XPATH,'//*[@class="applyBtn btn btn-sm btn-primary"]').click()
    sleep(1)
    driver.find_element(By.XPATH, '//*[@id="owner-find"]').click()
    sleep(1)
    rows = []

    next_page_btn = driver.find_element(By.XPATH,'//*[@id="divStart"]/div/div[3]/div[3]')
    num_pages = len(driver.find_elements(By.XPATH,'//*[@id="wraper-content-paging"]/div'))

    while True:
        row = extract_data_from_table(symbol=symbol,driver=driver)
        rows.extend(row)
        try:
            # check have/don't have a next page
            has_next = True if num_pages >= 2 else False
            
        # print(has_next)
        except:
            has_next = False
        
        if has_next:
            next_page_btn.click()
            sleep(1)
            num_pages -= 1
        else: 
            break       
        
    # df = pd.DataFrame(rows)
    # print(df)
    return rows

def extract_data_from_table(symbol:str,driver:WebElement):
    def handle_table_row(tr):
        tds = [td.text.strip() for td in tr.find_all("td")]
        ngay = datetime.strptime(tds[0], '%d/%m/%Y').date()

        gia_dong_cua = handling_helper.handle_price_thousand_vnd(tds[1])
        # print(tds[1])
        gia_dieu_chinh = handling_helper.handle_price_thousand_vnd(tds[2])

        change_strs = tds[3].strip().split('(')
        if len(change_strs) == 2:
            gia_tri_thay_doi = handling_helper.handle_price_thousand_vnd(change_strs[0]) 

            percent_change_str = re.sub(r'[( %)]', '', change_strs[1])
            temp = handling_helper.convert_str_to_float(percent_change_str)
            phan_tram_thay_doi =  temp / 100 if temp is not None else None 
        else:
            gia_tri_thay_doi = None
            phan_tram_thay_doi = None

        gd_khop_lenh_khoi_luong = handling_helper.convert_str_to_decimal(tds[4].replace(',', ''))
        gd_khop_lenh_gia_tri = handling_helper.convert_str_to_decimal(tds[5].replace(',', ''))

        gd_thoa_thuan_khoi_luong = handling_helper.convert_str_to_decimal(tds[6].replace(',', ''))
        gd_thoa_thuan_gia_tri = handling_helper.convert_str_to_decimal(tds[7].replace(',', ''))

        gia_mo_cua = handling_helper.handle_price_thousand_vnd(tds[8])
        gia_cao_nhat = handling_helper.handle_price_thousand_vnd(tds[9])
        gia_thap_nhat = handling_helper.handle_price_thousand_vnd(tds[10])

        tr_list = [symbol,ngay,gia_dong_cua,gia_dieu_chinh,gia_tri_thay_doi,phan_tram_thay_doi,
                   gd_khop_lenh_khoi_luong, gd_khop_lenh_gia_tri
                   ,gd_thoa_thuan_khoi_luong,gd_thoa_thuan_gia_tri,
                    gia_mo_cua,gia_cao_nhat,gia_thap_nhat]

        return tr_list

    tbody = driver.find_element(By.XPATH,'//*[@id="render-table-owner"]')
    # Parser website
    soup = BeautifulSoup(tbody.get_attribute('innerHTML'),"html.parser")
    rows = []

    trs = soup.find_all('tr')
    for tr in trs:
        rows.append(handle_table_row(tr))

    return rows

# symbols = "TCB,HPG,FPT,POW,VHM"
# symbols = symbols.split(",")
# for symbol in symbols:
#     extract_daily_symbol_price_data(symbol,datetime(2024,6,28),datetime(2024,6,28))