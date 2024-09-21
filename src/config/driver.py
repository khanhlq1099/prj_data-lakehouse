from selenium import webdriver
from webdriver_manager.chrome import ChromeDriverManager
from selenium.webdriver.chrome.service import Service as ChromeService

def setup_driver_local_mode():
    # Set up Driver
    chrome_options = webdriver.ChromeOptions()
    chrome_options.add_argument("--headless")
    chrome_options.add_argument('--no-sandbox')
    chrome_options.add_argument("--disable-setuid-sandbox")
    chrome_options.add_argument('--disable-dev-shm-usage') 
    chrome_options.add_argument("--window-size=1920x1080")
    chrome_options.add_argument('--disable-gpu')
    driver = webdriver.Chrome(service=ChromeService(ChromeDriverManager().install()), options=chrome_options)
    

    # driver.quit()
    return driver

def setup_driver_selenium_mode():
    # Set up Driver
    chrome_options = webdriver.ChromeOptions()
    chrome_options.add_argument("--headless")
    chrome_options.add_argument('--no-sandbox')
    chrome_options.add_argument("--disable-setuid-sandbox")
    chrome_options.add_argument('--disable-dev-shm-usage') 
    chrome_options.add_argument("--window-size=1920x1080")
    chrome_options.add_argument('--disable-gpu')
    driver = webdriver.Remote("http://selenium:4444/wd/hub",options=chrome_options)

    return driver

# setup_driver_local_mode()