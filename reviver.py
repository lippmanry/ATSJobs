#keep datacat jobs from going to sleep on streamlit
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
import time

streamlit_url = "https://datacat-jobs.streamlit.app/"

options = Options()
options.add_argument('--headless=new')
driver = webdriver.Chrome(options=options)

try:
    driver.get(streamlit_url)
    time.sleep(2)
    title = driver.title
    print(f"{title} visited.")
finally:
    driver.quit()
    