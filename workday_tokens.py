import requests
import re
import random
import time
from urllib.parse import quote_plus
from datetime import datetime
import undetected_chromedriver as uc
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
import time
import random
from utils import save_tokens_mongo
import os
from dotenv import load_dotenv
from pymongo import MongoClient


load_dotenv(override=True)
mongo_uri = os.getenv('MONGO_URI')
client = MongoClient(mongo_uri)
db = client['all_jobs']
token_collection = db['workday_tokens']


token_collection = db['workday_tokens']
#github version
def get_driver():
    options = uc.ChromeOptions()
    options.add_argument('--headless')  
    options.add_argument('--no-sandbox')
    options.add_argument('--disable-dev-shm-usage')
    options.add_argument('--window-size=1920,1080')
    driver = uc.Chrome(options=options)
    
    return driver

def workday_token_search(keyword, limit=10, start=0, gl="ca"):
    keywords = " ".join([f'"{w}"' for w in keyword.split()])
    raw_query = f'site:myworkdayjobs.com {keywords}'
    query = quote_plus(raw_query)
    
    tokens = set()
    
    driver = get_driver()
    print(f"Searching: https://www.google.com/search?q={query}&num={limit}&gl={gl}&start={start}")

    
    try:
        driver.get("https://www.google.com")
        time.sleep(random.uniform(2, 4))
        
        search_url = f"https://www.google.com/search?q={query}&num={limit}&gl={gl}&start={start}"
        driver.get(search_url)
        if "Forbidden" in driver.title or "403" in driver.title:
            print(f"Still getting 403. Wait.")
            return []
        
        #deal with potential consent banner
        try:

            wait = WebDriverWait(driver, 5) 
            consent_btn = wait.until(EC.element_to_be_clickable((By.ID, 'W0wltc')))
            consent_btn.click()
            print("Consent clicked.")
            time.sleep(2)                
        except:
            #continue as normal if no consent banner
            print("No consent required.")
            pass            

        
        time.sleep(random.uniform(7,12))
        
        #emulate human scrolling to try and avoid scraper flag
        driver.execute_script(f"window.scrollBy(0, {random.randint(400,900)});")

        
        #find link elements
        links = driver.find_elements(By.XPATH, "//a[contains(@href, 'myworkdayjobs.com')]")
        
        for link in links:
            url = link.get_attribute("href")
            if url:
                match = re.search(r"([a-zA-Z0-9_-]+)\.(wd[0-9]+)\.myworkdayjobs\.com", url)
                if match:
                    tenant = match.group(1).lower()
                    datacenter = match.group(2).lower()
                    tokens.add(f"{tenant}:{datacenter}")

        
    except Exception as e:
        print(f"An error has occurred: {e}")     
    finally:
        driver.quit()
        
    return list(tokens)
def workday_new_tokens():
    #search for multiple industries via common operations
    anchor_words = ["Analyst", "Developer", "Manager", "Operations", "IT", "Engineer", "Compliance", "Staff", "Information Technology", "Analytics", "Security"]
    locations = ["Canada", "United Kingdom", "UK", "Global", "Remote", "EMEA", "North America"]
    gls = ["us", "ca", "uk"]
    successful_runs = 0
    
    while successful_runs < 3:
        new_found = {}
        
        word = random.choice(anchor_words)
        location = random.choice(locations)
        query = f'{word} {location}'
        start_index = random.choice([0,10])
        gl = random.choice(gls)
        new_found = workday_token_search(keyword=query, limit=20, start=start_index, gl=gl)
        
        if new_found:
            save_tokens_mongo(new_found, token_collection=token_collection)
            successful_runs += 1
            print(f"{len(new_found)} new tokens found on [{datetime.now()}]: {new_found}") 
            
            if successful_runs < 3:
                wait_time = random.randint(30,60)
                print(f"Resting {wait_time}s...")
                time.sleep(wait_time)
        
        else:
            wait_time = random.randint(30,60)
            print(f"No new tokens found for {query}. Trying new keywords in {wait_time}s...")
            time.sleep(wait_time)