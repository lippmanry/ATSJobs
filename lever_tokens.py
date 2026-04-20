#company search
import re
import time
from datetime import datetime
import random
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.options import Options
from webdriver_manager.chrome import ChromeDriverManager
from dotenv import load_dotenv
from pymongo import MongoClient, UpdateOne
from datetime import datetime
import os

#inits
load_dotenv(override=True)
mongo_uri = os.getenv('MONGO_URI')
client = MongoClient(mongo_uri)
db = client['all_jobs']        
token_collection = db['lever_tokens']


#save tokens to mongo
def save_tokens_mongo(tokens):
    ops = []
    for token in tokens:
        ops.append(UpdateOne(
            {'token': token},
                {
                '$setOnInsert': {
                    'is_active': True,
                    'failures': 0,
                    'priority': False
                },
                '$set': {'last_seen_on_google': datetime.now()}
                },
            upsert=True
            )
        )
    if ops:
        token_collection.bulk_write(ops)
def lever_token_search(limit=10, start=0):
    #blacklist ai gigs
    BLACKLIST = {"mercor", "dataannotation", "lionbridge"}
    
    query = 'site:jobs.lever.co OR site:jobs.eu.lever.co ("Canada" OR "United Kingdom" OR "UK" OR "Global" OR "Remote")'
    
    #chrome options
    chrome_options = Options()
    chrome_options.add_argument("--disable-blink-features=AutomationControlled")
    chrome_options.add_argument("user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36")
    chrome_options.add_argument("--headless=False")
    chrome_options.add_argument("--no-sandbox")
    chrome_options.add_argument("--disable-dev-shm-usage")
    chrome_options.add_argument("--disable-extensions")
    chrome_options.add_argument("--proxy-server='direct://'")
    chrome_options.add_argument("--proxy-bypass-list=*")
    chrome_options.add_argument("--start-maximized")

    driver = webdriver.Chrome(service=Service(ChromeDriverManager().install()), options=chrome_options)
    
    tokens = {}
    
    try:
        driver.get(f"https://www.google.com/search?q={query}&num={limit}&gl=ca&start={start}")

        time.sleep(random.uniform(7,12))
        
        #emulate human scrolling to try and avoid scraper flag
        driver.execute_script(f"window.scrollBy(0, {random.randint(400,900)});")
        
        #find link elements
        links = driver.find_elements(By.XPATH, "//a[contains(@href, 'lever.co')]")
        print(len(links))
        for link in links:
            url = link.get_attribute("href")
            if url:
                match = re.search(r"jobs\.((?:eu)\.)?lever\.co/([^/&?#]+)", url)
                if match:
                    prefix = match.group(1)
                    token = match.group(2).strip().lower()
                    
                    if token not in ['apply', 'job-collections', 'questionnaire', 'search'] and token not in BLACKLIST:
                        region = "eu" if (prefix and "eu" in prefix) else "global" 
                        tokens[token] = region
        
                
        
    except Exception as e:
        print(f"An error has occurred: {e}")     
    finally:
        driver.quit()
        
    return tokens

def lever_new_tokens():
    current_count = token_collection.count_documents({})
    start_index = current_count if current_count < 300 else random.randint(0,150)
    new_found = lever_token_search(limit=60, start=start_index)
    if new_found:
        save_tokens_mongo(new_found)
        print(f"{len(new_found)} new tokens found on [{datetime.now()}]: {new_found}")
    else:
        print(f"No new tokens found.")

if __name__ == "__main__":
    lever_new_tokens()