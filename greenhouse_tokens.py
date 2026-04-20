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
from urllib.parse import quote_plus


#inits
load_dotenv(override=True)
mongo_uri = os.getenv('MONGO_URI')
client = MongoClient(mongo_uri)
db = client['all_jobs']        
token_collection = db['greenhouse_tokens']


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
        
#token search        
def greenhouse_token_search(keyword, limit=50, start=0):
    raw_query = f'site:jobs.lever.co OR site:jobs.eu.lever.co {keyword}'
    query = quote_plus(raw_query)
    
    tokens = set()
    
    #chrome options
    chrome_options = Options()
    chrome_options.add_argument("--disable-blink-features=AutomationControlled")
    chrome_options.add_argument("user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36")
    chrome_options.add_argument("--headless=new")
    chrome_options.add_argument("--no-sandbox")
    chrome_options.add_argument("--disable-dev-shm-usage")

    
    
    driver = webdriver.Chrome(service=Service(ChromeDriverManager().install()), options=chrome_options)
    
    try:
        driver.get(f"https://www.google.com/search?q={query}&num={limit}&gl=ca&start={start}")
        
        #deal with potential consent banner
        try:
            time.sleep(2)
            consent_btn = driver.find_element(By.XPATH, "//button[contains(., 'Accept all')]")
            consent_btn.click()                
        except:
            #continue as normal if no consent banner
            pass            

        
        time.sleep(random.uniform(7,12))
        
        #emulate human scrolling to try and avoid scraper flag
        driver.execute_script(f"window.scrollBy(0, {random.randint(400,900)});")
        
        #find link elements
        links = driver.find_elements(By.XPATH, "//a[contains(@href, 'boards.greenhouse.io')]")
        
        for link in links:
            url = link.get_attribute("href")
            if url:
                match = re.search(r"boards\.greenhouse\.io/([^/&?#]+)", url)
                if match:
                    token = match.group(1).strip()
                    if token not in ['embed', 'search', 'expect', 'v1', 'boards']:
                        tokens.add(token)
        
    except Exception as e:
        print(f"An error has occurred: {e}")     
    finally:
        driver.quit()
        
    return list(tokens)

#slow token discovery, add to mongo - 3 iterations via common keywords
def greenhouse_new_tokens():
    #search for multiple industries via common operations
    anchor_words = ["Analyst", "Developer", "Manager", "Operations", "Sales", "Engineer"]
    
    locations = ["Canada", "United Kingdom", "UK", "Global", "Remote"]
    
    successful_runs = 0
    
    while successful_runs < 3:
        new_found = {}
        
        word = random.choice(anchor_words)
        location = random.choice(locations)
        query = f"{word} {location}"
        start_index = random.choice([0,5,10,15,20])
        
        new_found = greenhouse_token_search(keyword=query, limit=20, start=start_index)
        
        if new_found:
            save_tokens_mongo(new_found)
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
        
    # max_attempts =  3
    # new_found = {}
    
    # for attempt in range(1, max_attempts + 1):
    #     word = random.choice(anchor_words)
    #     location = random.choice(locations)
    #     query = f"{word} {location}"
        
    #     start_index = random.choice([0,5,10,15,20])        
    #     new_found = greenhouse_token_search(keyword=query, limit=20, start=start_index)
    #     if new_found:
    #         save_tokens_mongo(new_found)
    #         print(f"{len(new_found)} new tokens found on [{datetime.now()}]: {new_found}")
            
    #     if attempt < max_attempts:
    #         wait_time = random.randint(30, 60)
    #         print(f"No new tokens found, switching keywords and waiting {wait_time}s...")
    #         time.sleep(wait_time)            
    #     else:
    #         print(f"No new tokens found.")

if __name__ == "__main__":
    greenhouse_new_tokens()    