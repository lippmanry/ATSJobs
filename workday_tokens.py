
import re
import random
import time
from ddgs import DDGS
from datetime import datetime
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
def workday_token_search(site, keyword):
    tokens = set()
    query = f'site:{site} {keyword}'
    print(f"Searching DDG for {query}")
    
    system_blacklist = {'embed', 'search', 'v1', 'd', 'api', 'js', 'widgets', 'careers', 'www', 'myworkday', 'impl', 'preview'}
    pattern = r"([a-zA-Z0-9_-]+)\.(wd[0-9]+)\.myworkdayjobs\.com"
    
    with DDGS() as ddgs:
        results = ddgs.text(query, max_results=20)
        for r in results:
            url = r['href']
            match = re.search(pattern, url)
            if match:
                tenant = match.group(1).lower()
                datacenter = match.group(2).lower()
                if tenant not in system_blacklist:
                    tokens.add(f"{tenant}:{datacenter}")
    return list(tokens)
def workday_new_tokens():
    #search for multiple industries via common operations
    anchor_words = ["Analyst", "Developer", "Manager", "Operations", "IT", "Engineer", "Compliance", "Staff", "Analytics", "Security"]
    locations = ["Canada", "United Kingdom", "UK", "Global", "Remote", "EMEA", "North America"]
    site = "myworkdayjobs.com"
    successful_runs = 0
    new_tokens = set()
    
    while successful_runs < 3:
        new_found = {}
        
        word = random.choice(anchor_words)
        location = random.choice(locations)
        query = f'{word} {location}'
        
        new_found = workday_token_search(site,keyword=query)
        
        if new_found:
            save_tokens_mongo(new_found, token_collection=token_collection)
            new_tokens.update(new_found)
            successful_runs += 1
            print(f"{len(new_found)} new tokens found on [{datetime.now()}]: {new_found}") 
            
            if successful_runs < 3:
                wait_time = random.randint(30,60)
                print(f"Resting {wait_time}s...")
                time.sleep(wait_time)
        
        else:
            print(f"No new tokens found found for {query}. Retrying...")
        wait_time = random.uniform(15,30)
        print(f"Sleeping for {round(wait_time, 2)}s...")
        time.sleep(wait_time)
    print(f"Total unique tokens found: {len(new_tokens)}")
    return list(new_tokens)

if __name__ == "__main__":
    workday_new_tokens()