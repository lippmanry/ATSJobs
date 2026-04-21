#company search
import re
import time
from datetime import datetime
import random
from dotenv import load_dotenv
from pymongo import MongoClient, UpdateOne
import os

from ddgs import DDGS


#inits
load_dotenv(override=True)
mongo_uri = os.getenv('MONGO_URI')
client = MongoClient(mongo_uri)
db = client['all_jobs']        
token_collection = db['ashby_tokens']

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
                '$set': {
                    'last_seen_by_SE': datetime.now()
                    }
                },
            upsert=True
            )
        )
    if ops:
        token_collection.bulk_write(ops)
def ashby_token_search(keyword):
    tokens = set()
    query = f'site:jobs.ashbyhq.com {keyword}'
    print(f"Searching for {query}")
    
    with DDGS() as ddgs:
        #DuckDuckGo for faster results and less block heavy
        results = ddgs.text(query, max_results=20)
        for r in results:
            url = r['href']
            match = re.search(r"jobs\.ashbyhq\.com/([^/&?#]+)", url)
            if match:
                token = match.group(1).strip()
                if token not in ['embed', 'search', 'v1', 'd']:
                    tokens.add(token)
    return list(tokens)

def ashby_new_tokens():
    #search for multiple industries via common operations, jobs, tech, benefits, locations...
    anchor_words = ["Analyst", "Developer", "Manager", "Operations", "Sales", "Engineer", "Compliance", "Staff", "Sustainability", "Analytics"]
    tech_stack = ["Splunk", "SIEM", "Vue", "python", "javascript", ""]
    benefit_list = ["Remote", "asynchronous", "autonomy", "Flexible", ""]
    locations = ["Canada", "United Kingdom", "UK", "Global", "Remote", "EMEA"]
    
    successful_runs = 0
    new_tokens = set()
    
    while successful_runs < 3:
        new_found = {}
        
        word = random.choice(anchor_words)
        location = random.choice(locations)
        tech = random.choice(tech_stack)
        benefits = random.choice(benefit_list)
        query_bits = [p for p in [word, location, tech, benefits] if p]
        query = " ".join(query_bits)
        
        
        new_found = ashby_token_search(keyword=query)


        if new_found:
            save_tokens_mongo(new_found)
            new_tokens.update(new_found)
            successful_runs += 1
            print(f"{len(new_found)} new tokens found on [{datetime.now()}]: {new_found}")
        else:
            print(f"No new tokens found found for {query}. Retrying...")
        wait_time = random.uniform(15,30)
        print(f"Sleeping for {round(wait_time, 2)}s...")
        time.sleep(wait_time)
    print(f"Total unique tokens found: {len(new_tokens)}")
    return list(new_tokens)

if __name__ == "__main__":
    ashby_new_tokens()