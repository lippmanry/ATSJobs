import time
from datetime import datetime, timezone, timedelta
import random
import requests
from dotenv import load_dotenv
from pymongo import MongoClient, UpdateOne
import os
import os
from dotenv import load_dotenv
load_dotenv(override=True)
from utils import (desc_cleanup,  
                format_salary_range, 
                location_validator, 
                date_handler,
                country_handler,
                job_matching)

from hdx.location.country import Country
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from concurrent.futures import ThreadPoolExecutor, as_completed

import math
mongo_uri = os.getenv('MONGO_URI')
app_id = os.getenv('ADZUNA_APP_ID')
app_key = os.getenv('ADZUNA_APP_KEY')
client = MongoClient(mongo_uri)
db = client['all_jobs']
collection = db['adzuna_jobs']
base_url = 'https://api.adzuna.com/v1/api/jobs/'    

def process_jobs(session, app_id, app_key, 
                country='ca',
                what=None,
                what_or=None,
                results_per_page=25,
                profile=None):
    
    #hold the dictionaries
    # job_list = []

    #iterators
    current_page = 1
    total_pages = 1
    total_saved = 0
    search_string = what
    
    #make sure to not slam the endpoint
    while current_page <= total_pages:
        time.sleep(random.uniform(0.5,1.5))
        country = country
        currency_code = Country.get_currency_from_iso2(country)

        url = f'{base_url}{country.lower()}/search/{current_page}'
        
        #params
        params = {
                'app_id': app_id,
                'app_key': app_key,
                'results_per_page': results_per_page,
                'what': what,
                'what_or': what_or,
                'sort_by': 'date'
                }

        #clean up empties/none
        clean_params = {k: v for k, v in params.items() if v is not None}
        
        print(f"Requesting Page {current_page}: {url}?{'&'.join([f'{k}={v}' for k,v in clean_params.items()])}")        
        try:
            response = session.get(url, params=clean_params, timeout=10)
            #check response
            if response.status_code == 200:
                data = response.json()
                results = data.get('results', [])
                
                if not results:
                    break

                #get the total number of results and pages
                if current_page == 1:
                    total_count = data.get('count', 0)
                    total_pages = min(math.ceil(total_count / results_per_page), 10)
                    print(f"Found {total_count} jobs. Syncing...")
                
                page_items = []
                for job in results:
                    title = job.get('title', 'Not given').lower().strip()
                    company = job.get('company', {}).get('display_name', 'Not given')
                    job_id = str(company) + ":" + str(job.get('id'))
                    location_list = job.get('location', {}).get('area', [])
                    location = ", ".join(location_list) if location_list else 'Not given'
                    is_remote = any(term in (title + location).lower() for term in ["remote", "work from home", "wfh"])
                    
                    #days since posted date handling
                    created_str = job.get('created')


                    #description for YOE 
                    job_desc = desc_cleanup(job.get('description', ''))
                    
                    
                    #salary min/max    
                    s_min_raw = job.get('salary_min')
                    s_max_raw = job.get('salary_max')
                    depts = []
                    all_loc_strings = location.lower()
                    is_match, match_word = job_matching(target_locs=profile["country"], 
                                target_keywords=profile["keywords"],
                                all_loc_strings=all_loc_strings,
                                title=title,
                                depts = [],
                                is_remote=is_remote,
                                content=None)

                    
                    if is_match:
                        time_since, days_old, posted_date = date_handler(created_str)
                        if days_old is not None and days_old > 45:
                            continue

                        search_flag = match_word 
                    #formatting
                        salary_range = format_salary_range(s_min_raw, s_max_raw, currency_code)
                        salary_range_usd = format_salary_range(s_min_raw, s_max_raw, currency_code, is_usd=True)

                        extracted_fields = {
                            'job_id': job_id,
                            'job_title': title,
                            'company': company,
                            'location': location,
                            'is_remote': "Unknown",
                            'date_posted': posted_date,
                            'time_since_posted': time_since,
                            'experience': 'Not given',
                            'employment_type': job.get('contract_time', 'Not given'),
                            'salary_range': salary_range,
                            'salary_range_usd': salary_range_usd,
                            'url': job.get('redirect_url', 'Unknown'),
                            'description': job_desc,
                            'search_flag': search_string,
                            "last_scanned": datetime.now(timezone.utc)
                        }

                        page_items.append(UpdateOne(
                                {'job_id': extracted_fields['job_id']}, 
                                {'$set': extracted_fields}, 
                                upsert=True
                            )
                        )

                    if page_items:
                        result = collection.bulk_write(page_items)
                        total_saved += (result.upserted_count + result.modified_count)

                    
                    # job_list.extend(page_items)
                    current_page += 1
                    time.sleep(1)
                
            #too many datas, take a sleep
            elif response.status_code == 429:
                print("Rate limit hit. Sleeping for 60s...")
                time.sleep(60)
                continue
            #something is messed up
            else:
                print(f'Error response status {response.status_code}: {response.text}')
                break
        
        except Exception as e:
            print(f'An error has occurred: {e}')
            break
        
    print(f"Sync complete. {total_saved} items processed.")

def adzuna_jobs():
    session = requests.Session()
    retries = Retry(total=3, backoff_factor=1, status_forcelist=[502, 503, 504])
    session.mount('https://', HTTPAdapter(max_retries=retries))
    session.headers.update({
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36"
    })
    ryan_keywords = ["cybersecurity", "siem", "splunk", "threat", "vulnerability", "security", "junior software", "junior backend",]
    mik_keywords = ["frontend", "front end", "front-end", "vue", "product engineer", "design engineer", "web design"]
    ryan_loc = ["canada", "ontario"]
    mik_loc = ["united kingdom", "uk", "gb", "london"]
    profiles = [
        {"name": "Ryan",
        "keywords": ryan_keywords,
        "country": "ca",
        "location": ryan_loc},
        {"name": "Mik",
        "keywords": mik_keywords,
        "country": "gb",
        "location": mik_loc}
    ]


    for person in profiles:
        name = person["name"]
        keywords = person["keywords"]
        country = person["country"]
        
        with ThreadPoolExecutor(max_workers=3) as executor:
            futures = [executor.submit(
                process_jobs,
                session=session,
                app_id=app_id,
                app_key=app_key,
                country=country,
                what=query,
                profile=person
            ) for query in keywords
        ]
        total_saved = 0
        for future in as_completed(futures):
            try:
                count = future.result()
                if count:
                    total_saved += count
            except Exception as e:
                print(f"Keyword failed: {e}")
        

        
    session.close()
    print(f"Total jobs saved: {total_saved}")
    return total_saved

if __name__ == "__main__":
    adzuna_jobs()