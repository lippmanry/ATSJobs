#company search

import time
from datetime import datetime, timezone
import random
import requests
from dotenv import load_dotenv
from pymongo import MongoClient, UpdateOne
from datetime import datetime
import os
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from concurrent.futures import ThreadPoolExecutor
from utils import (desc_cleanup, 
                fix_pay, 
                format_salary_range, 
                job_detail_getter, 
                job_matching, 
                remote_checker, 
                date_handler)


#inits
load_dotenv(override=True)


mongo_uri = os.getenv('MONGO_URI')
client = MongoClient(mongo_uri)
db = client['all_jobs']        
token_collection = db['greenhouse_tokens']
collection = db["greenhouse_jobs"]

def process_single_token(profile, token, session):
    total_saved = 0
    job_list = []
    token = token
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36"
    }

    api_url = f"https://boards-api.greenhouse.io/v1/boards/{token}/jobs?content=true"
        
    try:
        response = session.get(api_url, timeout=(10,90))
        
        #rate limit handler
        if response.status_code == 429:
            wait_time = int(response.headers.get("Retry-After", 30))
            print(f"Rate limit hit, sleeping for {wait_time}s")
            time.sleep(wait_time)
            return 0
        
        if response.status_code == 200:
            #reset failures on success
            token_collection.update_one({'token': token}, {'$set': {'failures': 0}})
            
            data = response.json()
            jobs = data.get('jobs', [])
            total_jobs_found = len(jobs)
            print(f"    > {total_jobs_found} jobs for token {token}. Filtering...")                

            #heartbeat for debugging
            for index, job in enumerate(jobs):
                if index % 100 == 0 and index > 0:
                    print(f"    > Processed {index}/{total_jobs_found}")
            
                title = job.get('title', '').lower()
                raw_content = job.get('content', '').strip()
                clean_content = desc_cleanup(raw_content) or ""
                searchable_content = clean_content.lower()
                depts = [d.get('name', '').lower() for d in job.get('departments',[]) if d.get('name')]
                id = str(token) + ":" + str(job.get('id'))
            
                
                primary_loc = job.get('location', {}).get('name', "").strip().lower()
                
                metadata_list = job.get('metadata') or []
                remote_metas = next((str(m.get('value', '')).lower() for m in metadata_list if m.get('id') == 7742247003), "")
                is_remote = remote_checker([primary_loc, remote_metas])
                
                exclude_names = {'cost', 'tier', 'weighted', 'responsible'}
                exclude_values = {'mid', 'high', 'low'}
                meta_locs = []
                for m in metadata_list:
                    m_name = (m.get('name') or "").lower()
                    m_val = m.get('value')
                    
                    if 'location' in m_name and m_val:
                        if not any(term in m_name for term in exclude_names):
                            vals = m_val if isinstance(m_val, list) else [m_val]
                            
                            for v in vals:
                                v_str = str(v).strip().lower()
                                if v_str not in exclude_values:
                                    meta_locs.append(v_str)
                all_loc_strings = list(set(
                    [primary_loc.lower()] + [l.lower() for l in meta_locs]
                )) if primary_loc else list(set(l.lower() for l in meta_locs))  
                                
                display_location = ", ".join(dict.fromkeys([primary_loc] + meta_locs)) if primary_loc or meta_locs else "Not given"
                
                is_match, match_word = job_matching(profile["locations"], profile["keywords"], all_loc_strings, title, depts, is_remote, content=None)
                
                if is_match:
                    job_id = job.get('id')
                    details = job_detail_getter(token, job_id, headers)
                    if not details:
                        continue
                    updated_dt = details.get('updated_at')
                    time_since, days_old, date_posted = date_handler(updated_dt)                           
                    if days_old is not None and days_old > 60:
                        continue
                    

                    search_flag = match_word

                    

                    salary_range = "Not given"
                    salary_range_usd = "Not given"
                    name = profile["name"]
                    country = "canada" if name == "Ryan" else "uk"
                    
                    #try built in transparency first
                    pay_details = details.get('pay_input_ranges', {}) if details else {}
                    pay_data = next((item for item in pay_details if item.get('min_cents')), None)

                    if pay_data:
                        min_v = pay_data.get('min_cents')/100
                        max_v = pay_data.get('max_cents')/100
                        currency = pay_data.get('currency_type')
                        salary_range = format_salary_range(min_v, max_v, currency)
                        salary_range_usd = format_salary_range(min_v, max_v, currency, is_usd=True)
                    
                    else:
                        regex_pay = fix_pay(clean_content, country)
                        if regex_pay:
                            min_v = regex_pay['min']
                            max_v = regex_pay['max']
                            currency = regex_pay['currency']
                            salary_range = format_salary_range(min_v, max_v, currency)
                            salary_range_usd = format_salary_range(min_v, max_v, currency, is_usd=True)
                
                    extracted_fields = {
                        "job_id": id,
                        'job_title': title,
                        'company': job.get('company_name'),
                        'location': display_location,
                        'is_remote': is_remote,
                        'date_posted': date_posted,
                        'time_since_posted': time_since,
                        "experience": "Not given",
                        "employment_type": "Not given",
                        "salary_range": salary_range,
                        "salary_range_usd": salary_range_usd,
                        "url": job.get('absolute_url'),
                        "description": clean_content,
                        "search_flag":search_flag,
                        "last_scanned": datetime.now(timezone.utc)                           
                    }

                    job_list.append(UpdateOne(
                    {'job_id': extracted_fields['job_id']},
                    {'$set': extracted_fields},
                    upsert=True
                    )  
                )
            if job_list:
                result = collection.bulk_write(job_list)
                total_saved += (result.upserted_count + result.modified_count)
                print(f"Token {token}: Saved {result.upserted_count + result.modified_count} jobs.")

            #a wee polite sleep
            time.sleep(random.uniform(0.8, 1.5))
        else:
            #count failures in token collection
            token_collection.update_one({'token': token}, {'$inc': {'failures': 1}})
        return 0
    except requests.exceptions.Timeout:
        print(f"Timeout occurred for token {token}. Skipping...")
        return 0
    except Exception as e:
        import traceback
        print(f"An error has occurred for token {token}: {e}")
        traceback.print_exc()
        return 0

def greenhouse_jobs():
    active_tokens = token_collection.find({
    'is_active': True,
    'failures': {'$lt': 3}
    })
    #session logic
    session = requests.Session()
    retries = Retry(total=3, backoff_factor=1, status_forcelist=[502, 503, 504])
    session.mount('https://', HTTPAdapter(max_retries=retries))
    session.headers.update({
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36"
    })    
    
    token_data = [t.get('token') for t in active_tokens if t.get('token')]
    

    
    ryan_keywords = ["cybersecurity", "siem", "splunk", "threat", "vulnerability", "security", "analytics engineer", "analytic", "incident", "risk", "junior software", "junior backend", "junior back end", "junior developer"]
    mik_keywords = ["frontend", "front end", "front-end", "vue", "product engineer", "web design", "web developer"]
    
    ryan_loc = ["canada", "ontario", "global"]
    mik_loc = ["united kingdom", "uk", "gb", "global"]
    profiles = [
    {
        "name": "Ryan",
        "keywords": ryan_keywords,
        "locations": ryan_loc
    },
    {
        "name": "Mik",
        "keywords": mik_keywords,
        "locations": mik_loc
    }
    ]
    tasks = []
    for token in token_data:
        for person in profiles:
            tasks.append((token, person))
    with ThreadPoolExecutor(max_workers=5) as executor:
        results = list(executor.map(
            lambda t: process_single_token(                
                profile=t[1], 
                token= t[0],
                session=session
                ),
                tasks
            ))
    total_saved = sum(results)
    session.close()
    print(f"Total jobs saved: {total_saved}")
    return total_saved


if __name__ == "__main__":
    greenhouse_jobs()