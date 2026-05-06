#company search
import time
from datetime import datetime, timezone, timedelta
import random
import requests
from dotenv import load_dotenv
from pymongo import MongoClient, UpdateOne
import os

from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from concurrent.futures import ThreadPoolExecutor
from functools import partial
from utils import (desc_cleanup,  
                format_salary_range, 
                location_validator, 
                date_handler,
                country_handler,
                job_matching)


#inits
load_dotenv(override=True)
mongo_uri = os.getenv('MONGO_URI')
client = MongoClient(mongo_uri)
db = client['all_jobs']        
token_collection = db['ashby_tokens']
collection = db['ashby_jobs']

def process_single_token(profile, token, session):
    total_saved = 0
    job_list = []
    token = token

    
    url = f"https://api.ashbyhq.com/posting-api/job-board/{token}?includeCompensation=true"
    
    try:
        response = session.get(url, timeout=(10,90))
        
        if response.status_code == 429:
            wait_time = int(response.headers.get("Retry-After", 30))
            print(f"Rate limit hit, sleeping for {wait_time}s")
            time.sleep(wait_time)
            return 0
        
        if response.status_code == 200:
            #reset failures on success
            token_collection.update_one({'token': token}, {'$set': {'failures': 0}})
            data = response.json()
            jobs = data['jobs']
            total_jobs_found = len(jobs)
            print(f"    > {total_jobs_found} jobs for token {token}. Filtering...")
            
            #heartbeat for debugging
            for index, job in enumerate(jobs):
                if index % 100 == 0 and index > 0:
                    print(f"    > Processed {index}/{total_jobs_found}")
            
                title = job.get('title', '').lower().strip()
                primary_loc = (job.get('location') or "").lower()
                workplace = (job.get('workplaceType') or "").lower()
                is_remote = ("remote" in workplace or "remote" in primary_loc or job.get('isRemote') is True)
                # date_posted = job.get('publishedAt')
                

                company = token.lower().strip()
                job_id = str(company) + ":" + str(job.get('id'))
                
                #country handler
                location = country_handler(job)
                
                #locations
                
                secondary_loc = job.get('secondaryLocations') or []
                
                loc_strings = []
                for loc in secondary_loc:
                    if not loc:
                        continue
                    loc_strings.append(loc.get('location', '').lower())
                    address_obj = loc.get('address') or {}
                    s_addr = address_obj.get('postalAddress', {}) or {}
                    loc_strings.extend([str(v).lower() for v in s_addr.values() if v])
                
                #location soup
                all_loc_strings = list(set([primary_loc, location.lower()] + loc_strings))
                all_loc_strings = [l.strip() for l in all_loc_strings if l]
                
                
                desc = job.get('descriptionPlain', '').strip()
                raw_content = f"{title} {desc}"
                clean_content = desc_cleanup(raw_content) or ""
                searchable_content = clean_content.lower()
                
                #job categories
                team = (job.get('team') or "").lower()
                dept = (job.get('department') or "").lower()
                depts = [d for d in [team, dept] if d]
                
                #validators
            
                is_match, match_word = job_matching(
                    profile["locations"],
                    profile["keywords"],
                    all_loc_strings,
                    title, depts, is_remote, content=None, company=company
                )
                if is_match:
                    time_since, days_old, date_posted = date_handler(job.get('publishedAt'))                           
                    if days_old is not None and days_old > 60:
                        continue

                    search_flag = match_word
                
                
                    
                    all_locations = ", ".join([loc.title() for loc in all_loc_strings])

                    
                    comp = (job.get('compensation') or {}).get('summaryComponents') or []
                    salary_data = next((c for c in comp if c.get('compensationType') == 'Salary'), {})
                    
                    min_v, max_v, currency = None, None, None
                    currency = salary_data.get('currencyCode')
                    min_v = salary_data.get('minValue')
                    max_v = salary_data.get('maxValue')
                    
                    if min_v:
                        salary_range = format_salary_range(min_v, max_v, currency)
                        salary_range_usd = format_salary_range(min_v, max_v, currency, is_usd=True)
                    else: 
                        salary_range = "Not given"
                        salary_range_usd = "Not given"
                    
                    
                    job_url = job.get('jobUrl', '')
                    
                    extracted_fields = {
                        "job_id": job_id,
                        "job_title": title,
                        "company": token,
                        "location": all_locations,
                        "is_remote": is_remote,
                        "date_posted": date_posted,
                        "time_since_posted": time_since,
                        "experience": "Not given",
                        "employment_type": job.get('employmentType'),
                        "salary_range": salary_range,
                        "salary_range_usd": salary_range_usd,
                        "url": job_url,
                        "description": desc,
                        "search_flag": search_flag,
                        "last_scanned": datetime.now(timezone.utc)
                    }
                    job_list.append(UpdateOne(
                        {'job_id': extracted_fields['job_id']},
                        {'$set': extracted_fields},
                        upsert=True
                    ))
            if job_list:
                result = collection.bulk_write(job_list)
                total_saved += (result.upserted_count + result.modified_count)
                print(f"Token {token}: Saved {total_saved} jobs.")
                
            #a wee slumber
            time.sleep(random.uniform(0.8, 1.5))
            return total_saved
        else:
            token_collection.update_one({'token': token}, {'$inc':{'failures': 1}})
        return 0  
    except requests.exceptions.Timeout:
        print(f"Timeout occurred for token {token}. Skipping...")
        return 0
    except Exception as e:
        import traceback
        print(f"An error has occurred for token {token}: {e}")
        traceback.print_exc()
        return 0
def ashby_jobs():
    
    session = requests.Session()
    retries = Retry(total=3, backoff_factor=1, status_forcelist=[502, 503, 504])
    session.mount('https://', HTTPAdapter(max_retries=retries))
    session.headers.update({
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36"
    })
    
    active_tokens = token_collection.find({
    'is_active': True,
    '$or': [
        {'failures': {'$lt': 3}},
        {'last_scanned': {'$lt': datetime.now(timezone.utc) - timedelta(days=7)}}
    ]
    })
    token_data = [t.get('token') for t in active_tokens if t.get('token')]
    
    ryan_keywords = ["cybersecurity", "siem", "splunk", "threat", "vulnerability", "security", "analytics engineer", "analytic", "incident", "risk", "junior software", "junior backend", "junior back end", "junior developer", "privacy", "cyber", "data analyst", "detection"]
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
    ashby_jobs()