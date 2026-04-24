
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
from concurrent.futures import ThreadPoolExecutor, as_completed
from functools import partial
from utils import (desc_cleanup,  
                format_salary_range, 
                job_matching, 
                date_handler,
                build_lever_url,
                fix_pay)



#inits
load_dotenv(override=True)

mongo_uri = os.getenv('MONGO_URI')
client = MongoClient(mongo_uri)
db = client['all_jobs']    
token_collection = db['lever_tokens']
collection = db['lever_jobs']




def process_single_token(item, session, profile):
    total_saved = 0
    job_list = []
    token = item['token']
    region = item['region']
    region = "global"
    url = build_lever_url(token, region)
    

    try:
        response = session.get(url, timeout=(10,90))
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
            total_jobs_found = len(data)
            print(f"    > {total_jobs_found} jobs for token {token}. Filtering...")
            
            #heartbeat for debugging
            for index, job in enumerate(data):
                if index % 100 == 0 and index > 0:
                    print(f"    > Processed {index}/{total_jobs_found}")
            
            for job in data:
                company = token
                job_id = str(company) + ":" + str(job.get('id'))
                title = job.get('text', '').lower()
                desc_plain = job.get('descriptionPlain', '')
                add_plain = job.get('additionalPlain', '')
                
                # list data
                list_content = ""
                for item in job.get('lists',[]):
                    list_content += f" {item.get('text', '')} {item.get('content', '')}".lower()
                
                # check content for keywords
                raw_content = f"{title} {desc_plain} {add_plain} {list_content}"
                clean_content = desc_cleanup(raw_content).strip() or ""
                searchable_content = clean_content.lower()
                
                #job categories
                cats = job.get('categories', {})
                depts = []
                if cats.get('team'): depts.append(cats.get('team').lower())
                if cats.get('subteam'): depts.append(cats.get('subteam').lower())
                
                #remote check
                workplace = job.get('workplaceType') or ""
                is_remote = True if "remote" in workplace.lower() else False
                
                #deal with locations
                onsite_req = (cats.get('location') or "").strip().lower()
                all_loc_list = cats.get('allLocations') or []
                
                if not isinstance(all_loc_list, list):
                    all_loc_list = [all_loc_list]
                
                country_code = (job.get('country') or "").strip().lower()
                
                clean_all_locations = [str(loc).lower().strip() for loc in all_loc_list if loc]
                
                all_loc_strings = list(set([onsite_req, country_code] + clean_all_locations))
                
                # #ryan's
                # ryan_loc_check = location_validator(ryan_loc, all_loc_strings)
                # ryan_target_in_soup = any(any(t in loc_str for t in ryan_loc) for loc_str in all_loc_strings)
                
                # mik_loc_check = location_validator(mik_loc, all_loc_strings)
                # mik_target_in_soup = any(any(t in loc_str for t in mik_loc) for loc_str in all_loc_strings)
                
                # ryan_match_word = next((k for k in ryan_keywords if k in title or k in searchable_content or any(d and k in d for d in depts)), None)
                # ryan_key_match = any(k in title or k in searchable_content or any(d and k in d for d in depts) for k in ryan_keywords)


                # #mik's
                # mik_match_word = next((k for k in mik_keywords if k in title or k in searchable_content or any(d and k in d for d in depts)), None)
                # mik_key_match = any(k in title or k in searchable_content or any(d and k in d for d in depts) for k in mik_keywords)

                # ryan_match= ryan_key_match and (ryan_loc_check or (is_remote and ryan_target_in_soup))

                # mik_match = mik_key_match and (mik_loc_check or (is_remote and mik_target_in_soup))

                is_match, match_word = job_matching(profile["locations"], profile["keywords"], all_loc_strings, title, depts, is_remote, content=None)
                    
                if is_match:
                    #check posting age, filter out old posts
                    time_since, days_old, date_posted = date_handler(job.get('createdAt'))
                    if days_old is not None and days_old > 45:
                        continue

                    all_locations = job.get('categories', {}).get('allLocations',[])
                    location = ", ".join(all_locations)

                    #time commitment 
                    commitment = job.get('categories', {}).get('commitment') 
                    
                    min_v, max_v, currency = None, None, None
                    country = job.get('country')
                    
                    #try built in pay data first
                    pay_data = job.get('salaryRange', {}) 
                    
                    if pay_data and pay_data.get('min') is not None:
                        min_v = pay_data.get('min')
                        max_v = pay_data.get('max')
                        currency = pay_data.get('currency')
                        pay_interval = pay_data.get('interval')
                        
                        #handle hourly pay
                        if pay_interval == 'per-hour-wage' and min_v:
                            min_v = min_v * 2080
                            if max_v:
                                max_v = max_v * 2080
                        
                        #skip one time payments and extremely low compensation
                        if pay_interval == 'one-time' or (min_v and min_v < 40000):
                            continue
                        
                        salary_range = format_salary_range(min_v, max_v, currency)
                        salary_range_usd = format_salary_range(min_v, max_v, currency, is_usd=True)
                    
                    if min_v is None:
                        content_parts = [
                            job.get('additionalPlain', ''),
                            job.get('descriptionBodyPlain', '')
                                        ]
                        clean_content = " ".join(filter(None, content_parts)).strip()
                        regex_pay = fix_pay(clean_content, country)
                        if regex_pay:
                            min_v = regex_pay['min']
                            max_v = regex_pay['max']
                            currency = regex_pay['currency']

                    if min_v:
                        salary_range = format_salary_range(min_v, max_v, currency)
                        salary_range_usd = format_salary_range(min_v, max_v, currency, is_usd=True)
                    else:
                        salary_range = "Not given"        
                        salary_range_usd = "Not given"
                    search_flag = match_word    
                    extracted_fields = {
                        "job_id": job_id,
                        "job_title": title,
                        "company": company,
                        "location": location,
                        "is_remote": is_remote,
                        "date_posted": date_posted,
                        "time_since_posted": time_since,
                        "experience": "Not given",
                        "employment_type": commitment,
                        "salary_range": salary_range,
                        "salary_range_usd": salary_range_usd,
                        "url": job.get('hostedUrl'),
                        "description": job.get('descriptionBodyPlain'),
                        "search_flag": search_flag,
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
                
            #a wee slumber
            time.sleep(random.uniform(0.8, 1.5))
        else:
            token_collection.update_one({'token': token}, {'$inc':{'failures': 1}})
        return total_saved    
    except requests.exceptions.Timeout:
        print(f"Timeout occurred for token {token}. Skipping...")
        return 0
    except Exception as e:
        import traceback
        print(f"An error has occurred for token {token}: {e}")
        traceback.print_exc()
        return 0


def lever_jobs():
    
    session = requests.Session()
    retries = Retry(total=3, backoff_factor=1, status_forcelist=[502, 503, 504])
    session.mount('https://', HTTPAdapter(max_retries=retries))
    session.headers.update({
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36"
    })
    
    active_tokens = list(token_collection.find({
    'is_active': True,
    '$or': [
        {'failures': {'$lt': 3}},
        {'last_scanned': {'$lt': datetime.now(timezone.utc) - timedelta(days=7)}}
    ]
    }))
    token_data = [{
        'token': t.get('token'),
        'region': t.get('region')
        }
        for t in active_tokens if t.get('token')
    ]
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
    for item in token_data:
        for person in profiles:
            tasks.append((item, person))
    
    with ThreadPoolExecutor(max_workers=5) as executor:
        futures = [
            executor.submit(process_single_token,
                            item=t[0],
                            profile=t[1],
                            session=session
                            ) for t in tasks
        ]
        results = []
        for future in as_completed(futures):
            try:
                res = future.result()
                results.append(res if res is not None else 0)
            except Exception as e:
                print(f"An error has occurred: {e}")
    total_saved = sum(results)
    print(f"Total jobs saved: {total_saved}")
    session.close()
if __name__ == "__main__":
    lever_jobs()        