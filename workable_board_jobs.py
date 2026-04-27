#jobs from the workable board
import time
from datetime import datetime, timezone
import random
import requests
from dotenv import load_dotenv
from pymongo import MongoClient, UpdateOne
import os
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from concurrent.futures import ThreadPoolExecutor
from utils import (desc_cleanup,  
                date_handler,
                salary_handler,
                job_matching)

#inits
load_dotenv(override=True)
mongo_uri = os.getenv('MONGO_URI')
client = MongoClient(mongo_uri)
db = client['all_jobs']
collection = db['workable_board_jobs']   

def workable_scraper(profile, keywords, locations):
    total_saved = 0
    job_list = []
    
    search_url = "https://jobs.workable.com/api/v1/jobs"
    session = requests.Session()
    retries = Retry(
        total=5,
        backoff_factor=2,
        status_forcelist=[429, 500, 502, 503, 504])
    session.mount('https://', HTTPAdapter(max_retries=retries))
    session.headers.update({
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36"
        })
    kw = keywords
    loc = locations
        
    params = {
        "query": kw,
        "location": loc,
        "limit": 50
        }
            
    try:
        response = session.get(search_url, params=params)
        
        if response.status_code == 429:
            wait_time = int(response.headers.get("Retry-After", 30))
            print(f"Rate limit hit, sleeping for {wait_time}s")
            time.sleep(wait_time)
            return 0
        if response.status_code == 200:
            data = response.json() 
            jobs = data.get('jobs', [])
            
            for job in jobs:
                #check if already in DB
                company = job.get('company', {}).get('title')
                id = str(company) + ":" + str(job.get('id'))
                if collection.find_one({"job_id": id}):
                    continue
                title = job.get('title')
                
                #remote handling
                workplace_raw = (job.get('workplace') or "")
                workplace = str(workplace_raw).lower() if workplace_raw else ""
                
                job_loc_raw = job.get('locations', []) or []
                job_locs = [str(loc).lower() for loc in job_loc_raw if loc]
                
                remote_flag = any("telecommute" in loc for loc in job_locs)
                is_remote = ("remote" in workplace or remote_flag)
                
                #department
                department = job.get('department') or []
                

                employment_type = job.get('employmentType')
                country_raw = job.get('location',{}).get('countryName')
                country = str(country_raw).lower() if country_raw else ""
                
                city = job.get('location',{}).get('city')
                #display location
                d_loc = [l for l in [city, country] if l]
                display_location = ", ".join(d_loc) if d_loc else "Not given"
                
                full_text = f"{job.get('benefitsSection', '')} {job.get('description', '')}"
                
                text_search = desc_cleanup(full_text)
                desc = desc_cleanup(job.get('requirementsSection',''))
                
                salary_range, salary_range_usd = salary_handler(text_search, country)

                all_loc_strings = list(set(job_locs + [country]))
                all_loc_strings = [l.strip() for l in all_loc_strings if l.strip()]
                
                for person in profile:
                    is_match, match_word = job_matching(
                        person["locations"],
                        person["keywords"],
                        all_loc_strings,
                        title, full_text, department, is_remote
                    )
                
                if is_match:
                    time_since, days_old, date_posted = date_handler(job.get('created'))                            
                    if days_old is not None and days_old > 45:
                        continue

                    search_flag = match_word
                    url = job.get('url')
                    
                    extracted_fields = {
                        "job_id": id,
                        "job_title": title,
                        "company": company,
                        "location": display_location,
                        "is_remote": is_remote,
                        "date_posted": date_posted,
                        "time_since_posted": time_since,
                        "experience": "Not given",
                        "employment_type": employment_type,
                        "salary_range": salary_range,
                        "salary_range_usd": salary_range_usd,
                        "url": url,
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
            print(f"Saved {total_saved} jobs.")
        time.sleep(random.uniform(3,7))
        return total_saved
    except Exception as e:
        print(f"Error for {profile} searching for {kw}: {e}")
        return 0
    

def workable_board_jobs():
    ryan_keywords = ["cybersecurity", "siem", "splunk", "threat", "vulnerability", "security", "analytics engineer", "analytic", "incident", "risk", "junior software", "junior backend", "junior back end", "junior developer", "privacy", "cyber", "data analyst"]
    mik_keywords = ["frontend", "front end", "front-end", "vue", "product engineer", "web design", "web developer"]

    ryan_loc = ["canada", "ontario"]
    mik_loc = ["united kingdom", "uk", "gb"]
    
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
    for person in profiles:
        for kw in person["keywords"]:
            for loc in person["locations"]:
                tasks.append(([person],kw,loc))

    with ThreadPoolExecutor(max_workers=5) as executor:
        results = list(executor.map(
            lambda t: workable_scraper(t[0], t[1], t[2]), 
            tasks
            ))
        total_saved = sum(results)
        print(f"Total jobs saved: {total_saved}")
        return total_saved
    session.close()

if __name__ == "__main__":
    workable_board_jobs()