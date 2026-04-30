import requests
import re
import random
import time
from datetime import datetime, timezone

import time
import random

import os
from dotenv import load_dotenv
from pymongo import MongoClient, UpdateOne
from utils import date_handler, desc_cleanup, fix_pay, format_salary_range
load_dotenv(override=True)
mongo_uri = os.getenv('MONGO_URI')
client = MongoClient(mongo_uri)
db = client['all_jobs']
token_collection = db['workday_tokens']
collection = db['workday_jobs']


ryan_keywords = ["cybersecurity", "siem", "splunk", "threat", "vulnerability", "security", "analytics engineer", "analytic", "incident", "risk", "junior software", "junior backend", "junior back end", "junior developer", "privacy", "cyber", "data analyst"]
mik_keywords = ["frontend", "front end", "front-end", "vue", "product engineer", "web design", "web developer"]
profiles = [
    {
        "name": "Ryan",
        "keywords": ryan_keywords,
        "locations": ["Canada"]
    },
    {
        "name": "Mik",
        "keywords": mik_keywords,
        "locations": ["United Kingdom"]
    }
]
#funcs
def search_recursive(item_list, parent_param, target):
        for item in item_list:
            descriptor = str(item.get('descriptor', '')).lower()
            current_item_param = item.get('facetParameter', parent_param)
            
            if target in descriptor:
                return item['id'], current_item_param
            
            if 'values' in item and item['values']:

                res = search_recursive(item['values'], current_item_param, target)
                if res: return res
        return None
def facet_search(facets_list, target_name):
    target = target_name.lower().strip()
    for group in facets_list:
        group_p = group.get('facetParameter')
        if any(x in group_p.lower() for x in ['location', 'hierarchy', 'region']):
            found = search_recursive(group.get('values', []), group_p, target)
            if found: return found
    return None, None


def job_matching(target_keywords, title):
    target_keywords = list(target_keywords) if target_keywords else []
    title_lower = str(title).lower() if title else ""
    
    
    #blacklist some word combos
    blacklist = ["security guard", "director", "mechanical design", "electrical design", "electronics design" "intern", "business executive"]
    for word in blacklist:
        pattern = rf"\b{re.escape(word)}\b"
        if re.search(pattern, title_lower):
            return False, None
    
    #check title first
    title_match = next((k for k in target_keywords if k in title_lower), None)

    match_word = title_match 
    if not match_word:
        return False, None
    
    
    is_match = False
        #check matches
    if title_match:
        is_match = True
    else:
        is_match = True
    return is_match, match_word


        
def job_getter(token, dc, site, targets):
    session = requests.Session()

    base_url = f"https://{token}.{dc}.myworkdayjobs.com/{site}"
    search_url = f"https://{token}.{dc}.myworkdayjobs.com/wday/cxs/{token}/{site}/jobs"
    user_agents = [
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36",
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
        "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36",
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:124.0) Gecko/20100101 Firefox/124.0"
    ]

    headers_base = {
        "User-Agent": random.choice(user_agents),
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8",
    }
    
    try:

        session.get(base_url, headers=headers_base, timeout=60)
        time.sleep(1)
    except Exception as e:
        print(f"Connection issue for token {token}: {e}")
        return []
    
    try:
        search_headers = {
            "Content-Type": "application/json",
            "Accept": "application/json, text/plain, */*",           
            "User-Agent": headers_base["User-Agent"],
            "Referer": f"{base_url}/jobs",
            "Origin": f"https://{token}.{dc}.myworkdayjobs.com",

        }

        #discovery to find the location facets
        discovery_payload = {"appliedFacets": {}, "limit": 1, "offset": 0, "searchText": ""}
        d_res = session.post(search_url, json=discovery_payload, headers=search_headers)

        if d_res.status_code != 200:
            return []
        if d_res.status_code == 200:
            facets = d_res.json().get('facets', [])
            if not facets:
                print(f"Warning: No facets found for token {token}.")
    except Exception as e:
        print(f"Discovery failed for token {token}: {e}")
        return []
        
    applied_facets = {}
    for loc in targets:
        f_id, f_param = facet_search(facets, loc)
        if f_id:
            #rename if maingroup
            if "maingroup" in f_param.lower():
                f_param = "locations"
            
            if f_param not in applied_facets:
                applied_facets[f_param] = []
            applied_facets[f_param].append(f_id)
                    # print(f"   [DEBUG] facets: {applied_facets}")
    #pagination vars
    jobs = []
    limit = 20
    offset = 0
    initial_run = True
    #placeholder
    total_count = 999
    
    try:        
        while offset < total_count:
            #final payload using dynamic location facets
            final_payload = {
                "appliedFacets": applied_facets,
                "limit": limit, 
                "offset": offset,
                "searchText": ""
            }
            # print(f"   [DEBUG] Payload: {final_payload}")
            
            response = session.post(search_url, json=final_payload, headers=search_headers)
            
            if response.status_code == 429:
                wait_time = int(response.headers.get("Retry-After", 30))
                print(f"Rate limit hit, sleeping for {wait_time}s")
                time.sleep(wait_time)
                continue
            
            if response.status_code == 200:
                if initial_run:
                    total_count = response.json().get('total',0)
                    initial_run = False
                    print(f"        [INFO] Total jobs found for {token}: {total_count}")
                raw_jobs = response.json().get('jobPostings', [])
                
                if not raw_jobs:
                    break
                
                for job in raw_jobs:
                    jobs.append(job)
                offset += limit
                if offset % 100 == 0:
                    print(f"        [PROGRESS] Processed {offset}/{total_count} jobs...")
                time.sleep(0.5)
            else:
                print(f"An error has occurred at offset {offset}: {response.status_code}")
    except Exception as e:
            print(f"Error scraping jobs at offset {offset} for token {token}: {e}")
            return jobs
    return jobs
def location_matcher(job, targets):
    targets = [loc.lower() for loc in targets]
    
    loc_text = job.get('location', '').lower()
    country_desc = job.get('country', {}).get('descriptor', '').lower()
    ext_path = job.get('externalUrl', '').lower()
    

    searchable_content = f"{loc_text} {country_desc} {ext_path}"
    if 'canada' in targets:
        if 'united kingdom' in searchable_content or 'london' in loc_text or 'uk' in ext_path:
            return False
            

    if 'united kingdom' in targets:
        if 'canada' in searchable_content or 'toronto' in loc_text or 'calgary' in loc_text:
            return False

    if any(t in searchable_content for t in targets):
        return True
        
    city_map = {
        'canada': ['calgary', 'toronto', 'montreal', 'vancouver', 'ottawa', 'alberta', 'ontario', 'quebec'],
        'united kingdom': ['london', 'manchester', 'edinburgh', 'reading', 'england', 'scotland']
    }
    
    for t in targets:
        if t in city_map:
            if any(city in searchable_content for city in city_map[t]):
                return True
                
    return False
def token_eater(token, dc, site, profiles):
    all_target_locs = set()
    matches = []
    for profile in profiles:
        for loc in profile.get('locations', []):
            all_target_locs.add(loc)
        
    #run the getter
    found_jobs = job_getter(
        token=token, 
        dc=dc, 
        site=site, 
        targets=list(all_target_locs)
    )
    if not found_jobs:
        print(f"Issue with found_jobs.")
        return []
    for job in found_jobs:
    
        job_id = str(job.get('bulletFields')).strip("[]' ")
        title = job.get('title')

        for profile in profiles:
            
            keywords = profile['keywords']
            location = profile['locations']
            loc_str = str(location).strip("[]' ")
            
            
            is_match, match_word = job_matching(keywords, title)
            search_flag = match_word 
            if is_match:
                ext_path = job.get('externalPath')
                
                base_url = f"https://{token}.{dc}.myworkdayjobs.com/wday/cxs/{site}"
                url = base_url + ext_path
                slug =  ext_path.rsplit('/', 1)[-1]
                extracted_fields = {
                    'company': token,
                    'job_id': job_id,
                    'job_title': title,
                    'location': loc_str,
                    'search_flag': search_flag,
                    'url': url,
                    'raw_path': ext_path,
                    'job_slug': slug,
                    'target_locations': location
                }
                matches.append(extracted_fields)
            else:
                pass
    return matches
def job_detail_getter(token, dc, site, datas):
    print(f"        > [INFO] Fetching details for {len(datas)} matched jobs...")
    session = requests.Session()
    session.headers.update({
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
        "Accept": "application/json"
    })
    
    # Return a dict: {slug: data}
    results = {}
    
    for d in datas:
        slug = d.get('job_slug')
        url = f"https://{token}.{dc}.myworkdayjobs.com/wday/cxs/{token}/{site}/job/{slug}"
        response = session.get(url)
        
        if response.status_code == 200:
            results[slug] = response.json()
        else:
            print(f"        > [WARN] Failed to fetch details for {slug} (Status: {response.status_code})")
            
        time.sleep(0.5)
    return results
def process_single_token(token, dc, site, profiles):
    company_blacklist = {"kraken.com", "kraken"}
    profiles = profiles
    token = token
    dc = dc
    site = site
    clean_jobs = []
    datas = token_eater(token,dc,site,profiles)
    if not datas:
        print(f"No matching jobs found in search for {token}")
        return []
    jobs = job_detail_getter(token, dc, site, datas)
    if not jobs:
        print(f"Failed to retrieve details for {len(datas)} jobs at {token}.")
    
    for d in datas:
        if token in company_blacklist:
            continue
        slug = d.get('job_slug')
        title = d.get('job_title')
        if slug not in jobs:
            print(f"[DROPPED] No details found for: {title} (Slug: {slug})")
            continue
        
        j = jobs[slug]
        if not isinstance(j, dict):
            continue
    
        
        target_locs = d.get('target_locations', [])

        detail_info = j.get('jobPostingInfo', {})
        if not location_matcher(detail_info, target_locs):
            print(f"        > [DROPPED] Location mismatch for: {title} (Target: {target_locs})")
            continue
        
        search_flag = d.get('search_flag')
        job = j.get('jobPostingInfo', {})
        if not job:
            print(f"        > [DROPPED] No jobPostingInfo for: {title}")
            continue
        
        job_id = str(token) + ":" + str(job.get('id'))
        title = job.get('title', "")
        country = job.get('country', {}).get('descriptor')
        is_remote = "remote" in job.get('remoteType', '').lower()
        date_posted = job.get('startDate')
        time_since, days_old, date_posted = date_handler(date_posted)
        if days_old is not None and days_old > 60:
            print(f"[DROPPED] Too old ({days_old} days): {title}")
            continue
        commitment = job.get('timeType')
        raw_content = job.get('jobDescription')
        clean_content = desc_cleanup(raw_content) or ""
        regex_pay = fix_pay(clean_content, country)
        salary_range = "Not given"
        salary_range_usd = "Not given"
        if regex_pay:
            min_v = regex_pay['min']
            max_v = regex_pay['max']
            currency = regex_pay['currency']
            salary_range = format_salary_range(min_v, max_v, currency)
            salary_range_usd = format_salary_range(min_v, max_v, currency, is_usd=True)
        url = job.get('externalUrl')

        extracted_fields = {
        "job_id": job_id,
        "job_title": title,
        "company": token,
        "location": country,
        "is_remote": is_remote,
        "date_posted": date_posted,
        "time_since_posted": time_since,
        "experience": "Not given",
        "employment_type": commitment,
        "salary_range": salary_range,
        "salary_range_usd": salary_range_usd,
        "url": url,
        "description": clean_content,
        "search_flag": search_flag,
        "last_scanned": datetime.now(timezone.utc)                              
        }
        clean_jobs.append(extracted_fields)
    return clean_jobs
def save_jobs_to_db(found_jobs):
    if not found_jobs:
        return
    
    ops = []
    for job in found_jobs:
        ops.append(UpdateOne(
            {'job_id': job['job_id']},
            {'$set': job},
            upsert=True
        ))
    
    try:
        collection.bulk_write(ops)
    except Exception as e:
        print(f"        > [ERROR] Failed to write to database: {e}")
def workday_jobs(batch_size=50):
    tokens = list(token_collection.find({
        'is_active': True,
        'failures': {'$lt': 3}
    }).sort('last_run',1).limit(batch_size))
    
    if not tokens:
        print("No tokens found")
        return
    ryan_keywords = ["cybersecurity", "siem", "splunk", "threat", "vulnerability", "security", "analytics engineer", "analytic", "incident", "risk", "junior software", "junior backend", "junior back end", "junior developer", "privacy", "cyber", "data analyst", "incident"]
    mik_keywords = ["frontend", "front end", "front-end", "vue", "product engineer", "web design", "web developer"]
    profiles = [
        {
            "name": "Ryan",
            "keywords": ryan_keywords,
            "locations": ["Canada"]
        },
        {
            "name": "Mik",
            "keywords": mik_keywords,
            "locations": ["United Kingdom"]
        }
    ]
    for i, t in enumerate(tokens):
        raw_tokens = t.get('token', '')
        try:
            token, dc, site = raw_tokens.split(':')
            found_jobs = process_single_token(token, dc, site, profiles)
            
            if found_jobs:
                print(f"Saved {len(found_jobs)} jobs for token {token}.")
                save_jobs_to_db(found_jobs)
                token_failures = 0
            else:
                # print(f"        > No matching jobs found for token {token}:")
                token_failures = 0
                
            token_collection.update_one({'_id': t['_id']}, 
                            {'$set': { 
                                'last_run': datetime.now(timezone.utc),
                                'failures': token_failures
                                }}
                            )
            sleep_time = random.uniform(3, 7) if random.random() > 0.1 else random.uniform(15, 30)
            time.sleep(sleep_time)
        except ValueError:
            print(f"        > [ERROR] Malformed token string: '{raw_tokens}'. Expected 'token:dc:site'")
            continue
        except Exception as e:
            print(f"        > [ERROR] Failed {raw_tokens}: {e}")
            token_collection.update_one({'_id': t['_id']}, 
                                        {'$inc': {'failures': 1},
                                        '$set': { 'last_run': datetime.now(timezone.utc)}
                                        })
            continue
        if (i + 1) % 5 == 0:
            sleep_time = random.uniform(120,300)
            print(f"Mini-batch complete. Cooling down for {int(sleep_time)/60} minutes...")
            

if __name__ == "__main__":
    workday_jobs(50)
        