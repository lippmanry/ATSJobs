
#company search
import re
import time
import logging
from datetime import datetime, timezone, timedelta
import random
import requests
from hdx.location.country import Country
from hdx.location.currency import Currency
from dateutil import parser
from bs4 import BeautifulSoup
from dotenv import load_dotenv
from pymongo import MongoClient, UpdateOne
import os
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from concurrent.futures import ThreadPoolExecutor
from functools import partial



#inits
load_dotenv(override=True)
Currency.setup(fallback_historic_to_current=True, fallback_current_to_static=True, log_level=logging.INFO)


mongo_uri = os.getenv('MONGO_URI')
client = MongoClient(mongo_uri)
db = client['all_jobs']    
token_collection = db['lever_tokens']
collection = db['lever_jobs']

def get_currency(country_input):
    try:
        country_holder = Country.get_iso3_country_code_fuzzy(country_input)
        iso_code, is_valid = country_holder
        currency_code = Country.get_currency_from_iso3(iso_code)
        
        return currency_code if currency_code else 'USD'
    except Exception:
        pass
def fix_pay(html_desc, country_input):
    target_currency = get_currency(country_input)
    
    #make soups
    soup = BeautifulSoup(html_desc, 'html.parser')
    text_content = soup.get_text(separator=' ')
    
    #begin fix
    lower_text = text_content.lower()
    keyword_index = lower_text.rfind('salary')
    if keyword_index == -1:
        keyword_index = lower_text.rfind('compensation')
    if keyword_index != -1:
        search_window = text_content[keyword_index : keyword_index + 200]
    else:
        search_window = text_content
    #end fix
    
    #match all numbers in the ranges
    number_pattern = r'\$\s*([\d,]+)'
    
    match = re.findall(number_pattern, search_window)
    if not match:
        return None
    
    all_values =[float(val.replace(',', '')) for val in match]
    salary_values = [v for v in all_values if v > 10000]
    if not salary_values:
        return None
    
    min_pay = min(salary_values)
    max_pay = max(salary_values)
    
    return {
        'min': min_pay,
        'max': max_pay,
        'currency': target_currency
    }
def date_handler(posted_date):
    if not posted_date:
        return 'Not given', float('inf')
    try:
        if isinstance(posted_date, (int, float)):
            posted_date = datetime.fromtimestamp(posted_date / 1000, tz=timezone.utc)
        else:
            posted_date = parser.isoparse(posted_date)
            if posted_date.tzinfo is None:
                posted_date = posted_date.replace(tzinfo=timezone.utc)
        iso_string = posted_date.isoformat(timespec='seconds')
        current_date = datetime.now(timezone.utc)
        delta = current_date - posted_date
        total_seconds = max(0, delta.total_seconds())

        if total_seconds < 3600:
            #less than 1 hr
            minutes = int(total_seconds // 60)
            time_since = f'{minutes} min ago'
        elif total_seconds < 86400:
            #less than 1 day
            hours = int(total_seconds // 3600)
            time_since = f'{hours} hours ago'
        else:
            #more than 1 day
            time_since = f'{delta.days} days ago'
        

    except Exception as e:
        print(f'Error with date: {e}')
        return 'Unknown', float('inf')

    return time_since, iso_string
def format_usd(val, currency):
    if isinstance(val, (int, float)) and currency:
        try:
            usd_val = Currency.get_current_value_in_usd(val, currency)
            
            if usd_val is not None:
                return f'{usd_val:,.2f}'
        
        except Exception:
            pass
        
    return 'Not given'
def format_salary_range(min_val, max_val, currency, is_usd=False):
    if is_usd:
        s_min = format_usd(min_val, currency)
        s_max = format_usd(max_val, currency)
        suffix = 'USD'
    else:
        s_min = f'{min_val:,.2f}' if isinstance(min_val, (int, float)) else 'Not given'
        s_max = f'{max_val:,.2f}' if isinstance(max_val, (int, float)) else 'Not given'
        suffix = currency
    
    if s_min == 'Not given' and s_max == 'Not given':
        return 'Not given'
    return f'{s_min} - {s_max} {suffix}'
def build_lever_url(token, region):
    base = "api.eu.lever.co" if region == "eu" else "api.lever.co"
    base_url = f"https://{base}/v0/postings/{token}"
    return base_url
#cleanup job description
def desc_cleanup(content):
    if not content or not isinstance(content, str):
        return 'Not given'
    try:
        soup = BeautifulSoup(content, 'html.parser')
        #we don't care about the headers for this. it's parsed elsewhere and could be a job or company description
        for junk in soup(['script', 'style','h1','h2','h3','h4','h5','h6']):
            junk.decompose()
            
        text = soup.get_text(separator=' ')
        clean_text = re.sub(r'<[^>]+>', ' ', text)
        
        words = clean_text.split()
        final_string = ' '.join(words)
        
        return final_string if final_string else 'Not given'
        
    except Exception as e:
        return f'Error parsing html description: {e}'
    
def location_validator(targets, strings):
    
    combined = " ".join(strings).lower()
    
    #global position checker
    if "global" in combined:
        return True
    
    #specific countries
    if any(t in combined for t in targets):
        return True
    
    #remote only with targets
    if combined.strip() == "remote":
        return True
    
    return False


def process_single_token(item, session, ryan_loc, mik_loc, ryan_keywords, mik_keywords):
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
                clean_content = desc_cleanup(raw_content) or ""
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
                
                #ryan's
                ryan_loc_check = location_validator(ryan_loc, all_loc_strings)
                ryan_target_in_soup = any(any(t in loc_str for t in ryan_loc) for loc_str in all_loc_strings)
                
                mik_loc_check = location_validator(mik_loc, all_loc_strings)
                mik_target_in_soup = any(any(t in loc_str for t in mik_loc) for loc_str in all_loc_strings)
                
                ryan_match_word = next((k for k in ryan_keywords if k in title or k in searchable_content or any(d and k in d for d in depts)), None)
                ryan_key_match = any(k in title or k in searchable_content or any(d and k in d for d in depts) for k in ryan_keywords)


                #mik's
                mik_match_word = next((k for k in mik_keywords if k in title or k in searchable_content or any(d and k in d for d in depts)), None)
                mik_key_match = any(k in title or k in searchable_content or any(d and k in d for d in depts) for k in mik_keywords)

                ryan_match= ryan_key_match and (ryan_loc_check or (is_remote and ryan_target_in_soup))

                mik_match = mik_key_match and (mik_loc_check or (is_remote and mik_target_in_soup))
                
                if ryan_match or mik_match:

                    #check posting age, filter out old posts
                    created_at_ms = job.get('createdAt')
                    if created_at_ms:
                        created_dt = datetime.fromtimestamp(created_at_ms / 1000.0, tz=timezone.utc)
                        if (datetime.now(timezone.utc) - created_dt).days > 45:
                            continue
                        
                        
                        time_since, date_posted = date_handler(job.get('createdAt'))
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
                    search_flag = ryan_match_word if ryan_match else mik_match_word    
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
def location_validator(targets, strings):
    
    combined = " ".join(strings).lower()
    
    #global position checker
    if "global" in combined:
        return True
    
    #specific countries
    if any(t in combined for t in targets):
        return True
    
    #remote only with targets
    if combined.strip() == "remote":
        return True
    
    return False

def lever_jobs():
    
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
    token_data = [{
        'token': t.get('token'),
        'region': t.get('region')
        }
        for t in active_tokens if t.get('token')
    ]
    ryan_keywords = ["cybersecurity", "siem", "splunk", "threat", "vulnerability", "security engineer", "security analyst"]
    mik_keywords = ["frontend", "frontend developer", "front-end", "vue", "product engineer"]
    
    ryan_loc = ["canada", "ontario"]
    mik_loc = ["united kingdom", "uk", "gb"]
    
    with ThreadPoolExecutor(max_workers=5) as executor:
        worker_func = partial(process_single_token, session=session, ryan_loc=ryan_loc, mik_loc=mik_loc, ryan_keywords=ryan_keywords, mik_keywords=mik_keywords)
        
        results = list(executor.map(worker_func, token_data))
        print(f"Total jobs saved across all tokens: {sum(results)}")
                
if __name__ == "__main__":
    lever_jobs()        