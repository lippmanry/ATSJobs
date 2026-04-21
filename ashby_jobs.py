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
token_collection = db['ashby_tokens']
collection = db['ashby_jobs']

def country_handler(job):
    address =  job.get('address') or {}
    postal = address.get('postalAddress') or {}
    country = postal.get('addressCountry')
    
    if country and country.strip():
        return country.strip()
    loc_string = job.get('location', '')
    
    return country if (country and country.strip()) else (loc_string or "Not given")

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
def process_single_token(token, session, ryan_loc, mik_loc, ryan_keywords, mik_keywords):
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
            
                title = job.get('title', '').lower()
                workplace = job.get('workplaceType') or ""
                is_remote = True if "remote" in workplace.lower() or job.get('isRemote') == True else False
                # date_posted = job.get('publishedAt')
                

                company = token.lower().strip()
                job_id = str(company) + ":" + str(job.get('id'))
                
                #country handler
                location = country_handler(job)
                
                #locations
                primary_loc = job.get('location', '').lower()
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
                
                

                
                time_since, date_posted = date_handler(job.get('publishedAt'))
                
                desc = job.get('descriptionPlain', '')
                raw_content = f"{title} {desc}"
                clean_content = desc_cleanup(raw_content) or ""
                searchable_content = clean_content.lower()
                
                #job categories
                team = (job.get('team') or "").lower()
                dept = (job.get('department') or "").lower()
                depts = [d for d in [team, dept] if d]
                
                #validators
                
                #ryan's
                ryan_loc_check = location_validator(ryan_loc, all_loc_strings)
                ryan_target_in_soup = any(any(t in loc_str for t in ryan_loc) for loc_str in all_loc_strings)
                ryan_match_word = next((k for k in ryan_keywords if k in title or k in searchable_content or any(d and k in d for d in depts)), None)
                ryan_key_match = any(k in title or k in searchable_content or any(d and k in d for d in depts) for k in ryan_keywords)
                
                #mik's
                mik_loc_check = location_validator(mik_loc, all_loc_strings)
                mik_target_in_soup = any(any(t in loc_str for t in mik_loc) for loc_str in all_loc_strings)
                mik_match_word = next((k for k in mik_keywords if k in title or k in searchable_content or any(d and k in d for d in depts)), None)
                mik_key_match = any(k in title or k in searchable_content or any(d and k in d for d in depts) for k in mik_keywords)
                
                ryan_match = ryan_key_match and (ryan_loc_check or (is_remote and ryan_target_in_soup))
                mik_match = mik_key_match and (mik_loc_check or (is_remote and mik_target_in_soup))
                
                if ryan_match or mik_match:
                    try:
                        time_val = int(time_since) if time_since is not None else None
                    except (ValueError, TypeError):
                        time_val = None
                    if time_val is not None and time_val > 45:
                        continue
                    all_locations = ", ".join([loc.title() for loc in all_loc_strings])
                                        
                    desc = job.get('descriptionPlain', '')
                    raw_content = f"{title} {desc}"
                    clean_content = desc_cleanup(raw_content) or ""
                    searchable_content = clean_content.lower()
                    
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
                    
                    search_flag = ryan_match_word if ryan_match else mik_match_word
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
    
    ryan_keywords = ["cybersecurity", "siem", "splunk", "threat", "vulnerability", "security engineer", "security analyst"]
    mik_keywords = ["frontend", "frontend developer", "front-end", "vue", "product engineer"]
    
    ryan_loc = ["canada", "ontario"]
    mik_loc = ["united kingdom", "uk", "gb"]
    
    with ThreadPoolExecutor(max_workers=5) as executor:
        worker_func = partial(process_single_token, session=session, ryan_loc=ryan_loc, mik_loc=mik_loc, ryan_keywords=ryan_keywords, mik_keywords=mik_keywords)
        
        results = list(executor.map(worker_func, token_data))
        print(f"Total jobs saved across all tokens: {sum(results)}")

if __name__ == "__main__":
    ashby_jobs()