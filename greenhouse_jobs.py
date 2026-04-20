#company search
import re
import time
import logging
from datetime import datetime, timezone
import random
import requests
from hdx.location.country import Country
from hdx.location.currency import Currency
from dateutil import parser
from bs4 import BeautifulSoup
from dotenv import load_dotenv
from pymongo import MongoClient, UpdateOne
from datetime import datetime
import os
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry


#inits
load_dotenv(override=True)
Currency.setup(fallback_historic_to_current=True, fallback_current_to_static=True, log_level=logging.INFO)

mongo_uri = os.getenv('MONGO_URI')
client = MongoClient(mongo_uri)
db = client['all_jobs']        
token_collection = db['greenhouse_tokens']
collection = db["greenhouse_jobs"]

#format to usd for lazy americans like me
def format_usd(val, currency):
    if isinstance(val, (int, float)) and currency:
        try:
            usd_val = Currency.get_current_value_in_usd(val, currency)
            
            if usd_val is not None:
                return f'{usd_val:,.2f}'
        
        except Exception:
            pass
        
    return 'Not given'

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
#use the country to change the currency instead of defaulting to USD
def get_currency(country_input):
    try:
        country_holder = Country.get_iso3_country_code_fuzzy(country_input)
        iso_code, is_valid = country_holder
        currency_code = Country.get_currency_from_iso3(iso_code)
        
        return currency_code if currency_code else 'USD'
    except Exception:
        pass

#grab all ranges and list the lowest and highest for min and max instead of listing the first number twice
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
#compensation range
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
def job_detail_getter(token, job_id, headers):
    detail_url = f"https://boards-api.greenhouse.io/v1/boards/{token}/jobs/{job_id}?pay_transparency=true"
    try:
        response = requests.get(detail_url, headers=headers, timeout=30)
        if response.status_code == 200:
            return response.json()
    except Exception as e:
        print(f"Error getting details for {token} job with ID {job_id}: {e}")
    return None

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

def remote_checker(loc_list):    
    if isinstance(loc_list, str):
        loc_list = [loc_list]
    
    for loc in loc_list:
        if not loc: continue
    
        loc_lower = loc.lower()
            
        if 'remote' in loc_lower or 'distributed' in loc_lower:
            if 'non-remote' not in loc_lower:
                return True
    return False

def date_handler(posted_date):
    if not posted_date:
        return 'Not given', float('inf')
    try:
        posted_date = parser.isoparse(posted_date)
        
        if posted_date.tzinfo is None:
            posted_date = posted_date.replace(tzinfo=timezone.utc)
    
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

    return time_since

def greenhouse_jobs():
    total_saved = 0
    #pull healthy tokens from mongo
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
    
    tokens = [t.get('token') for t in active_tokens if t.get('token')]
    

    
    ryan_keywords = ["cybersecurity", "siem", "splunk", "threat", "vulnerability", "security engineer", "security analyst"]
    mik_keywords = ["frontend", "frontend developer", "front-end", "vue", "product engineer"]
    
    ryan_loc = ["canada", "ontario"]
    mik_loc = ["united kingdom", "uk", "gb"]
    
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36"
    }
    
    for token in tokens:
        job_list = []
        api_url = f"https://boards-api.greenhouse.io/v1/boards/{token}/jobs?content=true"
        
        try:
            response = session.get(api_url, timeout=(10,90))
            
            #rate limit handler
            if response.status_code == 429:
                wait_time = int(response.headers.get("Retry-After", 30))
                print(f"Rate limit hit, sleeping for {wait_time}s")
                time.sleep(wait_time)
                continue
            
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
                
                for job in jobs:
                    #searchable text
                    title = job.get('title', '').lower()
                    raw_content = job.get('content', '')
                    clean_content = desc_cleanup(raw_content) or ""
                    searchable_content = clean_content.lower()
                    depts = [d.get('name', '').lower() for d in job.get('departments',[]) if d.get('name')]
                    id = str(token) + ":" + str(job.get('id'))
                
                    
                    onsite_name = job.get('location', {}).get('name') or ""
                    onsite_req = onsite_name.strip().lower()
                    metadata_list = job.get('metadata') or []
                    remote_metas = next((str(m.get('value', '')).lower() for m in metadata_list if m.get('id') == 7742247003), "")
                    is_remote = remote_checker([onsite_req, remote_metas])
                    
                    meta_locs = []
                    for m in metadata_list:
                        m_name = m.get('name', '')
                        m_val = m.get('value')
                        
                        if 'Location' in m_name and m_val:
                            if isinstance(m_val, list):
                                meta_locs.extend([str(v).strip() for v in m_val if v and str(v).strip()])
                            else:
                                val_str = str(m_val).strip()
                                if val_str:
                                    meta_locs.append(str(m_val))
                    all_loc_strings = list(set([onsite_req] + [loc.lower().strip() for loc in meta_locs]))
                    
                    display_location = ", ".join(meta_locs) if meta_locs else job.get('location', {}).get('name', "Not given")
                    
                    ryan_loc_check = location_validator(ryan_loc, all_loc_strings)
                    mik_loc_check = location_validator(mik_loc, all_loc_strings)
                    
                    ryan_match_word = next((k for k in ryan_keywords if k in title or k in searchable_content or any(d and k in d for d in depts)), None)
                    ryan_key_match = any(k in title or k in searchable_content or any(d and k in d for d in depts) for k in ryan_keywords)

                    
                    mik_match_word = next((k for k in mik_keywords if k in title or k in searchable_content or any(d and k in d for d in depts)), None)
                    mik_key_match = any(k in title or k in searchable_content or any(d and k in d for d in depts) for k in mik_keywords)
                    
                    ryan_match = ryan_key_match and ryan_loc_check
                    mik_match = mik_key_match and mik_loc_check
                    
                    if ryan_match or mik_match:
                        job_id = job.get('id')
                        details = job_detail_getter(token, job_id, headers)
                        if not details:
                            continue
                        updated_at = details.get('updated_at')
                        updated_dt = parser.isoparse(updated_at)
                        if updated_dt.tzinfo is None:
                            updated_dt = updated_dt.replace(tzinfo=timezone.utc)
                        days_old = (datetime.now(timezone.utc) - updated_dt).days
                        if days_old > 45:
                            continue
                        
                        time_since = date_handler(updated_at)
                        salary_range = "Not given"
                        salary_range_usd = "Not given"
                        country = "canada" if ryan_match else "uk"
                        
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
                            'date_posted': updated_at,
                            'time_since_posted': time_since,
                            "experience": "Not given",
                            "employment_type": "Not given",
                            "salary_range": salary_range,
                            "salary_range_usd": salary_range_usd,
                            "url": job.get('absolute_url'),
                            "description": clean_content,
                            "search_flag": ryan_match_word if ryan_match else mik_match_word,
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
                
        except requests.exceptions.Timeout:
            print(f"Timeout occurred for token {token}. Skipping...")
        except Exception as e:
            import traceback
            print(f"An error has occurred for token {token}: {e}")
            traceback.print_exc()

if __name__ == "__main__":
    greenhouse_jobs()