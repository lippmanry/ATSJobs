import re
import logging
from datetime import datetime, timezone
import requests
from hdx.location.country import Country
from hdx.location.currency import Currency
from dateutil import parser
from bs4 import BeautifulSoup
from dotenv import load_dotenv
from pymongo import MongoClient, UpdateOne
from ddgs import DDGS
import pandas as pd



#inits
load_dotenv(override=True)
Currency.setup(fallback_historic_to_current=True, fallback_current_to_static=True, log_level=logging.INFO)

#currency by country
def get_currency(country_input):
    try:
        country_holder = Country.get_iso3_country_code_fuzzy(country_input)
        iso_code, is_valid = country_holder
        currency_code = Country.get_currency_from_iso3(iso_code)
        
        return currency_code if currency_code else 'USD'
    except Exception:
        pass

#check desc for pay information if not given
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

#format salary ranges
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
    for t in targets:
        pattern = rf"\b{re.escape(t)}\b"
        if re.search(pattern, combined):
            return True
    
    #remote only with targets
    if combined.strip() == "remote":
        return True
    
    return False

#handle various date formats
#handle various date formats
def date_handler(posted_date):
    if not posted_date:
        return 'Not given', None
    try:
        if isinstance(posted_date, datetime):
            pass
        elif isinstance(posted_date, (int, float)):
            posted_date = datetime.fromtimestamp(posted_date / 1000, tz=timezone.utc)
        else:
            posted_date = parser.isoparse(posted_date)
            
        if posted_date.tzinfo is None:
            posted_date = posted_date.replace(tzinfo=timezone.utc)
        else:
            posted_date = posted_date.astimezone(timezone.utc)
            

        current_date = datetime.now(timezone.utc)
        delta = current_date - posted_date
        total_seconds = max(0, delta.total_seconds())
        
        #raw int for match handling and stale posts
        days_int = delta.days
        
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
        
        posted_date = posted_date.isoformat()
    except Exception as e:
        print(f'Error with date: {e}')
        return 'Unknown', None, None

    return time_since, days_int, posted_date

#greenhouse specific (for now) detail getter
def job_detail_getter(token, job_id, headers):
    detail_url = f"https://boards-api.greenhouse.io/v1/boards/{token}/jobs/{job_id}?pay_transparency=true"
    try:
        response = requests.get(detail_url, headers=headers, timeout=30)
        if response.status_code == 200:
            return response.json()
    except Exception as e:
        print(f"Error getting details for {token} job with ID {job_id}: {e}")
    return None

#for apis without a specific remote flag
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

#ashby specific country handler
def country_handler(job):
    address =  job.get('address') or {}
    postal = address.get('postalAddress') or {}
    country = postal.get('addressCountry')
    
    if country and country.strip():
        return country.strip()
    loc_string = job.get('location', '')
    
    return country if (country and country.strip()) else (loc_string or "Not given")

#lever url builder
def build_lever_url(token, region):
    base = "api.eu.lever.co" if region == "eu" else "api.lever.co"
    base_url = f"https://{base}/v0/postings/{token}"
    return base_url

#mongo token saver
def save_tokens_mongo(tokens, token_collection):
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

#reusable token search for DDG
def ddg_token_search(site, keyword):
    tokens = set()
    query = f'site:{site} {keyword}'
    print(f"Searching DDG for {query}")
    
    system_blacklist = {'embed', 'search', 'v1', 'd', 'api', 'js', 'widgets', 'careers'}
    pattern = rf"{re.escape(site)}/([^/&?#]+)"
    
    with DDGS() as ddgs:
        results = ddgs.text(query, max_results=20)
        for r in results:
            url = r['href']
            match = re.search(pattern, url)
            if match:
                token = match.group(1).lower()
                if token not in system_blacklist:
                    tokens.add(token)
    return list(tokens)
def salary_handler(text, country):
    min_v = max_v = currency = None
    salary_range = salary_range_usd = "Not given"
    regex_pay = fix_pay(text, country)
    if regex_pay:
        min_v = regex_pay['min']
        max_v = regex_pay['max']
        currency = regex_pay['currency']        
        salary_range = format_salary_range(min_v, max_v, currency)
        salary_range_usd = format_salary_range(min_v, max_v, currency, is_usd=True)
    
    return salary_range, salary_range_usd
#reusable job matching function
def job_matching(target_locs, target_keywords, all_loc_strings, title, depts, is_remote,content=None):
    target_locs = list(target_locs) if target_locs else []
    target_keywords = list(target_keywords) if target_keywords else []
    all_loc_strings = list(all_loc_strings) if all_loc_strings else []
    search_depts = [str(d).lower() for d in depts if d] if depts else []
    title_lower = str(title).lower() if title else ""
    content_lower = str(content).lower() if content else ""
    
    #blacklist some word combos
    blacklist = ["security guard", "director", "mechanical design", "electrical design", "intern", "electronics design", "cs&a design"]
    for word in blacklist:
        pattern = rf"\b{re.escape(word)}\b"
        if re.search(pattern, title_lower):
            return False, None
    
    #check title first
    title_match = next((k for k in target_keywords if k in title_lower)), None)
    
    content_match = None
    if content and not title_match:
        content_lower = str(content).lower() 
        content_match = next((k for k in target_keywords if k in content_lower), None)
    
    match_word = title_match or content_match
    if not match_word:
        return False, None
    
    #check targeted locations
    loc_check = location_validator(target_locs, all_loc_strings)
    
    target_in_soup = False
    for loc_str in all_loc_strings:
        loc_lower = loc_str.lower()
        if any(re.search(rf"\b{re.escape(t.lower())}\b", loc_lower) for t in target_locs):
            target_in_soup = True
            break
    
    is_match = False
        #check matches
    if title_match:
            if loc_check or (is_remote and target_in_soup):
                is_match = True
    elif content_match:
        if loc_check:
            is_match = True

    return is_match, match_word

#for streamlit - adjusting dates to display properly, not by when the script ran
def display_date_helper(ts):
    if pd.isna(ts): return "Unknown"
    now = pd.Timestamp.now(tz='UTC')
    diff = now - ts
    
    if diff.total_seconds() < 3600:
        return f"{int(diff.total_seconds() // 60)} min ago"
    elif diff.total_seconds() < 86400:
        return f"{int(diff.total_seconds() // 3600)} hours ago"
    else:
        return f"{diff.days} days ago"

#streamlit load and label helper    
def load_and_label(db, collection_name, source_label):
    collection = db[collection_name]
    data = list(collection.find({}, {"_id": 0}))
    df = pd.DataFrame(data)
    
    if not df.empty:
        df['source'] = source_label
        
    return df
