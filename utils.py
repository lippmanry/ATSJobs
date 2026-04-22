import re
import logging
from datetime import datetime, timezone
import requests
from hdx.location.country import Country
from hdx.location.currency import Currency
from dateutil import parser
from bs4 import BeautifulSoup
from dotenv import load_dotenv



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
    if any(t in combined for t in targets):
        return True
    
    #remote only with targets
    if combined.strip() == "remote":
        return True
    
    return False

#handle various date formats
def date_handler(posted_date):
    if not posted_date:
        return 'Not given', None
    try:
        if isinstance(posted_date, (int, float)):
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
        return 'Unknown', None

    return time_since, posted_date

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