#both ddg and google - WORKABLE
from utils import save_tokens_mongo, ddg_token_search
import re
import time
import logging
from datetime import datetime, timezone, timedelta
import random
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.options import Options
from webdriver_manager.chrome import ChromeDriverManager
import requests
import html
from hdx.location.country import Country
from hdx.location.currency import Currency
from dateutil import parser
from bs4 import BeautifulSoup
from dotenv import load_dotenv
from pymongo import MongoClient, UpdateOne
import os
from fake_useragent import UserAgent
from urllib.parse import quote_plus
from ddgs import DDGS
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from concurrent.futures import ThreadPoolExecutor
from functools import partial

load_dotenv(override=True)
mongo_uri = os.getenv('MONGO_URI')
client = MongoClient(mongo_uri)
db = client['all_jobs']
token_collection = db['workable_tokens']

"""
DDG TOKEN COLLECTION - WORKABLE
"""
def workable_ddg_tokens():
    anchor_words = ["Analyst", "Developer", "Manager", "Operations", "IT", "Engineer", "Compliance", "Staff", "Information Technology", "Analytics", "Security"]
    locations = ["Canada", "United Kingdom", "UK", "Global", "Remote", "EMEA", "North America"]
    
    successful_runs = 0
    new_tokens = set()
    
    while successful_runs < 3:
        new_found ={}
        word = random.choice(anchor_words)
        location = random.choice(locations)

        query_bits = [p for p in [word, location] if p]
        query = " ".join(query_bits)
        site = 'apply.workable.com'
    
        new_found = ddg_token_search(site, query)
    
        if new_found:
            save_tokens_mongo(new_found,token_collection)
            new_tokens.update(new_found)
            successful_runs += 1
            print(f"{len(new_found)} new tokens found on [{datetime.now()}]: {new_found}")
        else:
            print(f"No new tokens found found for {query}. Retrying...")
        wait_time = random.uniform(15,30)
        print(f"Sleeping for {round(wait_time, 2)}s...")
        time.sleep(wait_time)
    print(f"Total unique tokens found: {len(new_tokens)}")
    return list(new_tokens)

if __name__ == "__main__":
    workable_ddg_tokens()