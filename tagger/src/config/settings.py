import os
from dotenv import load_dotenv


# MongoDB ayarları
load_dotenv()
MONGO_URI = os.getenv('MONGO_URI')
MONGO_DB_NAME = os.getenv('MONGO_DB_NAME')
PROCESSED_COLLECTION_SPIDER = os.getenv('PROCESSED_COLLECTION_SPIDER')
MONGO_NOT_FOUND_URLS_COLL = os.getenv('MONGO_NOT_FOUND_URLS_COLL')
MONGO_WHOIS_ERROR_URLS_COLL = os.getenv('MONGO_WHOIS_ERROR_URLS_COLL')
PROCESSED_SITES_SEO = os.getenv('PROCESSED_SITES_SEO')
PROCESSED_SITES_SEO_LINKS = os.getenv('PROCESSED_SITES_SEO_LINKS')
UNPROCESSABLE_SITES = os.getenv('UNPROCESSABLE_SITES')

#Diğer sabitler
SLEEP_DELAY = 5
RETRY_ATTEMPTS = 3
USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36 Edg/91.0.864.59",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10.15; rv:90.0) Gecko/20100101 Firefox/90.0",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36 Edge/91.0.864.59"
]

COOKIES = [
    {"name": "time_zone", "value": "GMT+3", "domain": "example.com", "path": "/"},
    {"name": "daily_visit_count", "value": "5", "domain": "example.com", "path": "/"},
    {"name": "last_purchase", "value": "2024-11-01", "domain": "example.com", "path": "/"},
    {"name": "device_type", "value": "mobile", "domain": "example.com", "path": "/"},
    {"name": "trial_expiry", "value": "2024-12-01", "domain": "example.com", "path": "/"},
    {"name": "visited_tutorial", "value": "true", "domain": "example.com", "path": "/"},
    {"name": "notification_preference", "value": "email", "domain": "example.com", "path": "/"},
    {"name": "region", "value": "North_America", "domain": "example.com", "path": "/"},
    {"name": "vip_status", "value": "gold", "domain": "example.com", "path": "/"},
    {"name": "referral_code", "value": "XYZ1234", "domain": "example.com", "path": "/"}
]