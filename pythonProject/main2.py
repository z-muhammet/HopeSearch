import os
import unicodedata
import requests
import random
import time
import json
import re
import ssl
import socket
import multiprocessing
from datetime import datetime, timedelta
from urllib.parse import urlparse, urljoin
import cloudscraper
from bs4 import BeautifulSoup
import whois
from dateutil import parser
from dotenv import load_dotenv
from playwright.sync_api import sync_playwright
from mongo_db_context import MongoDbContext
from queue import Queue
from concurrent.futures import ThreadPoolExecutor
import urllib3
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)


SLEEP_DELAY = 5
RETRY_ATTEMPTS = 5

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
not_found_urls = []
whois_error_urls = []

def ensure_url_scheme(url):

    return url if url.startswith(('http://', 'https://')) else 'http://' + url

def clean_text(text):
    # Noktalama işaretlerini boşlukla değiştir (virgül, nokta, tire gibi)
    text = re.sub(r"[.,;:!?()\"“”’‘\[\]{}<>|/\\]", " ", text)

    # Unicode karakterleri normalize et (örn. İ → i, ü → u gibi)
    text = unicodedata.normalize("NFKD", text)

    # Gereksiz sembolleri kaldır, sadece harf, rakam ve boşluk bırak
    text = re.sub(r"[^a-zA-Z0-9çÇğĞıİöÖşŞüÜ ]", "", text)

    # Birden fazla boşluğu tek boşluğa indir
    text = re.sub(r"\s+", " ", text)

    return text.strip()

def decode_response_content(response):

    try:
        return response.content.decode(response.apparent_encoding)
    except Exception as e:
        print(f"Error decoding content: {e}")
        return response.text


def get_random_content(content, length=300):
    cleaned = clean_text(content)
    words = cleaned.split()

    if not words:
        return ""

    # Karıştır
    random.shuffle(words)

    snippet = []
    current_length = 0
    for word in words:
        if current_length + len(word) + 1 > length:
            break
        snippet.append(word)
        current_length += len(word) + 1

    return ",".join(snippet)

def has_ssl_certificate(url):

    parsed = urlparse(url)
    host = parsed.hostname
    port = 443
    try:
        context = ssl.create_default_context()
        with socket.create_connection((host, port)) as conn:
            with context.wrap_socket(conn, server_hostname=host) as ssock:
                ssock.getpeercert()
                return True
    except Exception:
        return False


def get_dynamic_thread_count():

    return max(2, multiprocessing.cpu_count() // 2)


def get_site_age(url):

    try:
        parsed = urlparse(url)
        domain = parsed.netloc
        domain_info = whois.whois(domain)
        creation_date = domain_info.creation_date
        if isinstance(creation_date, list):
            creation_date = creation_date[0]
        if isinstance(creation_date, str):
            creation_date = parser.parse(creation_date)
        if isinstance(creation_date, datetime):
            return datetime.now().year - creation_date.year
        print("Creation date is not available or not in expected format.")
    except Exception as e:
        print(f"Error getting site age for {url}: {e}")
    return None
def get_last_updated_date(url, response=None, soup=None):
    url = ensure_url_scheme(url)
    headers = {
        'User-Agent': random.choice(USER_AGENTS),
        'Accept-Language': 'en-US,en;q=0.9',
        'Connection': 'keep-alive'
    }

    if response is None:
        try:
            response = requests.get(url, timeout=45, headers=headers,verify=False)
            print(f"HTTP Response for {url}: {response.status_code}")
            response.raise_for_status()
        except requests.exceptions.RequestException as e:
            print(f"Error fetching last updated date for {url}: {e}")
            return {"year": None, "month": None}

    if soup is None:
        try:
            soup = BeautifulSoup(response.content.decode('utf-8', 'ignore'), "html.parser")
        except Exception as e:
            print(f"Error creating soup for {url}: {e}")
            return {"year": None, "month": None}

    #time etiketlerinden tarih arama
    time_elements = soup.find_all('time')
    for time_el in time_elements:
        if time_el.has_attr('datetime'):
            date = parse_date(time_el['datetime'])
            if date:
                return {"year": date.year, "month": date.month}

    #Microdata / RDFa kontrolü
    for itemprop in ['dateModified', 'datePublished']:
        element = soup.find(attrs={"itemprop": itemprop})
        if element:
            date_str = element.get("content") or element.get_text()
            date = parse_date(date_str)
            if date:
                return {"year": date.year, "month": date.month}

    #meta etiketlerinden tarih arama
    meta_tags = [
        {'name': 'last-modified'},
        {'property': 'og:updated_time'},
        {'property': 'article:modified_time'},
        {'name': 'datePublished'},
        {'name': 'dateModified'},
        {'name': 'revised'},
        {'name': 'guncellenme_tarihi'},
        {'name': 'olusturulma_tarihi'},
        {'name': 'yayimlanma_tarihi'},
        {'name': 'published_time'},
        {'name': 'modified_time'},
        {'property': 'og:published_time'},
        {'name': 'son_guncelleme'},
        {'name': 'haber_guncellenme'},
        {'name': 'dc.date.modified'},
        {'name': 'dc.date.created'},
        {'name': 'article:published_time'},
        {'name': 'lastupdate'},
        {'name': 'revision_date'},
        {'name': 'son_duzenleme_tarihi'}
    ]

    dates = []
    for meta_tag in meta_tags:
        meta_element = soup.find('meta', meta_tag)
        if meta_element and 'content' in meta_element.attrs:
            date_str = meta_element['content']
            date = parse_date(date_str)
            if date:
                dates.append(date)

    #JSON-LD içinde tarih arama
    json_ld_scripts = soup.find_all("script", type="application/ld+json")
    for script in json_ld_scripts:
        try:
            data = json.loads(script.string)
            if isinstance(data, dict):
                for key in ["dateModified", "datePublished"]:
                    if key in data:
                        date = parse_date(data[key])
                        if date:
                            dates.append(date)
        except json.JSONDecodeError:
            continue

    #sayfa metninde arama
    possible_texts = ["son güncelleme", "last updated", "last modified", "güncellendi"]
    for text in possible_texts:
        found_text = soup.find(string=re.compile(text, re.IGNORECASE))
        if found_text:
            date_match = re.search(r'\b(\d{1,2}[./-]\d{1,2}[./-]\d{4})\b', found_text)
            if date_match:
                date_str = date_match.group(1)
                date = parse_date(date_str)
                if date:
                    dates.append(date)

    # script etiketleri içinde
    script_tags = soup.find_all('script')
    date_pattern = re.compile(r'\b(\d{1,2}[./-]\d{1,2}[./-]\d{4})\b')
    for script in script_tags:
        if script.string:
            for match in date_pattern.findall(script.string):
                date = parse_date(match)
                if date:
                    dates.append(date)

    #HTTP Headers'dan Last-Modified kontrolü
    if 'Last-Modified' in response.headers:
        date = parse_date(response.headers['Last-Modified'])
        if date:
            dates.append(date)

    #sitemap.xml den tarih kontrolü
    try:
        sitemap_url = url.rstrip('/') + "/sitemap.xml"
        sitemap_response = requests.get(sitemap_url, timeout=40,verify=False)
        if sitemap_response.status_code == 200:
            sitemap_soup = BeautifulSoup(sitemap_response.content, 'xml')
            lastmod_tag = sitemap_soup.find('lastmod')
            if lastmod_tag:
                date = parse_date(lastmod_tag.text.strip())
                if date:
                    dates.append(date)
    except requests.exceptions.RequestException as e:
        print(f"Error fetching sitemap for {url}: {e}")

    # En güncel tarih seçimi
    if dates:
        latest_date = max(dates)
        return {"year": latest_date.year, "month": latest_date.month}

    return {"year": None, "month": None}

def parse_date(date_str):
    date_formats = [
        "%Y-%m-%dT%H:%M:%S",
        "%Y-%m-%dT%H:%M:%S.%fZ",
        "%Y-%m-%d",
        "%d-%m-%Y",
        "%m/%d/%Y",
        "%B %d, %Y",
        "%d %B %Y",
        "%a, %d %b %Y %H:%M:%S %Z"
    ]
    for fmt in date_formats:
        try:
            return datetime.strptime(date_str, fmt)
        except ValueError:
            continue
    return None

def extract_external_backlinks(soup, base_url):

    backlinks = []
    base_domain = urlparse(base_url).netloc.lower()
    for tag in soup.find_all('a', href=True):
        href = tag.get('href')
        absolute_url = urljoin(base_url, href)
        link_domain = urlparse(absolute_url).netloc.lower()
        if link_domain and link_domain != base_domain:
            backlinks.append(absolute_url)
    return list(set(backlinks))

def extract_meaningful_text(soup):
    content_tags = soup.find_all(["p", "h1", "h2", "h3", "li"])
    texts = [clean_text(tag.get_text(strip=True)) for tag in content_tags if tag.get_text(strip=True)]
    return " ".join(texts)

def fetch_and_parse(url):

    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        page = browser.new_page()
        page.goto(url, timeout=30000)
        page.wait_for_timeout(5000)
        html_content = page.content()
        browser.close()
        return html_content

def tag_website(_id, url, use_playwright=False):

    url = ensure_url_scheme(url)
    start_time = time.time()

    #last_updated = {"year": None, "month": None}

    if use_playwright:
        try:
            html_content = fetch_and_parse(url)
            soup = BeautifulSoup(html_content, "html.parser")
            load_time = round(time.time() - start_time, 2)
            last_updated = get_last_updated_date(url,soup=soup)
        except Exception as e:
            print(f"Playwright ile içerik çekilirken hata oluştu {url}: {e}")
            return {"url": url, "ssl_certificate": False, "error": str(e)}
    else:
        scraper = cloudscraper.create_scraper()
        selected_cookie = random.choice(COOKIES)
        cookie_data = {selected_cookie["name"]: selected_cookie["value"]}
        headers = {
            'User-Agent': random.choice(USER_AGENTS),
            'Accept-Language': 'en-US,en;q=0.9',
            'Connection': 'keep-alive'
        }
        for attempt in range(RETRY_ATTEMPTS):
            try:
                response = scraper.get(url, timeout=15 + min(attempt * 5, 30), cookies=cookie_data, headers=headers)
                response.raise_for_status()
                load_time = round(time.time() - start_time, 2)
                html_content = response.content.decode('utf-8', 'ignore')
                soup = BeautifulSoup(html_content, "html.parser")


                last_updated = get_last_updated_date(url, response=response, soup=soup)
                break
            except requests.RequestException as e:
                if '404' in str(e):
                    not_found_urls.append(url)
                if attempt < RETRY_ATTEMPTS - 1:
                    wait_time = random.uniform(3, 6)
                    print(f"Timeout/error for {url}. Retrying in {wait_time:.2f} seconds...")
                    time.sleep(wait_time)
                else:
                    print(f"Error fetching {url}: {e}")
                    return {"url": url, "ssl_certificate": False, "error": str(e)}
            except Exception as e:
                print(f"Unexpected error for {url}: {e}")
                return {"url": url, "ssl_certificate": False, "error": str(e)}


    h1_tags = [clean_text(tag.get_text().strip()) for tag in soup.find_all('h1')]
    h2_tags = [clean_text(tag.get_text().strip()) for tag in soup.find_all('h2')]
    h3_tags = [clean_text(tag.get_text().strip()) for tag in soup.find_all('h3')]
    title_tag = clean_text(soup.title.string.strip()) if soup.title and soup.title.string else ""
    meta_tag = soup.find("meta", attrs={"name": "description"})
    meta_description = clean_text(meta_tag["content"].strip()) if meta_tag and meta_tag.get("content") else ""
    ssl_valid = has_ssl_certificate(url)
    mobile_compatible = bool(soup.find('meta', attrs={'name': 'viewport'}))
    domain_age = get_site_age(url)
    content = extract_meaningful_text(soup)
    random_content = get_random_content(content, length=300)
    strong_texts = [clean_text(tag.get_text(strip=True)) for tag in soup.find_all("strong") if
                    clean_text(tag.get_text(strip=True))]
    underline_texts = [clean_text(tag.get_text(strip=True)) for tag in soup.find_all("u") if
                       clean_text(tag.get_text(strip=True))]
    external_backlinks = extract_external_backlinks(soup, url)

    return {
        "_id": _id,
        "url": url,
        "h1_keyword": h1_tags,
        "h2_keyword": h2_tags,
        "h3_keyword": h3_tags,
        "title_keyword": title_tag,
        "meta_keyword": meta_description,
        "load_time": load_time,
        "last_update_year": last_updated["year"],
        "last_update_month": last_updated["month"],
        "mobile_compatibility": mobile_compatible,
        "ssl_certificate": ssl_valid,
        "site_age": domain_age,
        "processed_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "random_content": random_content,
        "strong_texts": strong_texts,
        "underline_texts": underline_texts,
        "external_backlinks": external_backlinks
    }

def process_with_delay(args, use_playwright=False):
    _id, url = args
    time.sleep(SLEEP_DELAY)
    return tag_website(_id, url, use_playwright=use_playwright)


def load_environment_variables():
    """Çevresel değişkenleri yükler ve geri döndürür."""
    load_dotenv()
    print("[LOG] Çevresel değişkenler yükleniyor...")

    MONGO_URI = os.getenv("MONGO_URI")
    MONGO_DB_NAME = os.getenv("MONGO_DB_NAME")
    PROCESSED_SPIDER_COLL = os.getenv("PROCESSED_COLLECTION_SPIDER")
    PROCESSED_SITES_SEO = os.getenv("PROCESSED_SITES_SEO")
    PROCESSED_SITES_SEO_LINKS = os.getenv("PROCESSED_SITES_SEO_LINKS")

    print(f"[LOG] MongoDB bağlanıyor: {MONGO_URI}, Veritabanı: {MONGO_DB_NAME}")

    return MONGO_URI, MONGO_DB_NAME, PROCESSED_SPIDER_COLL, PROCESSED_SITES_SEO, PROCESSED_SITES_SEO_LINKS


def get_new_records(mongo_db_context, collection, one_week_ago):
    """MongoDB'den bir haftadan eski kayıtları alır."""
    query = {
        "$or": [
            {"last_processed_time": {"$exists": False}},
            {"last_processed_time": {"$lt": one_week_ago}}
        ]
    }
    new_records = mongo_db_context.get_datas_from_mongodb(collection, query=query, limit=50)
    return new_records


def process_batch(batch, thread_count):
    """Batch işlemeyi yapar ve geçerli sonuçları döndürür."""
    with ThreadPoolExecutor(max_workers=thread_count) as executor:
        results = list(executor.map(lambda args: process_with_delay(args, use_playwright=True), batch))
    return [r for r in results if r and "error" not in r]


def save_results(mongo_db_context, batch_results, PROCESSED_SITES_SEO, PROCESSED_SITES_SEO_LINKS,
                 PROCESSED_SPIDER_COLL):
    """İşlenen sonuçları MongoDB'ye kaydeder ve günceller."""
    for result in batch_results:
        mongo_db_context.save_datas_to_mongo(PROCESSED_SITES_SEO, result)
        print(f"[LOG] URL {result['url']} processed_sites_seo koleksiyonuna kaydedildi.")

        link_record = {
            "url": result["url"],
            "processed_time": datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        }
        mongo_db_context.save_datas_to_mongo(PROCESSED_SITES_SEO_LINKS, link_record)
        print(f"[LOG] URL {result['url']} processed_sites_seo_links koleksiyonuna kaydedildi.")

        mongo_db_context.update_mongo_record(
            PROCESSED_SPIDER_COLL,
            {"_id": result["_id"]},
            {"$set": {"last_processed_time": datetime.now()}}
        )
        print(f"[LOG] kayıt last_processed_time alanı güncellendi.")


def main():
    """Ana döngüyü çalıştırır ve kayıtları işler."""
    MONGO_URI, MONGO_DB_NAME, PROCESSED_SPIDER_COLL, PROCESSED_SITES_SEO, PROCESSED_SITES_SEO_LINKS = load_environment_variables()

    mongo_db_context = MongoDbContext(MONGO_URI, MONGO_DB_NAME)
    task_queue = Queue()

    while True:
        one_week_ago = datetime.now() - timedelta(weeks=1)

        # Yeni kayıtları al
        new_records = get_new_records(mongo_db_context, PROCESSED_SPIDER_COLL, one_week_ago)

        if not new_records:
            print("[LOG] Yeni kayıt bulunamadı, bekleniyor...")
            time.sleep(1)
            continue

        print(f"[LOG] {len(new_records)} yeni kayıt bulundu.")
        for record in new_records:
            task_queue.put((record['_id'], record['url']))

        print(f"[LOG] {task_queue.qsize()} kayıt işlenmek üzere kuyruğa eklendi.")

        thread_count = get_dynamic_thread_count()  # Dinamik işleme sayısını al
        batch_results = []

        while not task_queue.empty():
            batch = []
            for _ in range(min(50, task_queue.qsize())):
                batch.append(task_queue.get())
            print(f"[LOG] {len(batch)} kayıt işleniyor...")

            # Batch işlemesi
            valid_results = process_batch(batch, thread_count)
            batch_results.extend(valid_results)
            print(f"[LOG] {len(valid_results)} kayıt başarıyla işlendi.")

        # Sonuçları kaydet
        save_results(mongo_db_context, batch_results, PROCESSED_SITES_SEO, PROCESSED_SITES_SEO_LINKS,
                     PROCESSED_SPIDER_COLL)


if __name__ == "__main__":
    main()

"""
# JSON dosyalarına yazma
with open('not_found_urls.json', 'w', encoding='utf-8') as nf_file:
    json.dump(not_found_urls, nf_file, ensure_ascii=False, indent=4)

with open('whois_error_urls.json', 'w', encoding='utf-8') as we_file:
    json.dump(whois_error_urls, we_file, ensure_ascii=False, indent=4)

with open('sonuclar2.json', 'w', encoding='utf-8') as output_file:
    json.dump(sonuclar, output_file, ensure_ascii=False, indent=4)

print("Sonuçlar 'sonuclar.json' dosyasına kaydedildi.")
print("404 hatası veren URL'ler 'not_found_urls.json' dosyasına kaydedildi.")
print("Whois hatası veren URL'ler 'whois_error_urls.json' dosyasına kaydedildi.")"""