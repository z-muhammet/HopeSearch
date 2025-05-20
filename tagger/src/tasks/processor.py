import multiprocessing
import time
import random
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime

from config.settings import SLEEP_DELAY, RETRY_ATTEMPTS
from core.delays import delay_request
from core.utils import clean_text, ensure_url_scheme

from fetcher.http_client import (
    get_headers,
    get_cookies,
    decode_response_content,
    has_ssl_certificate
)
from fetcher.playwright_client import fetch_and_parse

import cloudscraper
import requests
from bs4 import BeautifulSoup
from parser.date_extractor import parse_date, get_last_updated_date
from parser.html_parser import has_captcha_or_bot_protection

from extractor.random_content import get_random_content
from extractor.backlink_extractor import extract_external_backlinks

from extractor.site_age import get_site_age

from tqdm import tqdm


from logs.logger import setup_logger
logger = setup_logger()


# Başarısız veya bulunamayan URL listeleri
not_found_urls = []
unprocessable_sites = []
     

def get_dynamic_thread_count():
    return max(2, multiprocessing.cpu_count() // 2)
def tag_website(_id, url, use_playwright=False):
    url = ensure_url_scheme(url)
    start_time = time.time()

    if use_playwright:
        try:
            html_content = fetch_and_parse(url)
            soup = BeautifulSoup(html_content, "html.parser")
            load_time = round(time.time() - start_time, 2)
            last_updated = get_last_updated_date(url, soup=soup)
        except Exception as e:
            logger.error(f"Playwright ile içerik çekilirken hata oluştu {url}: {e}")
            unprocessable_sites.append({
                "_id": _id,
                "url": url,
                "error": str(e),
                "processed_time": datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            })
            return None
    else:
        headers = get_headers(url)
        cookie_jar = get_cookies(url)
        scraper = cloudscraper.create_scraper()

        for attempt in range(RETRY_ATTEMPTS):
            try:
                delay_request()
                response = scraper.get(url, timeout=15 + min(attempt * 5, 30), cookies=cookie_jar, headers=headers)
                response.raise_for_status()
                load_time = round(time.time() - start_time, 2)
                html_content = decode_response_content(response)
                soup = BeautifulSoup(html_content, "html.parser")

                if has_captcha_or_bot_protection(soup, html_content):
                    logger.warning(f"{url} => CAPTCHA tespit edildi, Playwright'e geçiliyor...")
                    return tag_website(_id, url, use_playwright=True)

                last_updated = get_last_updated_date(url, response=response, soup=soup)
                break
            except requests.RequestException as e:
                if '404' in str(e):
                    not_found_urls.append(url)
                if attempt < RETRY_ATTEMPTS - 1:
                    wait_time = random.uniform(3, 6)
                    logger.warning(f"{url} için hata oluştu, {wait_time:.2f} saniye sonra tekrar deneniyor...")
                    time.sleep(wait_time)
                else:
                    logger.error(f"{url} alınırken hata oluştu: {e}")
                    unprocessable_sites.append({
                        "_id": _id,
                        "url": url,
                        "error": str(e),
                        "processed_time": datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                    })
                    return None
            except Exception as e:
                logger.error(f"{url} için beklenmeyen hata: {e}")
                unprocessable_sites.append({
                    "_id": _id,
                    "url": url,
                    "error": str(e),
                    "processed_time": datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                })
                return None

    h1_tags = [clean_text(tag.get_text().strip()) for tag in soup.find_all('h1')]
    h2_tags = [clean_text(tag.get_text().strip()) for tag in soup.find_all('h2')]
    h3_tags = [clean_text(tag.get_text().strip()) for tag in soup.find_all('h3')]
    title_tag = clean_text(soup.title.string.strip()) if soup.title and soup.title.string else ""
    meta_tag = soup.find("meta", attrs={"name": "description"})
    meta_description = clean_text(meta_tag["content"].strip()) if meta_tag and meta_tag.get("content") else ""
    ssl_valid = has_ssl_certificate(url)
    mobile_compatible = bool(soup.find('meta', attrs={'name': 'viewport'}))
    domain_age = get_site_age(url)
    random_content = get_random_content(soup, length=300)
    strong_texts = [clean_text(tag.get_text(strip=True)) for tag in soup.find_all("strong") if clean_text(tag.get_text(strip=True))]
    underline_texts = [clean_text(tag.get_text(strip=True)) for tag in soup.find_all("u") if clean_text(tag.get_text(strip=True))]
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
        "external_backlinks": external_backlinks,
    }

def process_with_delay(args, use_playwright=False):
    _id, url = args
    time.sleep(SLEEP_DELAY)
    return tag_website(_id, url, use_playwright=use_playwright)


from concurrent.futures import ThreadPoolExecutor, as_completed
from tqdm import tqdm
import time
import threading
import logging

logger = logging.getLogger(__name__)

def process_batch(batch, thread_count):
    results = []
    start_time = time.time()
    progress_bar = tqdm(
        total=len(batch),
        desc="🚀 Siteler işleniyor",
        dynamic_ncols=True,
        mininterval=0.2
    )


    def update_timer():
        while not stop_timer.is_set():
            elapsed = int(time.time() - start_time)
            progress_bar.set_postfix_str(f"⏱️ {elapsed}s")
            time.sleep(1)

    stop_timer = threading.Event()
    timer_thread = threading.Thread(target=update_timer)
    timer_thread.start()

    try:
        with ThreadPoolExecutor(max_workers=thread_count) as executor:
            futures = {executor.submit(process_with_delay, args, True): args for args in batch}

            for future in as_completed(futures):
                _id, url = futures[future]
                try:
                    result = future.result()
                    if result is None:
                        logger.error(f"❌ Hata oluştu → {url}")
                    else:
                        results.append(result)
                except Exception as e:
                    logger.error(f"🔥 Thread exception: {url} → {e}")
                progress_bar.update(1)

    finally:
        stop_timer.set()
        timer_thread.join()
        progress_bar.close()

    success_count = len(results)
    fail_count = len(batch) - success_count

    logger.info(f"✅ {success_count} site başarıyla işlendi.")
    if fail_count > 0:
        logger.warning(f"⚠️ {fail_count} site işlenemedi.")

    return results
