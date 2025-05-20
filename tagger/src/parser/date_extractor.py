import json
import random
import re
from datetime import datetime
import requests
from bs4 import BeautifulSoup
from config.settings import USER_AGENTS
from core.utils import ensure_url_scheme
from logs.logger import setup_logger

logger = setup_logger()

def parse_date(date_str):
    date_formats = [
        "%Y-%m-%dT%H:%M:%S",
        "%Y-%m-%dT%H:%M:%S.%fZ",
        "%Y-%m-%d",
        "%d-%m-%Y",
        "%d.%m.%Y",
        "%m/%d/%Y",
        "%B %d, %Y",
        "%d %B %Y",
        "%a, %d %b %Y %H:%M:%S %Z"
    ]
    for fmt in date_formats:
        try:
            return datetime.strptime(date_str.strip(), fmt)
        except ValueError:
            continue
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
            response = requests.get(url, timeout=15, headers=headers)
            response.raise_for_status()
        except requests.exceptions.RequestException as e:
            logger.warning(f"{url} alınamadı: {e}")
            return {"year": None, "month": None}

    if soup is None:
        try:
            soup = BeautifulSoup(response.content.decode('utf-8', 'ignore'), "html.parser")
        except Exception as e:
            logger.warning(f"{url} için BeautifulSoup oluşturulamadı: {e}")
            return {"year": None, "month": None}

    dates = []

    # time etiketleri
    for time_el in soup.find_all('time'):
        if time_el.has_attr('datetime'):
            date = parse_date(time_el['datetime'])
            if date:
                dates.append(date)

    # itemprop
    for itemprop in ['dateModified', 'datePublished']:
        element = soup.find(attrs={"itemprop": itemprop})
        if element:
            date_str = element.get("content") or element.get_text()
            date = parse_date(date_str)
            if date:
                dates.append(date)

    # meta etiketleri
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
    for tag in meta_tags:
        meta = soup.find('meta', tag)
        if meta and 'content' in meta.attrs:
            date = parse_date(meta['content'])
            if date:
                dates.append(date)

    # JSON-LD
    for script in soup.find_all("script", type="application/ld+json"):
        try:
            data = json.loads(script.string)
            if isinstance(data, dict):
                for key in ["dateModified", "datePublished"]:
                    if key in data:
                        date = parse_date(data[key])
                        if date:
                            dates.append(date)
        except Exception as e:
            logger.debug(f"{url} içinde JSON-LD işlenemedi: {e}")
            continue

    # metin içinde regex ile tarih arama
    patterns = ["son güncelleme", "last updated", "last modified", "güncellendi"]
    for text in patterns:
        found = soup.find(string=re.compile(text, re.IGNORECASE))
        if found:
            match = re.search(r'\b(\d{1,2}[./-]\d{1,2}[./-]\d{4})\b', found)
            if match:
                date = parse_date(match.group(1))
                if date:
                    dates.append(date)

    # script etiketlerinde tarih arama
    date_pattern = re.compile(r'\b(\d{1,2}[./-]\d{1,2}[./-]\d{4})\b')
    for script in soup.find_all("script"):
        if script.string:
            for match in date_pattern.findall(script.string):
                date = parse_date(match)
                if date:
                    dates.append(date)

    # HTTP Header
    if 'Last-Modified' in response.headers:
        date = parse_date(response.headers['Last-Modified'])
        if date:
            dates.append(date)

    # sitemap.xml
    try:
        sitemap_url = url.rstrip("/") + "/sitemap.xml"
        sitemap_response = requests.get(sitemap_url, timeout=10)
        if sitemap_response.status_code == 200:
            sitemap_soup = BeautifulSoup(sitemap_response.content, 'xml')
            lastmod = sitemap_soup.find('lastmod')
            if lastmod:
                date = parse_date(lastmod.text.strip())
                if date:
                    dates.append(date)
    except Exception as e:
        logger.debug(f"Sitemap alınamadı: {e}")

    # sonuç
    if dates:
        latest = max(dates)
        return {"year": latest.year, "month": latest.month}

    return {"year": None, "month": None}
