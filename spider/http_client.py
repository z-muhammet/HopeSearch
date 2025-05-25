import asyncio
import json
import logging
import random
import re
from langdetect import detect
import chardet
from urllib.parse import urlparse
from aiohttp import ClientSession, ClientError
from bs4 import BeautifulSoup


logger = logging.getLogger("AsyncSpider")

import re
from urllib.parse import urlparse

def is_excluded_domain(url: str) -> bool:
    parsed_url = urlparse(url)
    domain = parsed_url.netloc.lower()
    path = parsed_url.path.lower()

    excluded_patterns = [
        r".*\.?google\.(com|co\.uk|fr|de)$",
        r".*\.?youtube\.(com|co\.uk|fr|de)$",
        r".*\.?facebook\.(com|co\.uk|fr|de)$",
        r".*\.?twitter\.(com|co\.uk|fr|de)$",
        r".*\.?linkedin\.(com|co\.uk|fr|de)$",
        r".*\.?whatsapp\.(com|co\.uk|fr|de)$"
    ]

    excluded_extensions = [
        r"\.(pdf|doc|docx|xls|xlsx|ppt|pptx|txt|jpg|jpeg|png|gif|bmp|svg|webp|mp4|avi|mkv|mov|wmv|flv|webm|mp3|wav|aac|flac|ogg|zip|rar|7z|tar|gz)$"
    ]

    # Domain bazlı engelleme
    for pattern in excluded_patterns:
        if re.match(pattern, domain):
            return True

    # Dosya uzantısı bazlı engelleme
    for ext_pattern in excluded_extensions:
        if re.search(ext_pattern, path): 
            return True

    return False


def load_user_agents() -> list[str]:
    try:
        with open("config/user_agent.json", "r", encoding="utf-8") as f:
            data = json.load(f)
            return data.get("USER_AGENTS", [])
    except Exception as e:
        logger.warning(f"user_agent.json okunamadı: {e}")
        return []

USER_AGENTS = load_user_agents()

def get_random_user_agent() -> str:
    if not USER_AGENTS:
        return "Mozilla/5.0 (Windows NT 10.0; Win64; x64)"
    return random.choice(USER_AGENTS)

import logging
import chardet
from bs4 import BeautifulSoup
from aiohttp import ClientSession
from langdetect import detect

logger = logging.getLogger("AsyncSpider")

async def fetch_turkce(session: ClientSession, url: str, max_retries: int = 2) -> bool:

    if not url.startswith(("http://", "https://")):
        url = "http://" + url
    try:
        async with session.get(url, timeout=10, ssl=False) as resp:
            raw_data = await resp.read()
            encoding = chardet.detect(raw_data)["encoding"] or "utf-8"
            html_text = raw_data.decode(encoding, errors="replace")

            clean_text = BeautifulSoup(html_text, "html.parser").get_text(separator=" ", strip=True)
            clean_text = clean_text[:3000]  

            words = clean_text.split()
            if len(words) < 20:  
                return False

            # **Kelime bazlı değil, paragraf bazlı kontrol yapıyoruz!**
            paragraphs = clean_text.split(". ")  

            turkish_count = 0
            total_checked = 0

            for paragraph in paragraphs:
                if len(paragraph) > 10:  
                    try:
                        detected_lang = detect(paragraph)
                        if detected_lang == "tr":
                            turkish_count += 1
                        total_checked += 1
                    except Exception as e:
                        logger.warning(f"LangDetect hata verdi (görmezden geliniyor): {e}")

            if total_checked == 0:
                return False 

            turkish_ratio = turkish_count / total_checked

            return turkish_ratio >= 0.3

    except Exception as e:
        logger.warning(f"Dil kontrol hatası: {url} -> {type(e).__name__}: {e}")
        return False
    
def contains_captcha(soup):
    body = soup.body if soup.body else soup
    
    indicators = [
        body.find(lambda tag: tag.has_attr("class") and any(re.search("captcha", cls, re.I) for cls in tag.get("class", []))),
        body.find(lambda tag: tag.has_attr("id") and re.search("captcha", tag.get("id", ""), re.I)),
        body.find("div", {"class": re.compile("g-recaptcha", re.I)}),
        body.find("input", {"data-sitekey": True})
    ]
    
    return any(indicators)


async def fetch(session: ClientSession, url: str, max_retries: int = 2) -> list[str] | None:
    if not url.startswith("http"):
        url = "http://" + url
    if is_excluded_domain(url):
        logger.info(f"[BLOCK] {url} -> Domain engellendi.")
        return None
    headers = {"User-Agent": get_random_user_agent()}
    attempt = 0
    while attempt < max_retries:
        try:
            async with session.get(url, headers=headers, timeout=20, ssl=False) as resp:
                if resp.status in (401, 403, 429, 503):
                    return None

                raw_data = await resp.read()  
                detected_encoding = chardet.detect(raw_data)["encoding"] or "utf-8"
                text = raw_data.decode(detected_encoding, errors="replace")
                
                soup = BeautifulSoup(text, "html.parser")
                if contains_captcha(soup):
                    from db_manager import mark_as_chapta_blocked
                    await mark_as_chapta_blocked(url)
                    return None

                links = []
                for a_tag in soup.find_all("a", href=True):
                    href = a_tag["href"].strip()
                    if href.startswith("http"):
                        links.append(href)
                return links

        except (ClientError, asyncio.TimeoutError) as e:
            attempt += 1
            if attempt < max_retries:
                await asyncio.sleep(random.uniform(0.05, 0.015))
        except Exception as e:
            logger.error(f"[ERROR] {url} -> {e}", exc_info=True)
            return None
    return None
