import random
import ssl
import socket
from urllib.parse import urlparse
from requests.cookies import RequestsCookieJar
from config.settings import USER_AGENTS, COOKIES
from logs import logger


def get_headers(url):
    headers = {
        "User-Agent": random.choice(USER_AGENTS),
        "Accept-Language": "en-US,en;q=0.9",
        "Accept-Encoding": "gzip, deflate, br",
        "Cache-Control": "no-cache",
        "Referer": url,
        "Connection": "keep-alive"
    }
    return headers


def get_cookies(url):
    parsed_domain = urlparse(url).netloc
    selected_cookie = random.choice(COOKIES)

    cookie_jar = RequestsCookieJar()
    cookie_jar.set(
        name=selected_cookie["name"],
        value=selected_cookie["value"],
        domain=parsed_domain,
        path=selected_cookie.get("path", "/")
    )
    return cookie_jar

def decode_response_content(response):
    try:
        return response.content.decode(response.apparent_encoding)
    except Exception as e:
        logger.error(f"İçerik çözülürken hata oluştu: {e}")
        return response.text


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