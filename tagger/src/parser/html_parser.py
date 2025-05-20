import re
from bs4 import BeautifulSoup

def has_captcha_or_bot_protection(soup, html_text):
    suspicious_keywords = [
        'captcha', 'recaptcha', 'verify', 'botcheck', 'are you human', 'robot check',
        'robot musun', 'robot değilim', 'doğrulama', 'ben robot değilim', 'güvenlik doğrulaması'
    ]

    html_text_lower = html_text.lower()

    # HTML metninde anahtar kelime arama
    for keyword in suspicious_keywords:
        if keyword in html_text_lower:
            return True

    # <form>, <div>, <script> ve <iframe> içinde kontrol
    for tag in ['form', 'div', 'script']:
        for el in soup.find_all(tag):
            if any(keyword in str(el).lower() for keyword in suspicious_keywords):
                return True

    # iframe src içinde reCAPTCHA kontrolü
    for iframe in soup.find_all("iframe"):
        src = iframe.get("src", "").lower()
        if any(keyword in src for keyword in ['captcha', 'recaptcha', 'verify']):
            return True

    return False