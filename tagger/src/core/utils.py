import re
import unicodedata


def ensure_url_scheme(url):
    return url if url.startswith(('http://', 'https://')) else 'http://' + url

def clean_text(text):
    text = re.sub(r"[.,;:!?()\"“”’‘\[\]{}<>|/\\]", " ", text)

    text = unicodedata.normalize("NFKD", text)

    text = re.sub(r"[^a-zA-Z0-9çÇğĞıİöÖşŞüÜ ]", "", text)

    text = re.sub(r"\s+", " ", text)

    return ",".join(text.strip().split())
