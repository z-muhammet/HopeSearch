from core.utils import clean_text
import random

def get_random_content(soup, length=300):
    allowed_tags = ['h1', 'h2', 'h3', 'li', 'p']
    selected_texts = []

    for tag in allowed_tags:
        elements = soup.find_all(tag)
        selected_texts.extend([el.get_text(strip=True) for el in elements])

    cleaned = clean_text(' '.join(selected_texts))
    words = cleaned.split(',')

    if len(' '.join(words)) < length:
        return cleaned

    random.shuffle(words)
    return ','.join(words)[:length]