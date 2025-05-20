from urllib.parse import urlparse, urljoin

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