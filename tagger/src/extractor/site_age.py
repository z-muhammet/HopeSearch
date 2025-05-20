from datetime import datetime
from urllib.parse import urlparse
import whois
from dateutil import parser
from logs.logger import setup_logger

logger = setup_logger()

def get_site_age(url):
    try:
        parsed = urlparse(url)
        domain = parsed.netloc or parsed.path
        if domain.startswith("www."):
            domain = domain[4:]

        domain_info = whois.whois(domain)
        creation_date = domain_info.creation_date

        if isinstance(creation_date, list):
            creation_date = creation_date[0]
        if isinstance(creation_date, str):
            creation_date = parser.parse(creation_date)

        if isinstance(creation_date, datetime):
            site_age = datetime.now().year - creation_date.year
            return site_age
        else:
            logger.warning(f"{url} için oluşum tarihi mevcut değil veya beklenen formatta değil.")

    except Exception as e:
        logger.error(f"{url} için site yaşı alınırken bir hata oluştu: {e}")

    return None
