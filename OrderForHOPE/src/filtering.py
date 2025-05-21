import os
from dotenv import load_dotenv
from pymongo import MongoClient

# .env dosyasını yükle
load_dotenv()

# Mongo bağlantısını ayarla
MONGO_URI = os.getenv("MONGO_URI")
MONGO_DB_NAME = os.getenv("MONGO_DB_NAME")

client = MongoClient(MONGO_URI)
db = client[MONGO_DB_NAME]
collection = db["search_keyword_cache"]

# Kullanıcıdan anahtar kelimeyi al
keyword = input("Aranacak kelimeyi girin: ").strip()
print(f"[INFO] Anahtar kelime alındı: '{keyword}'")

# MongoDB'den keyword'e göre belgeyi bul
doc = collection.find_one({"keyword": keyword})

if not doc:
    print(f"[WARN] '{keyword}' anahtar kelimesine ait veri bulunamadı.")
    exit()

# Belgeden sonuç listesini al
pages = doc.get("results", [])
page_count = len(pages)
print(f"[INFO] Veritabanından {page_count} kayıt bulundu.")

# Yoğunluk eşik değeri belirle
if page_count >= 1000:
    density = 0.5
elif 500 <= page_count < 1000:
    density = 0.4
else:
    density = 0

print(f"[INFO] Uygulanan yoğunluk filtresi (density): {density}")

# Filtreleme fonksiyonu
def filter_page_by_keyword(pages, density):
    filtered_pages = []
    for page in pages:
        update_year = page.get("last_update_year")
        if update_year is not None and update_year < 2015:
            continue
        if page.get("h1_keyword") or page.get("content_keyword_match", 0) > density or page.get("title_keyword") or page.get("meta_keyword_density",0):
            filtered_pages.append(page)
    return filtered_pages

# Sayfaları filtrele
filtered_pages = filter_page_by_keyword(pages, density)
print(f"[INFO] Filtrelenmiş sayfa sayısı: {len(filtered_pages)}")

# Filtrelenmiş sayfaları ekrana yazdır
print("\nFiltrelenmiş sayfalar:")
for idx, page in enumerate(filtered_pages):
    print(f"Site {idx + 1}: {page['url']}, H1 Keyword: {page['h1_keyword']}, Content Match: {page['content_keyword_match']}")
