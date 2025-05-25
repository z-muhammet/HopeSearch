from pymongo import MongoClient
from collections import defaultdict
import os

# Ortam değişkenleri veya doğrudan değerler
MONGO_URI = os.getenv("MONGO_URI", "mongodb://localhost:27017")
MONGO_DB_NAME = os.getenv("MONGO_DB_NAME", "HopeSearch")
PROCESSED_SITES_SEO = os.getenv("PROCESSED_SITES_SEO", "processed_sites_seo")

# Mongo bağlantısı
client = MongoClient(MONGO_URI)
db = client[MONGO_DB_NAME]
collection = db[PROCESSED_SITES_SEO]

# Aynı URL'ye sahip dokümanları bul
url_to_ids = defaultdict(list)

for doc in collection.find({}, {"_id": 1, "url": 1}):
    url = doc.get("url")
    if url:
        url_to_ids[url].append(doc["_id"])

# Aynı URL'ye sahip fazladan olanları sil
duplicate_count = 0
for url, ids in url_to_ids.items():
    if len(ids) > 1:
        # İlkini bırak, diğerlerini sil
        to_delete = ids[1:]
        result = collection.delete_many({"_id": {"$in": to_delete}})
        duplicate_count += result.deleted_count
        print(f"URL: {url} -> {result.deleted_count} duplicate(s) removed.")

print(f"\nToplam silinen kopya döküman sayısı: {duplicate_count}")
