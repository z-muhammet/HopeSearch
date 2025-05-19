import json
import os
from pymongo import MongoClient
from dotenv import load_dotenv

# .env dosyasını yükle
load_dotenv()

# Çevresel değişkenleri oku
MONGO_URI = os.getenv("MONGO_URI")
MONGO_DB_NAME = os.getenv("MONGO_DB_NAME")
MONGO_READ_COLLECTION = os.getenv("PROCESSED_COLLECTION_SPIDER")

# MongoDB'ye bağlan
client = MongoClient(MONGO_URI)
database = client[MONGO_DB_NAME]
collection = database[MONGO_READ_COLLECTION]

# JSON dosyasını oku ve MongoDB'ye ekle
with open('veriler2.json', 'r', encoding='utf-8') as file:
    veriler = json.load(file)

collection.insert_many(veriler)

# Bağlantıyı kapat
client.close()

print("Veriler başarıyla MongoDB'ye aktarıldı!")
