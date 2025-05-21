import json
import sys
import os
import logging
from pymongo import MongoClient
from gensim.models import KeyedVectors
from bson.objectid import ObjectId

# --- Loglama Yapılandırması ---
logging.basicConfig(
    filename='process.log',
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

# --- Word2Vec Modelini Yükle ---
try:
    word_vectors = KeyedVectors.load_word2vec_format('trmodel', binary=True)
    logging.info("Word2Vec modeli başarıyla yüklendi.")
    print("Word2Vec modeli başarıyla yüklendi.")
except Exception as e:
    logging.error(f"HATA: Word2Vec modeli yüklenirken bir hata oluştu: {e}")
    print(f"HATA: Word2Vec modeli yüklenirken bir hata oluştu: {e}. Lütfen 'trmodel' dosyasının bulunduğundan ve erişilebilir olduğundan emin olun.")
    sys.exit(1) # Model yüklenemezse betiği sonlandır

# --- Yardımcı Fonksiyonlar ---
def get_related_words(word, model, top_n=10):
    """Belirtilen kelimeye anlamsal olarak en yakın kelimeleri Word2Vec modelinden çeker."""
    try:
        related_words = model.most_similar(positive=[word], topn=top_n)
        logging.info(f"'{word}' için {len(related_words)} bağlamsal kelime bulundu.")
        return [w[0] for w in related_words]
    except KeyError:
        logging.warning(f"Kelime modelde bulunamadı: {word}.")
        return []
    except Exception as e:
        logging.error(f"'{word}' kelimesi için ilgili kelimeler alınırken hata: {e}")
        return []

def generate_search_variants(word_list, model):
    """Bir kelime listesi için arama varyantları (ilişkili kelimeler dahil) oluşturur."""
    search_variants = set(word_list)
    for word in word_list:
        search_variants.update(get_related_words(word, model))
    logging.info(f"Arama varyantları oluşturuldu: {search_variants}")
    print(f"Arama varyantları oluşturuldu: {search_variants}")
    return list(search_variants)

def keyword_in_field(field_value, variants, url=""):
    """Bir metin alanında arama varyantlarından herhangi bir anahtar kelime olup olmadığını kontrol eder."""
    if not isinstance(field_value, str):
        if isinstance(field_value, list):
            field_value = " ".join(filter(None, map(str, field_value)))
        else:
            field_value = str(field_value) if field_value is not None else ""

    keywords = [k.strip().lower() for k in field_value.split(",") if k.strip()]
    match_found = any(v.lower() in keywords for v in variants)
    logging.info(f"[{url}] Alanda eşleşme: {match_found}")
    return match_found

def calculate_ratio(field_value, variants, url=""):
    """Bir metin alanındaki anahtar kelimelerin, arama varyantlarıyla eşleşme oranını hesaplar."""
    if not isinstance(field_value, str):
        if isinstance(field_value, list):
            field_value = " ".join(filter(None, map(str, field_value)))
        else:
            field_value = str(field_value) if field_value is not None else ""

    keywords = [k.strip().lower() for k in field_value.split(",") if k.strip()]
    total_count = len(keywords)
    if total_count == 0:
        return 0.0
    
    match_count = sum(keywords.count(v.lower()) for v in variants)
    ratio = match_count / total_count
    logging.info(f"[{url}] Kelime oranı: {ratio:.3f}")
    return ratio

def convert_object_ids_to_str(data):
    """Bir sözlük veya liste içindeki tüm ObjectId nesnelerini yinelemeli olarak stringe dönüştürür."""
    if isinstance(data, list):
        return [convert_object_ids_to_str(item) for item in data]
    if isinstance(data, dict):
        return {k: str(v) if isinstance(v, ObjectId) else convert_object_ids_to_str(v) for k, v in data.items()}
    return data

# --- Ana Çalışma Bloğu ---
if __name__ == "__main__":
    # Komut satırı argümanlarını kontrol et
    if len(sys.argv) < 2:
        print("Hata: Aranacak kelime belirtilmedi.")
        print("Kullanım: python3 search.py <aranacak_kelime>")
        print("Örnek: python3 search.py haber")
        print("Örnek: python3 search.py Çiçek")
        sys.exit(1) # Parametre eksikse hata koduyla çık

    query_keyword = sys.argv[1] # İlk komut satırı argümanını arama kelimesi olarak al
    print(f"Aranacak anahtar kelime: '{query_keyword}'")

    # --- MongoDB Bağlantısı ---
    MONGO_URI = os.getenv("MONGO_URI", "mongodb://localhost:27017")
    DB_NAME = os.getenv("MONGO_DB_NAME", "HopeSearch")

    try:
        client = MongoClient(MONGO_URI)
        db = client[DB_NAME]
        logging.info(f"MongoDB'ye '{DB_NAME}' veritabanına başarıyla bağlanıldı.")
        print(f"MongoDB'ye '{DB_NAME}' veritabanına başarıyla bağlanıldı.")
    except Exception as e:
        logging.error(f"HATA: MongoDB'ye bağlanılamadı: {e}")
        print(f"HATA: MongoDB'ye bağlanılamadı. Lütfen MongoDB sunucusunun çalıştığından emin olun ve log dosyasını kontrol edin.")
        sys.exit(1)

    # --- Önbellek Kontrolü ve Güncel Veri Karşılaştırması ---
    logging.info(f"'{query_keyword}' için önbellek kontrol ediliyor...")
    print(f"'{query_keyword}' için önbellek kontrol ediliyor ('search_keyword_cache')...")

    try:
        current_total_sites_in_db = db.processed_sites_seo.count_documents({})
        logging.info(f"processed_sites_seo koleksiyonundaki güncel site sayısı: {current_total_sites_in_db}")
        print(f"processed_sites_seo koleksiyonundaki güncel site sayısı: {current_total_sites_in_db}")
    except Exception as e:
        logging.error(f"HATA: processed_sites_seo koleksiyonundaki site sayısı alınırken hata: {e}")
        print(f"HATA: processed_sites_seo koleksiyonundaki site sayısı alınırken hata: {e}")
        client.close()
        sys.exit(1)

    cache_result = db.search_keyword_cache.find_one({"keyword": query_keyword})

    if cache_result:
        cached_result_count = len(cache_result.get("results", []))
        logging.info(f"'{query_keyword}' için önbellekte {cached_result_count} sonuç bulundu.")
        print(f"'{query_keyword}' için önbellekte {cached_result_count} sonuç bulundu.")

        if cached_result_count == current_total_sites_in_db and current_total_sites_in_db > 0:
            print("Önbellekten sonuç getirildi (güncel).")
            logging.info(f"'{query_keyword}' için önbellek sonuçları güncel. JSON dosyasına yazılıyor...")
            
            processed_cache_results = convert_object_ids_to_str(cache_result.get("results", []))

            try:
                with open("processed_dataset.json", "w", encoding="utf-8") as f:
                    json.dump(processed_cache_results, f, ensure_ascii=False, indent=4)
                logging.info("Önbellek sonuçları processed_dataset.json dosyasına başarıyla yazıldı.")
                print("Önbellek sonuçları 'processed_dataset.json' dosyasına yazıldı. İşlem tamamlandı.")
            except Exception as e:
                logging.error(f"HATA: Önbellek sonuçları JSON'a yazılırken hata: {e}")
                print(f"HATA: Önbellek sonuçları JSON'a yazılırken bir sorun oluştu: {e}")
            
            client.close()
            sys.exit(0)
        else:
            print("Önbellekte sonuç bulundu ancak güncel değil veya site sayısı değişti. Yeniden arama başlatılıyor.")
            logging.info(f"'{query_keyword}' için önbellek sonuçları güncel değil. Yeniden arama başlatılıyor.")
    else:
        print(f"'{query_keyword}' kelimesi için önbellekte sonuç bulunamadı. Yeni arama başlatılıyor.")
        logging.info(f"'{query_keyword}' kelimesi için önbellekte sonuç bulunamadı. Yeni arama başlatılıyor.")

    # --- Yeni Arama İşlemi (Önbellekte bulunamadıysa veya güncel değilse) ---
    base_keywords = [query_keyword]
    search_variants = generate_search_variants(base_keywords, word_vectors)

    try:
        input_data = list(db.processed_sites_seo.find())
        logging.info(f"MongoDB'den {len(input_data)} adet site verisi başarıyla çekildi.")
        print(f"MongoDB'den {len(input_data)} adet site verisi başarıyla çekildi. İşlemeye başlanıyor...")

    except Exception as e:
        logging.error(f"HATA: MongoDB'den processed_sites_seo verisi çekilirken hata: {e}")
        print(f"HATA: Site verileri çekilirken bir sorun oluştu: {e}. Lütfen log dosyasını kontrol edin.")
        client.close()
        sys.exit(1)

    processed_data = []

    # --- Veri İşleme ---
    for i, record in enumerate(input_data):
        url = record.get("url", "URL_BULUNAMADI")
        current_site_index = i + 1
        logging.info(f"[{current_site_index}/{current_total_sites_in_db}] Site işleniyor: {url}")
        print(f"[{current_site_index}/{current_total_sites_in_db}] Site işleniyor: {url}")

        h1_kw_present = keyword_in_field(record.get("h1_keyword", ""), search_variants, url)
        title_kw_present = keyword_in_field(record.get("title_keyword", ""), search_variants, url)
        content_ratio = calculate_ratio(record.get("content_keyword_match", ""), search_variants, url)
        meta_ratio = calculate_ratio(record.get("meta_keyword_density", ""), search_variants, url)

        processed_record = {
            "_id": str(record["_id"]) if "_id" in record else None,
            "url": url,
            "h1_keyword": h1_kw_present,
            "title_keyword": title_kw_present,
            "content_keyword_match": round(content_ratio, 3),
            "meta_keyword_density": round(meta_ratio, 3),
            "load_time": record.get("load_time", 0.0),
            "last_update_year": record.get("last_update_year", 1970),   
            "last_update_month": record.get("last_update_month", 1),
            "last_update_day": record.get("last_update_day", 1),
            "site_type": record.get("site_type", "unknown"),
            "mobile_compatibility": record.get("mobile_compatibility", False),
            "ssl_certificate": record.get("ssl_certificate", False),
            "site_age": record.get("site_age", 0)
        }
        processed_data.append(processed_record)
        logging.info(f"[{current_site_index}/{current_total_sites_in_db}] Site işlendi: {url}")
        print(f"[{current_site_index}/{current_total_sites_in_db}] Site işlendi: {url}")

    # --- Sonuçları search_keyword_cache Koleksiyonuna ve JSON'a Yaz ---
    logging.info("Tüm siteler işlendi. Sonuçlar 'search_keyword_cache' koleksiyonuna ve JSON dosyasına yazılıyor.")
    print("Tüm siteler işlendi. Sonuçlar 'search_keyword_cache' koleksiyonuna ve JSON dosyasına yazılıyor...")

    final_processed_data_for_json = convert_object_ids_to_str(processed_data)

    try:
        db.search_keyword_cache.update_one(
            {"keyword": query_keyword},
            {"$set": {"results": final_processed_data_for_json}},
            upsert=True
        )
        logging.info(f"'{query_keyword}' için sonuçlar 'search_keyword_cache' koleksiyonuna başarıyla önbelleğe alındı/güncellendi.")
        print(f"'{query_keyword}' için sonuçlar 'search_keyword_cache' koleksiyonuna başarıyla kaydedildi/güncellendi.")
    except Exception as e:
        logging.error(f"HATA: Önbellek verisi 'search_keyword_cache' koleksiyonuna yazılırken/güncellenirken hata: {e}")
        print(f"HATA: Arama önbelleği kaydedilirken/güncellenirken bir sorun oluştu: {e}")

    try:
        with open("processed_dataset.json", "w", encoding="utf-8") as f:
            json.dump(final_processed_data_for_json, f, ensure_ascii=False, indent=4)
        logging.info("Veriler processed_dataset.json dosyasına başarıyla yazıldı.")
        print("Veriler işlenerek 'processed_dataset.json' dosyasına başarıyla yazıldı.")
    except Exception as e:
        logging.error(f"HATA: JSON dosyasına yazılırken hata: {e}")
        print(f"HATA: İşlenmiş veriler JSON dosyasına yazılırken bir sorun oluştu: {e}")

    client.close()
    logging.info("MongoDB bağlantısı kapatıldı.")
    print("MongoDB bağlantısı kapatıldı. İşlem tamamlandı.")