import logging
from datetime import datetime, timedelta
import asyncio
from urllib.parse import urlparse
from motor.motor_asyncio import AsyncIOMotorClient
from pymongo import ASCENDING
from pymongo.errors import DuplicateKeyError, PyMongoError

logger = logging.getLogger("AsyncSpider")

MONGO_URI = "mongodb://localhost:27017"
client = AsyncIOMotorClient(MONGO_URI)
db = client["HopeSearch"]

processed_collection = db["processed_sites"]
botlinks_collection = db["botlinks"]
queue_collection = db["queue_urls"]
botChaptaBlock_collection = db["botChaptaBlock"]

processed_set = set()
bot_dict = {}

bot_captcha_dict = {}


async def create_indexes():
    try:
        await processed_collection.create_index([("url", ASCENDING)], unique=True)
        await botlinks_collection.create_index([("url", ASCENDING)], unique=True)
        await queue_collection.create_index([("url", ASCENDING)], unique=True)
        # BotChaptaBlock için de index oluşturuyoruz
        await botChaptaBlock_collection.create_index([("url", ASCENDING)], unique=True)
        logger.info("Indexes başarıyla oluşturuldu.")
    except PyMongoError:
        logger.error("Index oluşturma hatası:", exc_info=True)

    
async def load_existing_data():
    try:
        async for doc in processed_collection.find({}, {"_id": 0, "url": 1}):
            processed_set.add(doc["url"])
        async for doc in botlinks_collection.find({}, {"_id": 0, "url": 1, "blocked_time": 1}):
            bot_dict[doc["url"]] = doc["blocked_time"]
        # İsteğe bağlı: CAPTCHA blok kayıtlarını da RAM'e yükleyebilirsiniz
        async for doc in botChaptaBlock_collection.find({}, {"_id": 0, "url": 1, "blocked_time": 1}):
            bot_captcha_dict[doc["url"]] = doc["blocked_time"]
        logger.info("Mevcut veriler RAM'e yüklendi.")
    except PyMongoError:
        logger.error("Veri yükleme hatası:", exc_info=True)

async def remove_expired_block(url: str):
    bot_dict.pop(url, None)
    try:
        await botlinks_collection.delete_one({"url": url})
        logger.info(f"Geçerliliğini yitirmiş bloklama kaldırıldı: {url}")
    except PyMongoError:
        logger.warning(f"{url} için engel kaldırılırken hata:", exc_info=True)

async def is_recently_blocked(url: str) -> bool:
    if url not in bot_dict:
        return False
    blocked_time = bot_dict[url]
    if (datetime.now() - blocked_time) < timedelta(minutes=30):
        return True
    await remove_expired_block(url)
    return False

async def mark_as_blocked(url: str):
    now = datetime.now()
    bot_dict[url] = now
    try:
        await botlinks_collection.insert_one({"url": url, "blocked_time": now})
        logger.info(f"[BOTBLOCK] {url} 30 dakika boyunca engellendi.")
    except DuplicateKeyError:
        try:
            await botlinks_collection.update_one({"url": url}, {"$set": {"blocked_time": now}})
            bot_dict[url] = now
            logger.info(f"[BOTBLOCK-UPDATE] {url} zaten engelliydi; zaman güncellendi.")
        except PyMongoError:
            logger.error(f"{url} için botblock güncellenirken hata:", exc_info=True)
    except PyMongoError:
        logger.error(f"{url} için botblock eklenirken hata:", exc_info=True)

    
async def cleanup_queue_urls(interval_seconds: int = 30):
    while True:
        try:
            # processed_sites koleksiyonundaki bütün URL'leri hafızaya al
            processed_set = set()
            async for doc in processed_collection.find({}, {"_id": 0, "url": 1}):
                processed_set.add(doc["url"])

            if processed_set:
                # queue_urls içinde processed_set'te bulunan URL'leri temizle
                result = await queue_collection.delete_many({"url": {"$in": list(processed_set)}})
                if result.deleted_count:
                    logger.info(f"[QUEUE CLEANUP] {result.deleted_count} kayıt queue_urls'tan silindi (processed_sites ile çakışan).")

        except Exception as e:
            logger.error(f"[QUEUE CLEANUP ERROR] {type(e).__name__}: {e}", exc_info=True)

        # 30 saniye bekle (ya da parametre gelen değeri)
        await asyncio.sleep(interval_seconds)
        
async def partial_cleanup_queue_urls(batch_size: int = 10000) -> None:
    skip = 0
    
    while True:
        try:
            # 1) processed_collection'dan 'batch_size' kadar kayıt oku
            #    (sadece url alanını alıyoruz).
            urls_chunk = []
            cursor = processed_collection.find({}, {"_id": 0, "url": 1}).skip(skip).limit(batch_size)
            async for doc in cursor:
                if "url" in doc:
                    urls_chunk.append(doc["url"])

            # 2) Eğer chunk boşsa, işlem tamam demektir
            if not urls_chunk:
                logger.info("[CLEANUP] Tüm kayıtlar tarandı, işlem tamam.")
                break

            # 3) Aynı chunk içinde mükerrer URL'leri elemek için set kullan
            unique_urls = set(urls_chunk)

            # 4) queue_urls'tan bu URL'leri sil
            result = await queue_collection.delete_many({"url": {"$in": list(unique_urls)}})

            logger.info(
                f"[CLEANUP] skip={skip}, chunk_size={len(unique_urls)}, "
                f"deleted={result.deleted_count} kayit silindi."
            )

            # 5) Sıradaki chunk'a geçmek için skip'i artır
            skip += batch_size

        except PyMongoError as e:
            logger.error(f"[CLEANUP ERROR] {type(e).__name__}: {e}", exc_info=True)
            # İsteğe bağlı olarak tekrar denemek veya tamamen çıkmak için karar verilebilir.
            break
        
async def remove_url_from_queue_db(url: str) -> int:
    try:
        result = await queue_collection.delete_many({"url": url})
        deleted_count = result.deleted_count
        if deleted_count > 0:
            logger.debug(f"[remove_url_from_queue_db] {url} -> {deleted_count} kayıt silindi.")
        return deleted_count
    except PyMongoError as e:
        logger.error(f"[DB-ERROR] remove_url_from_queue_db({url}) -> {e}", exc_info=True)
        return 0
    
    # --- CAPTCHA Bloklama Sistemi ---
def get_domain(url: str) -> str:
    """ URL'den domain'i çıkarır. """
    parsed_url = urlparse(url)
    return parsed_url.netloc.lower()

async def mark_as_chapta_blocked(url: str):
    """ CAPTCHA tespitinde, URL'nin domain'ini botChaptaBlock tablosuna ekler. """
    domain = get_domain(url)
    now = datetime.now()
    bot_captcha_dict[domain] = now
    try:
        await botChaptaBlock_collection.insert_one({"url": domain, "blocked_time": now})
        logger.info(f"[BOTCHAPTA BLOCK] {domain} captcha tespit edildi, 30 dakika boyunca engellendi.")
    except DuplicateKeyError:
        try:
            await botChaptaBlock_collection.update_one({"url": domain}, {"$set": {"blocked_time": now}})
            bot_captcha_dict[domain] = now
            logger.info(f"[BOTCHAPTA BLOCK-UPDATE] {domain} zaten engelliydi; zaman güncellendi.")
        except PyMongoError:
            logger.error(f"{domain} için botChaptaBlock güncellenirken hata:", exc_info=True)
    except PyMongoError:
        logger.error(f"{domain} için botChaptaBlock eklenirken hata:", exc_info=True)

async def is_captcha_blocked(url: str) -> bool:
    """ URL'nin domain'i CAPTCHA nedeniyle engelliyse True döner. """
    domain = get_domain(url)
    if domain not in bot_captcha_dict:
        return False
    blocked_time = bot_captcha_dict[domain]
    if (datetime.now() - blocked_time) < timedelta(minutes=30):
        return True
    await remove_expired_captcha_block(domain)
    return False

async def remove_expired_captcha_block(domain: str):
    bot_captcha_dict.pop(domain, None)
    try:
        await botChaptaBlock_collection.delete_one({"url": domain})
        logger.info(f"Geçerliliğini yitirmiş captcha engelleme kaldırıldı: {domain}")
    except PyMongoError:
        logger.warning(f"{domain} için captcha engelleme kaldırılırken hata:", exc_info=True)
