from config.settings import (
    PROCESSED_COLLECTION_SPIDER,
    PROCESSED_SITES_SEO,
    PROCESSED_SITES_SEO_LINKS,
    UNPROCESSABLE_SITES
)
from storage.mongo_context import MongoDbContext
from storage.repository import Repository
from datetime import datetime, timedelta, timezone
from queue import Queue
from tasks.processor import process_batch, get_dynamic_thread_count
import logging

# Log ayarları
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)

BATCH_SIZE = 50
MAX_RETRY_COUNT = 3

def make_aware(dt):
    """Datetime objesini UTC-aware hale getirir."""
    if isinstance(dt, datetime) and dt.tzinfo is None:
        return dt.replace(tzinfo=timezone.utc)
    return dt

def main():
    logger.info("🚀 İşlem başlatıldı.")
    start_time = datetime.now(timezone.utc)

    mongo = MongoDbContext()
    spider_repo = Repository(PROCESSED_COLLECTION_SPIDER, mongo)
    seo_repo    = Repository(PROCESSED_SITES_SEO, mongo)
    links_repo  = Repository(PROCESSED_SITES_SEO_LINKS, mongo)
    unproc_repo = Repository(UNPROCESSABLE_SITES, mongo)

    iteration = 1

    while True:
        logger.info("🔄 [%d. tur] Yeni kayıtlar alınıyor...", iteration)

        now = datetime.now(timezone.utc)
        one_week_ago = now - timedelta(weeks=1)
        twelve_hours_ago = now - timedelta(hours=12)

        # Yeni sitelerden henüz işlenmemiş veya 1 haftadan eski olanları al
        new_records = list(spider_repo.get(
            {"$or": [
                {"last_processed_time": {"$exists": False}},
                {"last_processed_time": {"$lt": one_week_ago}}
            ]},
            limit=BATCH_SIZE
        ))

        # Daha önce başarısız olmuş ama retry hakkı olanları al
        unproc = list(unproc_repo.get(
            {
                "processed_time": {"$lt": twelve_hours_ago},
                "$or": [
                    {"retry_count": {"$exists": False}},
                    {"retry_count": {"$lt": MAX_RETRY_COUNT}}
                ]
            },
            limit=BATCH_SIZE
        ))

        # Aynı URL'leri filtrele
        unproc_urls = {rec["url"] for rec in unproc}
        logger.info("🔁 Retry için alınan URL'ler: %s", list(unproc_urls))

        new_records = [rec for rec in new_records if rec["url"] not in unproc_urls]

        if not new_records and not unproc:
            logger.info("✅ [%d. tur] İşlenecek kayıt kalmadı, çıkılıyor.", iteration)
            break

        logger.info("📦 [%d. tur] Yeni kayıt: %d, Retry: %d", iteration, len(new_records), len(unproc))

        full_input = new_records + unproc
        id_url_map = {rec["_id"]: rec["url"] for rec in full_input}
        task_queue = Queue()
        for rec in full_input:
            task_queue.put((rec["_id"], rec["url"]))

        thread_count = get_dynamic_thread_count()
        batch_results = []
        failed_ids = set()

        while not task_queue.empty():
            batch = [task_queue.get() for _ in range(min(BATCH_SIZE, task_queue.qsize()))]
            result_batch = process_batch(batch, thread_count)

            batch_results.extend([r for r in result_batch if r])
            input_ids = {_id for _id, _ in batch}
            success_ids = {_r["_id"] for _r in result_batch if _r}
            failed_ids.update(input_ids - success_ids)

        # Başarılı kayıtların işlenmesi
        for res in batch_results:
            seo_repo.upsert({"url": res["url"]}, res)
            links_repo.upsert(
                {"url": res["url"]},
                {"url": res["url"], "processed_time": res["processed_at"]}
            )
            spider_repo.update(
                {"_id": res["_id"]},
                {"$set": {"last_processed_time": res["processed_at"]}}
            )
            unproc_repo.delete({"url": res["url"]})



        # Başarısız olanları işaretle
        for _id in failed_ids:
            url = id_url_map.get(_id)
            if not url:
                continue

            results = list(spider_repo.get({"url": url}, limit=1))
            existing = results[0] if results else None

            if existing:
                logger.warning("📄 Başarısız kaydın tam verisi: %s", existing)

            retry_count = 1
            should_retry = True
            last_time = None

            if existing:
                retry_count = existing.get("retry_count", 0) + 1
                last_time = existing.get("processed_time")

                if isinstance(last_time, str):
                    try:
                        last_time = datetime.strptime(last_time, "%Y-%m-%d %H:%M:%S")
                    except Exception as e:
                        logger.warning("⛔ Site %s (%s) için tarih parse edilemedi: %s", _id, url, str(e))
                        last_time = None

                last_time = make_aware(last_time)

                if isinstance(last_time, datetime) and last_time > twelve_hours_ago:
                    should_retry = False

            if not should_retry or retry_count > MAX_RETRY_COUNT:
                if retry_count > MAX_RETRY_COUNT:
                    logger.warning("❌ Site %s (%s) %d+ kez başarısız oldu, atlanıyor.", _id, url, retry_count)
                continue

            unproc_repo.upsert(
                {"url": url},
                {
                    "$set": {
                        "retry_count": retry_count,
                        "processed_time": now
                    },
                    "$setOnInsert": {
                        "url": url,
                        "created_at": now
                    }
                }
            )

            spider_repo.update(
                {"url": url},
                {"$set": {"last_processed_time": now}}
            )

        success_count = len(batch_results)
        fail_count = len(failed_ids)

        logger.info("✅ [%d. tur] Başarıyla işlenen: %d", iteration, success_count)
        logger.info("⚠️ [%d. tur] Başarısız (UNPROCESSABLE'a eklendi veya güncellenmedi): %d", iteration, fail_count)
        logger.info("⏱️ [%d. tur] Süre: %.2f saniye", iteration, (datetime.now(timezone.utc) - start_time).total_seconds())

        iteration += 1

    logger.info("🏁 Tüm işlemler tamamlandı. Toplam süre: %.2f saniye", (datetime.now(timezone.utc) - start_time).total_seconds())
