from config.settings import (
    PROCESSED_COLLECTION_SPIDER,
    PROCESSED_SITES_SEO,
    PROCESSED_SITES_SEO_LINKS,
    UNPROCESSABLE_SITES
)
from storage.mongo_context import MongoDbContext
from storage.repository import Repository
from datetime import datetime, timedelta
from queue import Queue
from tasks.processor import process_batch, get_dynamic_thread_count
import logging

# Log ayarları
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)

BATCH_SIZE = 50
MAX_RETRY_COUNT = 3

def main():
    logger.info("🚀 İşlem başlatıldı.")
    start_time = datetime.now()

    # Mongo bağlantısı ve repository'ler
    mongo = MongoDbContext()
    spider_repo = Repository(PROCESSED_COLLECTION_SPIDER, mongo)
    seo_repo    = Repository(PROCESSED_SITES_SEO, mongo)
    links_repo  = Repository(PROCESSED_SITES_SEO_LINKS, mongo)
    unproc_repo = Repository(UNPROCESSABLE_SITES, mongo)

    iteration = 1

    while True:
        logger.info("🔄 [%d. tur] Yeni kayıtlar alınıyor...", iteration)

        now = datetime.now()
        now_str = now.strftime("%Y-%m-%d %H:%M:%S")
        one_week_ago = now - timedelta(weeks=1)
        twelve_hours_ago = now - timedelta(hours=12)

        # Yeni işlenmemiş kayıtları al
        new_records = list(spider_repo.get(
            {"$or": [
                {"last_processed_time": {"$exists": False}},
                {"last_processed_time": {"$lt": one_week_ago}}
            ]},
            limit=BATCH_SIZE
        ))

        # UNPROCESSABLE kayıtları
        unproc = list(unproc_repo.get(
            {
                "processed_time": {"$lt": twelve_hours_ago.strftime("%Y-%m-%d %H:%M:%S")},
                "$or": [
                    {"retry_count": {"$exists": False}},
                    {"retry_count": {"$lt": MAX_RETRY_COUNT}}
                ]
            },
            limit=BATCH_SIZE
        ))

        if not new_records and not unproc:
            logger.info("✅ [%d. tur] İşlenecek kayıt kalmadı, çıkılıyor.", iteration)
            break

        logger.info("📦 [%d. tur] Yeni kayıt: %d, Retry: %d", iteration, len(new_records), len(unproc))

        # Kuyruğa ekleme
        full_input = new_records + unproc
        id_url_map = {}
        task_queue = Queue()
        for rec in full_input:
            id_url_map[rec["_id"]] = rec["url"]
            task_queue.put((rec["_id"], rec["url"]))

        # Batch işleme
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

        # Başarılı kayıtlar
        for res in batch_results:
            seo_repo.save(res)
            links_repo.save({
                "url": res["url"],
                "processed_time": now_str
            })
            spider_repo.update(
                {"_id": res["_id"]},
                {"$set": {"last_processed_time": now}}
            )
            unproc_repo.delete({"_id": res["_id"]})

        # Başarısız kayıtlar
        for _id in failed_ids:
            url = id_url_map.get(_id)
            if url:
                results = list(unproc_repo.get({"_id": _id}, limit=1))
                existing = results[0] if results else None
                retry_count = existing.get("retry_count", 0) + 1 if existing else 1

                if retry_count > MAX_RETRY_COUNT:
                    logger.warning("❌ Site %s (%s) %d+ kez başarısız oldu, atlanıyor.", _id, url, retry_count)
                    continue

                unproc_repo.upsert(
                    {"_id": _id},
                    {
                        "url": url,
                        "processed_time": now_str,
                        "retry_count": retry_count
                    }
                )

        # Tur özeti
        success_count = len(batch_results)
        fail_count = len(failed_ids)

        logger.info("✅ [%d. tur] Başarıyla işlenen: %d", iteration, success_count)
        logger.info("⚠️ [%d. tur] Başarısız (UNPROCESSABLE'a eklendi): %d", iteration, fail_count)
        logger.info("⏱️ [%d. tur] Süre: %.2f saniye", iteration, (datetime.now() - start_time).total_seconds())

        iteration += 1

    logger.info("🏁 Tüm işlemler tamamlandı. Toplam süre: %.2f saniye", (datetime.now() - start_time).total_seconds())
