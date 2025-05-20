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

def main():
    logger.info("🚀 İşlem başlatıldı.")
    start_time = datetime.now()

    # Mongo bağlantısı ve repository'ler
    mongo = MongoDbContext()
    spider_repo = Repository(PROCESSED_COLLECTION_SPIDER, mongo)
    seo_repo    = Repository(PROCESSED_SITES_SEO, mongo)
    links_repo  = Repository(PROCESSED_SITES_SEO_LINKS, mongo)
    unproc_repo = Repository(UNPROCESSABLE_SITES, mongo)

    # Tarih hesaplamaları
    now = datetime.now()
    now_str = now.strftime("%Y-%m-%d %H:%M:%S")
    one_week_ago = now - timedelta(weeks=1)
    twelve_hours_ago = now - timedelta(hours=12)

    # Yeni işlenmemiş kayıtları al
    new_records = spider_repo.get(
        {"$or": [
            {"last_processed_time": {"$exists": False}},
            {"last_processed_time": {"$lt": one_week_ago}}
        ]},
        limit=50
    )

    # UNPROCESSABLE kayıtları (12 saatten eski olanlar)
    unproc = list(unproc_repo.get(
        {"processed_time": {"$lt": twelve_hours_ago.strftime("%Y-%m-%d %H:%M:%S")}},
        limit=50
    ))

    logger.info("🧾 Yeni alınan kayıt sayısı: %d", len(new_records))
    logger.info("♻️ 12 saatten eski UNPROCESSABLE sayısı: %d", len(unproc))

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
        batch = [task_queue.get() for _ in range(min(50, task_queue.qsize()))]
        result_batch = process_batch(batch, thread_count)

        # Başarılı olanlar
        batch_results.extend([r for r in result_batch if r])

        # Başarısızları yakala
        input_ids = {_id for _id, _ in batch}
        success_ids = {_r["_id"] for _r in result_batch if _r}
        failed_ids.update(input_ids - success_ids)

    # Kayıtları veritabanına yaz
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
        unproc_repo.delete({"_id": res["_id"]})  # varsa eski UNPROC'dan çıkar

    for _id in failed_ids:
        url = id_url_map.get(_id)
        if url:
            unproc_repo.save({
                "_id": _id,
                "url": url,
                "processed_time": now_str
            })

    # Özet log
    success_count = len(batch_results)
    fail_count = len(failed_ids)

    logger.info("✅ Başarıyla işlenen site sayısı: %d", success_count)
    logger.info("⚠️ İşlenemeyen ve tekrar UNPROCESSABLE'a eklenen site sayısı: %d", fail_count)
    logger.info("📦 Toplam giriş (yeni + retry): %d", len(full_input))
    logger.info("⏱️ Toplam geçen süre: %s saniye", (datetime.now() - start_time).total_seconds())

    logger.info("🎯 İşlem tamamlandı.\n")

