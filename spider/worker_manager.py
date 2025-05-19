import asyncio
import logging
from db_manager import mark_as_blocked, processed_set, is_recently_blocked
from queue_manager import local_queue, refill_local_queue, enqueue_url_batch
from http_client import fetch
from pymongo.errors import DuplicateKeyError, PyMongoError

logger = logging.getLogger("AsyncSpider")

ACTIVE_WORKERS = set()

async def worker(session, worker_id: int, worker_manager_event: asyncio.Event, idle_limit: int = 3):
    """Her worker bir siteyi işler ve kuyruğu doldurur. Kuyruk boşsa belirli bir süre bekleyip kapanır."""
    global ACTIVE_WORKERS
    ACTIVE_WORKERS.add(worker_id)

    idle_count = 0
    while True:
        async with local_queue_lock:
            if local_queue.empty():
                await refill_local_queue(batch_size=200)
                if local_queue.empty():
                    idle_count += 1
                    logger.debug(f"Worker-{worker_id} boşta (idle_count={idle_count}).")

                    if idle_count >= idle_limit:
                        logger.info(f"Worker-{worker_id} idle limit aşıldı, sonlandırılıyor.")
                        ACTIVE_WORKERS.remove(worker_id)
                        worker_manager_event.set()  # Yeni worker başlatılması için sinyal gönder
                        return
                    else:
                        await asyncio.sleep(2)
                        continue
                else:
                    idle_count = 0  # Kuyruk doldu, idle sıfırla

            url = await local_queue.get()
            local_queue.task_done()

        links = await fetch(session, url)
        if links is None:
            await mark_as_blocked(url)
        else:
            processed_set.add(url)
            try:
                await processed_collection.insert_one({"url": url})
                logger.info(f"[INSERT] Worker-{worker_id} -> {url}, Taranan: {len(processed_set)}")
            except Exception as e:
                logger.error(f"processed_sites eklenirken hata: {url} -> {e}", exc_info=True)

            new_urls = [link for link in links if link not in processed_set and not await is_recently_blocked(link)]
            if new_urls:
                await enqueue_url_batch(new_urls)


async def worker_manager(session, max_workers: int):
    global ACTIVE_WORKERS
    worker_manager_event = asyncio.Event()  # Worker kapanınca tetiklenecek event
    workers = {}

    # Başlangıçta max_workers kadar worker başlat
    for i in range(max_workers):
        worker_id = len(ACTIVE_WORKERS)
        task = asyncio.create_task(worker(session, worker_id, worker_manager_event))
        workers[worker_id] = task
        ACTIVE_WORKERS.add(worker_id)

    while True:
        await asyncio.sleep(3)  # Sürekli kontrol etmek yerine 3 saniye bekle
        active_count = len(ACTIVE_WORKERS)
        
        # Log: Kaç worker aktif çalışıyor?
        logger.info(f"Aktif worker sayısı: {active_count}/{max_workers}")

        # Eğer worker sayısı azaldıysa yenisini başlat
        if active_count < max_workers:
            new_worker_id = max(ACTIVE_WORKERS) + 1 if ACTIVE_WORKERS else 0
            task = asyncio.create_task(worker(session, new_worker_id, worker_manager_event))
            workers[new_worker_id] = task
            ACTIVE_WORKERS.add(new_worker_id)
            logger.info(f"Yeni worker başlatıldı: Worker-{new_worker_id}")

        # Eğer tüm workerlar çalışıyorsa ve queue boşsa, refill_local_queue çağrılır
        if local_queue.empty():
            await refill_local_queue(batch_size=200)

        await asyncio.sleep(1)  # Yeni worker oluşturmadan önce bekleme süresi