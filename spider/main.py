import logging
import socket
import time
import asyncio
from datetime import datetime
from urllib.parse import urlparse
from db_manager import (
    create_indexes,
    get_domain,
    load_existing_data,
    partial_cleanup_queue_urls,
    processed_set,
    mark_as_blocked,
    is_recently_blocked,
    processed_collection,
    remove_url_from_queue_db
)
from queue_manager import (
    local_queue,
    refill_local_queue,
    enqueue_url_batch,
)
from http_client import fetch, fetch_turkce, is_excluded_domain
from pymongo.errors import DuplicateKeyError, PyMongoError

logger = logging.getLogger("AsyncSpider")

async def unblocker(interval_seconds: int = 60):
    from db_manager import bot_dict, remove_expired_block
    from queue_manager import enqueue_url
    try:
        while True:
            await asyncio.sleep(interval_seconds)
            now = datetime.now()
            expired = [u for u, t in bot_dict.items() if (now - t).total_seconds() > 1800]
            if expired:
                for url in expired:
                    await remove_expired_block(url)
                    if url not in processed_set:
                        #logger.info(f"[UNBLOCK] {url} blok süresi doldu, DB kuyruğuna eklendi.")
                        await enqueue_url(url,0)
    except asyncio.CancelledError:
        logger.info("Unblocker sonlandırılıyor.")
    except Exception as e:
        logger.error(f"Unblocker hatası:", exc_info=True)

'''async def worker(session, worker_id: int, idle_limit: int = 3):
    idle_count = 0
    while True:
        if local_queue.empty():
            await refill_local_queue(batch_size=200)
            if local_queue.empty():
                idle_count += 1
                logger.debug(f"Worker-{worker_id} kuyruk boş (idle_count={idle_count}).")
                if idle_count >= idle_limit:
                    logger.info(f"Worker-{worker_id} idle limit aşıldı, duruyor.")
                    return
                else:
                    await asyncio.sleep(5)
                    continue
            else:
                idle_count = 0
        url = await local_queue.get()
        local_queue.task_done()
        if is_excluded_domain(url):
            logger.info(f"[BLOCK] Worker-{worker_id} -> Domain engellendi: {url}")
            continue
        #links = await fetch(session, url)
        
        links = await fetch_turkce(session, url)  # Yeni satır (ara fonksiyon kullanılır)
        if links is None:
            await mark_as_blocked(url)
        else:
            processed_set.add(url)
            try:
                await processed_collection.insert_one({"url": url})
                logger.info(f"[INSERT] Worker-{worker_id} -> {url}, Taranan: {len(processed_set)}")
            except DuplicateKeyError:
                pass
            except PyMongoError as e:
                logger.error(f"processed_sites eklenirken hata: {url} ->", exc_info=True)
            new_urls = []
            for link in links:
                if link not in processed_set and not await is_recently_blocked(link):
                    new_urls.append(link)
            if new_urls:
                await enqueue_url_batch(new_urls)'''
'''2                
MAX_FOREIGN_DEPTH = 1  # Yabancı siteler için maksimum dallanma derinliği

async def worker(session, worker_id: int, idle_limit: int = 3):
    idle_count = 0
    while True:
        if local_queue.empty():
            await refill_local_queue(batch_size=200)
            if local_queue.empty():
                idle_count += 1
                if idle_count >= idle_limit:
                    logger.info(f"Worker-{worker_id} idle limit aşıldı, duruyor.")
                    return
                await asyncio.sleep(5)
                continue
            else:
                idle_count = 0

        url, depth = await local_queue.get()
        local_queue.task_done()

        if is_excluded_domain(url):
            logger.info(f"[BLOCK] Worker-{worker_id} -> Domain engellendi: {url}")
            continue

        links = await fetch(session, url)
        if links is None:
            await mark_as_blocked(url)
            continue

        # Dil kontrolü yap
        is_turkce = await fetch_turkce(session, url)

        if is_turkce:
            processed_set.add(url)
            try:
                await processed_collection.insert_one({"url": url})
                logger.info(f"[INSERT-TR] Worker-{worker_id} -> {url}")
            except DuplicateKeyError:
                pass
            except PyMongoError as e:
                logger.error(f"processed_sites eklenirken hata: {url} ->", exc_info=True)

            # Türkçe sitelerden tüm linkleri sınırsız derinlikle ekle
            next_depth = 0
        else:
            logger.info(f"[NON-TR] Worker-{worker_id} -> {url} (depth={depth})")
            if depth >= MAX_FOREIGN_DEPTH:
                continue  # yabancı sitelerde dallanmayı durdur
            next_depth = depth + 1

        new_urls = []
        for link in links:
            if link not in processed_set and not await is_recently_blocked(link):
                new_urls.append((link, next_depth))
        if new_urls:
            await enqueue_url_batch(new_urls)


async def async_spider(initial_sites: list[str], concurrency: int = 15):
    import aiohttp
    from queue_manager import enqueue_url
    await create_indexes()
    await load_existing_data()
    for site in initial_sites:
        if site not in processed_set and not await is_recently_blocked(site):
            await enqueue_url(site)
    connector = aiohttp.TCPConnector(limit=80, limit_per_host=30)
    async with aiohttp.ClientSession(connector=connector) as session:
        ub_task = asyncio.create_task(unblocker(interval_seconds=60))
        workers = []
        for i in range(concurrency):
            w = asyncio.create_task(worker(session, worker_id=i, idle_limit=13))
            workers.append(w)
        start_time = time.time()
        done, pending = await asyncio.wait([ub_task, *workers], return_when=asyncio.FIRST_EXCEPTION)
        for t in pending:
            t.cancel()
            try:
                await t
            except asyncio.CancelledError:
                pass
        ub_task.cancel()
        try:
            await ub_task
        except asyncio.CancelledError:
            logger.info("Unblocker iptal edildi.")
        elapsed = time.time() - start_time
        from db_manager import processed_collection
        total_count = await processed_collection.count_documents({})
        logger.info(f"[DONE] Tarama bitti. Süre: {elapsed:.2f}s, Toplam işlenen: {total_count}")
'''

async def remove_from_queues(url: str) -> None:

    deleted_count = await remove_url_from_queue_db(url)

    temp_list = []
    while not local_queue.empty():
        try:
            item = local_queue.get_nowait()  # (url, depth) gibi bir tuple
            if item[0] != url:
                temp_list.append(item)
        except asyncio.QueueEmpty:
            break
    for elem in temp_list:
        local_queue.put_nowait(elem)

async def get_ip(url: str) -> str | None:
    """
    URL içindeki domain'i alarak asenkron şekilde IP adresini çözümler.
    """
    try:
        parsed = urlparse(url)
        domain = parsed.hostname  # Sadece hostname (www.example.com) al
        if not domain:
            return None  # Geçersiz URL

        return await asyncio.to_thread(socket.gethostbyname, domain)
    except (socket.gaierror, ValueError, TypeError) as e:
        return None  # Çözümlenemeyen domain
    
MAX_FOREIGN_DEPTH = 2  # Yabancı siteler için maksimum dallanma derinliği

async def worker(session, worker_id: int, idle_limit: int = 3):
    idle_count = 0
    while True:
        if local_queue.empty():
            await refill_local_queue(batch_size=200)
            if local_queue.empty():
                idle_count += 1
                if idle_count >= idle_limit:
                    logger.info(f"Worker-{worker_id} idle limit aşıldı, duruyor.")
                    return
                await asyncio.sleep(5)
                continue
            else:
                idle_count = 0

        url, depth = await local_queue.get()
        local_queue.task_done()

        if is_excluded_domain(url):
            continue

        links = await fetch(session, url)
        if links is None:
            # Eğer fetch None döndürdüyse, önce CAPTCHA blok kontrolü yapıyoruz.
            from db_manager import is_captcha_blocked
            if await is_captcha_blocked(url):
                # CAPTCHA nedeniyle bloklandı; normal bloklamaya gerek yok.
                continue
            else:
                await mark_as_blocked(url)
                continue

        # Dil kontrolü
        is_turkce = await fetch_turkce(session, url)

        if is_turkce:
            await remove_from_queues(url)
            server_ip = await get_ip(url)

            try:
                await processed_collection.insert_one({
                    "url": url,
                    "server_ip": server_ip  # IP adresini ekledik
                })
                logger.info(f"[INSERT-TR] Worker-{worker_id} -> {url}, IP={server_ip}")
            except DuplicateKeyError:
                pass
            except PyMongoError:
                logger.error(f"processed_sites eklenirken hata: {url} ->", exc_info=True)

            next_depth = 0
        else:
            if depth >= MAX_FOREIGN_DEPTH:
                await remove_from_queues(url)
                continue
            next_depth = depth + 1

        new_urls = []
        for link in links:
            if link not in processed_set and not await is_recently_blocked(link):
                new_urls.append((link, next_depth))
        if new_urls:
            await enqueue_url_batch(new_urls)

async def schedule_partial_cleanup(interval_seconds: int = 100, batch_size: int = 10000):
    while True:
        await partial_cleanup_queue_urls(batch_size=batch_size)
        await asyncio.sleep(interval_seconds)

async def async_spider(initial_sites: list[str], concurrency: int = 25):
    import aiohttp
    from queue_manager import enqueue_url
    await create_indexes()
    await load_existing_data()
    for site in initial_sites:
        if site not in processed_set and not await is_recently_blocked(site):
            await enqueue_url((site, 0))
    connector = aiohttp.TCPConnector(limit=80, limit_per_host=30)
    async with aiohttp.ClientSession(connector=connector) as session:
        ub_task = asyncio.create_task(unblocker(interval_seconds=60))
        cleanup_task = asyncio.create_task(schedule_partial_cleanup(
            interval_seconds=100,
            batch_size=10000
        ))
        workers = []
        for i in range(concurrency):
            w = asyncio.create_task(worker(session, worker_id=i, idle_limit=13))
            workers.append(w)
        start_time = time.time()
        done, pending = await asyncio.wait(
            [ub_task, cleanup_task, *workers],
            return_when=asyncio.FIRST_EXCEPTION
        )
        for t in pending:
            t.cancel()
            try:
                await t
            except asyncio.CancelledError:
                pass
        ub_task.cancel()
        try:
            await ub_task
        except asyncio.CancelledError:
            logger.info("Unblocker iptal edildi.")

        elapsed = time.time() - start_time
        total_count = await processed_collection.count_documents({})
        logger.info(f"[DONE] Tarama bitti. Süre: {elapsed:.2f}s, Toplam işlenen: {total_count}")


def get_initial_sites(filepath: str, count: int = 10) -> list[str]:
    sites = []
    try:
        with open(filepath, 'r+', encoding='utf-8') as file:
            for _ in range(count):
                line = file.readline()
                if not line:
                    break
                sites.append(line.strip())
            remaining = file.readlines()
            file.seek(0)
            file.writelines(remaining)
            file.truncate()
        logger.info(f"{len(sites)} adet site okundu.")
    except Exception as e:
        logger.error(f"[FILE-ERROR]", exc_info=True)
    return sites

def main():
    import asyncio
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(name)s - %(message)s"
    )
    filepath = "search_results.txt"
    initial_sites = get_initial_sites(filepath, 10)
    logger.info(f"Başlangıç siteleri: {initial_sites}")
    asyncio.run(async_spider(initial_sites, concurrency=15))

if __name__ == "__main__":
    main()
