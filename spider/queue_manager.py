import logging
import asyncio
from typing import List

from pymongo.errors import PyMongoError, DuplicateKeyError,BulkWriteError
from db_manager import queue_collection
from http_client import is_excluded_domain
import random

logger = logging.getLogger("AsyncSpider")

local_queue = asyncio.Queue(maxsize=2000)
'''async def enqueue_url(url: str):
    from http_client import logger  # or simply use the same logger
    if is_excluded_domain(url):
        logger.debug(f"Domain engellendi: {url}")
        return
    try:
        await queue_collection.insert_one({"url": url})
    except DuplicateKeyError:
        pass
    except PyMongoError as e:
        logger.warning(f"Kuyruğa eklenirken hata: {url} ", exc_info=True)'''
        
async def enqueue_url(url_depth: tuple[str, int]):
    url, depth = url_depth
    if is_excluded_domain(url):
        logger.debug(f"Domain engellendi: {url}")
        return
    try:
        await queue_collection.insert_one({"url": url, "depth": depth})
    except DuplicateKeyError:
        pass
    except PyMongoError as e:
        logger.warning(f"Kuyruğa eklenirken hata: {url}", exc_info=True)


'''async def enqueue_url_batch(urls: List[str]):
    from http_client import logger  
    valid_docs = []
    for u in urls:
        if not is_excluded_domain(u):
            valid_docs.append({"url": u})
    if not valid_docs:
        return
    try:
          await queue_collection.insert_many(valid_docs, ordered=False)
    except BulkWriteError:
          # Duplicate key hatalarını yok sayıyoruz
          pass
    except PyMongoError as e:
        logger.warning(f"Kuyruğa toplu eklenirken hata:", exc_info=True)'''
        
async def enqueue_url_batch(url_depth_list: List[tuple[str, int]]):
    valid_docs = []
    for url, depth in url_depth_list:
        if not is_excluded_domain(url):
            valid_docs.append({"url": url, "depth": depth})
    if not valid_docs:
        return
    try:
        await queue_collection.insert_many(valid_docs, ordered=False)
    except BulkWriteError:
        # Duplicate hatalarını önemseme
        pass
    except PyMongoError as e:
        logger.warning("Kuyruğa toplu eklenirken hata:", exc_info=True)


'''async def dequeue_url_from_db_batch(batch_size: int = 50) -> List[str]:
    urls = []
    try:
        cursor = queue_collection.find({}).limit(batch_size)
        async for doc in cursor:
            urls.append(doc["url"])
        if urls:
            await queue_collection.delete_many({"url": {"$in": urls}})
    except PyMongoError as e:
        logger.warning(f"Kuyruktan batch çekilirken hata: ", exc_info=True)
    return urls

async def refill_local_queue(batch_size: int = 200) -> None:
        while not local_queue.full():
            batch = await dequeue_url_from_db_batch(batch_size=batch_size)
            if not batch:
                break
            for url in batch:
                await local_queue.put(url)
                if local_queue.full():
                    break'''
                    
async def dequeue_url_from_db_batch(batch_size: int = 50) -> List[tuple[str, int]]:
    url_depths = []
    try:
        cursor = queue_collection.find({}).limit(batch_size)
        async for doc in cursor:
            depth = doc.get("depth", 0)
            url_depths.append((doc["url"], depth))
        if url_depths:
            await queue_collection.delete_many({"url": {"$in": [u for u, _ in url_depths]}})
    except PyMongoError as e:
        logger.warning("Kuyruktan batch çekilirken hata:", exc_info=True)
    return url_depths

async def refill_local_queue(batch_size: int = 400) -> None:
    while not local_queue.full():
        batch = await dequeue_url_from_db_batch(batch_size=batch_size)
        if not batch:
            break
        for url_depth in batch:
            await local_queue.put(url_depth)
            if local_queue.full():
                break
