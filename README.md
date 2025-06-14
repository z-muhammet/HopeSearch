HopeSearch is a collection of tools for crawling Turkish web sites, extracting
metadata and textual information, caching keyword based search results and
ranking pages. MongoDB is used for storage and several scripts are provided for
data collection and processing.

## Repository layout

```
search_module/   # keyword search and cache generation
spider/          # asynchronous crawler implementation
OrderForHOPE/    # page scoring and ranking utilities
pythonProject/   # data processing and MongoDB helpers
```

## Requirements

* Python 3.10+
* MongoDB (Docker compose configuration provided)
* Optional: Playwright, Gensim and other dependencies for the crawler

The `pythonProject/docker-compose.yml` file contains a simple MongoDB setup with
a mongo-express instance for inspection.

```yaml
    services:
      mongodb:
        image: mongo:latest
        ports:
          - "27017:27017"
      mongo-express:
        image: mongo-express
        ports:
          - "8081:8081"
```

Install Python dependencies as required by the individual scripts.

## Crawling the web

The asynchronous crawler is implemented under `spider/`. It uses MongoDB to
track processed URLs and queued links. `spider/main.py` orchestrates workers that
fetch pages and apply language detection to keep only Turkish content.

```python
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
```

Start the crawler by providing an initial list of sites in
`spider/search_results.txt` and executing `python spider/main.py`.

## Building the keyword cache

`search_module/cache_builder.py` iterates over a list of Turkish words and runs
the search script for each entry. Each run checks MongoDB for cached results
before analysing all processed pages.

```python
    print(f"'{word_dataset_file}' dosyasındaki kelimeler taranıyor ve '{search_script}' çalıştırılıyor...")
    logging.info(f"'{word_dataset_file}' dosyasındaki kelimeler taranıyor ve '{search_script}' çalıştırılıyor...")
    try:
        with open(word_dataset_file, "r", encoding="utf-8") as f:
            all_words = [line.strip() for line in f if line.strip()]
        total_words = len(all_words)
        print(f"Toplam {total_words} kelime bulundu. İşleme başlanıyor...")
```

The actual search logic resides in `search_module/search.py`. It loads a Word2Vec
model (`trmodel`) and generates semantic variants of the search term. Results are
cached in the `search_keyword_cache` collection and also exported to
`processed_dataset.json`.

```python
word_vectors = KeyedVectors.load_word2vec_format('trmodel', binary=True)
...
cache_result = db.search_keyword_cache.find_one({"keyword": query_keyword})
if cache_result:
    cached_result_count = len(cache_result.get("results", []))
    if cached_result_count == current_total_sites_in_db and current_total_sites_in_db > 0:
        print("Önbellekten sonuç getirildi (güncel).")
        with open("processed_dataset.json", "w", encoding="utf-8") as f:
            json.dump(processed_cache_results, f, ensure_ascii=False, indent=4)
        sys.exit(0)
```

If no cache is available the script computes keyword matches across all stored
pages and saves the processed data back to MongoDB.

```python
for i, record in enumerate(input_data):
    url = record.get("url", "URL_BULUNAMADI")
    h1_kw_present = keyword_in_field(record.get("h1_keyword", ""), search_variants, url)
    processed_record = {
        "_id": str(record["_id"]) if "_id" in record else None,
        "url": url,
        "h1_keyword": h1_kw_present,
    }
```

## Ranking pages

The `OrderForHOPE` directory contains simple utilities for filtering pages and
scoring them with weighted criteria. Weights are defined in
`OrderForHOPE/src/weights.py`.

```python
preliminary_weights = {
    'h1_keyword': 6,
    'h2_keyword': 4,
    'load_time': 4,
    'mobile_compatibility': 3,
}

all_weights = {
    'h1_keyword': 10,
    'h2_keyword': 7,
    'h3_keyword': 5,
    'title_keyword': 9,
    'content_keyword_match': 8,
    'meta_keyword_density': 7,
    'strong_texts': 4,
    'underline_texts': 2,
    'load_time': 6,
    'last_update_year': 5,
    'last_update_month': 4.8,
    'last_update_day': 4.7,
    'mobile_compatibility': 3,
    'ssl_certificate': 2,
    'site_age': 1
}
```

`weightAndOrder.py` uses these weights to compute a score per page and prints a
sorted result list.

## Utilities and helper scripts

The `pythonProject` folder hosts assorted utilities for data processing. The
`MongoDbContext` class wraps basic MongoDB operations:

```python
class MongoDbContext:
    def __init__(self, url, db_name):
        self.url = url
        self.db_name = db_name

    def get_datas_from_mongodb(self, collection_name, query={}, limit=0):
        client = MongoClient(self.url)
        database = client[self.db_name]
        collection = database[collection_name]
        cursor = collection.find(query)
        if limit > 0:
            cursor = cursor.limit(limit)
        records = list(cursor)
        client.close()
        return records
```

Other scripts within this directory perform tasks such as tagging pages with
additional metadata (`main2.py`) and removing duplicate records from MongoDB
(`duplicate.py`).

## License

This project is released under the MIT License.
