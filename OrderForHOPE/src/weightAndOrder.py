import threading
import queue
from weights import all_weights
from weightProcess import filtered_pages
from datetime import datetime

# Site type henüz kullanılmıyor
result_queue = queue.Queue()

def calculate_page_score(page, weights, result_queue):
    try:
        current_date = datetime.now()
        score = 0

        # Booleans
        score += weights.get('h1_keyword', 0) if page.get('h1_keyword', False) else 0
        score += weights.get('title_keyword', 0) if page.get('title_keyword', False) else 0
        score += weights.get('mobile_compatibility', 0) if page.get('mobile_compatibility', False) else 0
        score += weights.get('ssl_certificate', 0) if page.get('ssl_certificate', False) else 0

        # Sayısal oranlar
        score += weights.get('content_keyword_match', 0) * page.get('content_keyword_match', 0)
        score += weights.get('meta_keyword_density', 0) * page.get('meta_keyword_density', 0)

        # Load time (ters orantı)
        load_time = page.get('load_time', None)
        if load_time and load_time > 0:
            score += weights.get('load_time', 0) / load_time

        # Last update difference
        year = page.get('last_update_year')
        month = page.get('last_update_month', 1)
        day = page.get('last_update_day', 1)
        if year:
            try:
                last_update_date = datetime(year, month, day)
                days_difference = (current_date - last_update_date).days
                score += weights.get('last_update_year', 0) / (days_difference + 1)
            except Exception as e:
                print(f"[WARN] Tarih hesaplama hatası: {e}")

        # Site age (ters orantı)
        site_age = page.get('site_age')
        if isinstance(site_age, (int, float)) and site_age >= 0:
            score += weights.get('site_age', 0) / (site_age + 1)

        result_queue.put({'score': score, 'url': page.get('url', 'URL Bilinmiyor')})

    except Exception as e:
        print(f"[ERROR] Sayfa puanlamada hata oluştu: {e}")

# Tüm sayfalar için iş parçacıkları oluştur
threads = []
for page in filtered_pages:
    thread = threading.Thread(target=calculate_page_score, args=(page, all_weights, result_queue))
    threads.append(thread)
    thread.start()

# Tüm iş parçacıklarının tamamlanmasını bekle
for thread in threads:
    thread.join()

# Kuyruktan skorlara erişip listeye ekleme
score_list = []
while not result_queue.empty():
    score_list.append(result_queue.get())

# Skorları sıralama ve sonuçları yazdırma
try:
    sorted_list = sorted(score_list, key=lambda x: x['score'], reverse=True)
    print("Sorted List:")
    for idx, page in enumerate(sorted_list):
        print(f"Site {idx + 1}: Score: {page['score']}, URL: {page['url']}")
except Exception as e:
    print(f"Bir hata oluştu: {e}")
