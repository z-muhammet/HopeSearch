from weights import preliminary_weights
from filtering import filtered_pages

def calculate_preliminary_score(pageList, weights):
    for page in pageList:
        score=0
        score += weights['h1_keyword'] if page.get('h1_keyword', False) else 0
        score += weights['h2_keyword'] if page.get('h1_keyword', False) else 0
        score += weights['load_time'] / page.get('load_time', 1)  
        score += weights['mobile_compatibility'] if page.get('mobile_compatibility', False) else 0
        if 10> score >=0: 
            pageList.remove(page)
    return pageList

if len(filtered_pages) >= 1000:
    filtered_pages=calculate_preliminary_score(filtered_pages,preliminary_weights)

print("Filtrelenmiş sayfalar:")
for idx, page in enumerate(filtered_pages):
    print(f"Site {idx + 1}: {page['url']}, H1 Keyword: {page['h1_keyword']}, Content Match: {page['content_keyword_match']}")