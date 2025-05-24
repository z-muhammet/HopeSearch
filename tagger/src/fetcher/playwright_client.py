from playwright.sync_api import sync_playwright

def fetch_and_parse(url):
    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        page = browser.new_page(ignore_https_errors=True)
        page.goto(url, timeout=60000)
        page.wait_for_timeout(5000)
        html_content = page.content()
        browser.close()
        return html_content

