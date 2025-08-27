
def crawl_donga(driver_path, max_page, bucket_name, prefix):
    """
    동아일보 부동산 뉴스 크롤링 태스크
    """
    from airflow.providers.amazon.aws.hooks.s3 import S3Hook
    from selenium import webdriver
    from selenium.webdriver.chrome.service import Service
    from selenium.webdriver.common.by import By
    from selenium.webdriver.chrome.options import Options
    from selenium.webdriver.support.ui import WebDriverWait
    from selenium.common.exceptions import WebDriverException
    from io import StringIO
    import pandas as pd
    import time
    import re
    from datetime import datetime

    options = Options()
    options.add_argument("--headless")
    options.add_argument("--disable-gpu")
    options.add_argument("--no-sandbox")

    try:
        service = Service(executable_path=driver_path)
        driver = webdriver.Chrome(service=service, options=options)
        wait = WebDriverWait(driver, 5)
    except WebDriverException as e:
        print(f"[FATAL] Failed to initialize WebDriver: {e}")
        return

    article_links = set()
    cutoff_date = 20200901  # 기준 날짜

    # 페이지 순회
    for page in range(1, max_page + 1):
        offset = (page - 1) * 20 + 1
        url = f"https://www.donga.com/news/Economy/RE?p={offset}&prod=news&ymd=&m="
        try:
            driver.get(url)
            print(f" Visiting page {page} -> {url}")
            time.sleep(2)

            links = driver.find_elements(By.CSS_SELECTOR, "a")
            for link in links:
                href = link.get_attribute("href")
                if href and "https://www.donga.com/news/Economy/article/all/" in href:
                    match = re.search(r'/all/(\d{8})/', href)
                    if match:
                        article_date = int(match.group(1))
                        if article_date >= cutoff_date:
                            article_links.add(href)

        except Exception as e:
            print(f" Failed to process page {page}: {e}")

    today_str = datetime.today().strftime("%Y%m%d")  # 오늘 날짜 문자열 "YYYYMMDD"
    filtered_links = []

    for link in article_links:
        match = re.search(r'/(\d{8})/', link)  # /YYYYMMDD/ 추출
        if match and match.group(1) == today_str:
            filtered_links.append(link)

    article_links = filtered_links
    print(f"Total collected article URLs for today: {len(article_links)}")

    # 본문 수집
    article = {}
    for i, url in enumerate(article_links):
        try:
            driver.get(url)
            time.sleep(2)
            section = driver.find_element(By.CSS_SELECTOR, 'section.news_view')
            driver.execute_script("""
                const section = arguments[0];
                const tags = section.querySelectorAll('script, style, iframe, div.a1, div.view_ad06, div.view_m_adA, div.view_m_adB');
                tags.forEach(tag => tag.remove());
            """, section)
            full_text = section.get_attribute('innerText').strip()
            if not full_text:
                full_text = "본문 없음"
                print(f" ({i+1}/{len(article_links)}) Crawled: {url} | 본문 없음")
            else:
                print(f" ({i+1}/{len(article_links)}) Crawled: {url} | {len(full_text)}자 추출")
        except Exception as e:
            full_text = "접근 실패"
            print(f" ({i+1}/{len(article_links)}) URL 접근 실패: {url} | 에러: {e}")

        # URL에서 날짜 추출
        match = re.search(r'/all/(\d{8})/', url)
        date = f"{match.group(1)[:4]}-{match.group(1)[4:6]}-{match.group(1)[6:]}" if match else "Unknown"

        article[url] = {
            'date': date,
            'content': full_text,
            'publisher' : '동아일보'
        }

    driver.quit()
    # DataFrame 변환
    df = pd.DataFrame([
        {"url": url, "date": data["date"], "content": data["content"], "publisher": "동아일보"}
        for url, data in article.items()
    ])

    if df.empty:
        print("No articles found for dong_a.")
        return ''

    csv_buffer = StringIO()
    df.to_csv(csv_buffer, index=False)

    s3_hook = S3Hook(aws_conn_id='s3_conn')
    key = f"{prefix}dong_a.csv"
    s3_hook.load_string(csv_buffer.getvalue(), key=key, bucket_name=bucket_name, replace=True)
    return ''
