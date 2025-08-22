from airflow import DAG
from airflow.decorators import task
from airflow.operators.empty import EmptyOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from datetime import datetime, timedelta 
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.support.ui import WebDriverWait, Select
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import (
    NoSuchElementException,
    StaleElementReferenceException,
    TimeoutException,
    WebDriverException,
    ElementClickInterceptedException,
)
from io import StringIO
import pandas as pd
import csv, time, os , re

dag_owner = 'Ian Kim'

default_args = {'owner': dag_owner,
        'depends_on_past': False,
        #'retries': 2,
        #'retry_delay': timedelta(seconds=3)
        }

DRIVER_PATH = '/usr/bin/chromedriver'
MAX_PAGE = 1
BUCKET_NAME = "ian-geonewsapt"

with DAG(dag_id='crawling_news',
        default_args=default_args,
        description='뉴스 크롤링',
        start_date=datetime(2019,1,1),
        #schedule='* * * * *',
        catchup=False,
        tags=['.']
) as dag:

    @task
    def chosun():

        options = Options() 
        # GUI 없이 실행 - 백엔드/서버 자동화
        options.add_argument("--headless")
        # GPU 가속 기능 off - 안정성 개선
        options.add_argument("--disable-gpu")
        # Chrome을 sandbox 없이 실행함 - 권한 오류 회피(주의)
        options.add_argument("--no-sandbox")

        try:
            service = Service(executable_path=DRIVER_PATH)
            driver = webdriver.Chrome(service=service, options=options)
            wait = WebDriverWait(driver, 5)
        except WebDriverException as e:
            print(f"[FATAL] Failed to initialize WebDriver: {e}")
            return

        try:
            driver.get("https://www.chosun.com/economy/real_estate/?page=1")
        except Exception as e:
            print(f"[FATAL] Failed to load initial page: {e}")
            driver.quit()
            return

        PAGES_PER_SCREEN = 10
        current = 1
        hrefs = []

        # URL 수집
        while current <= MAX_PAGE:
            for i in range(current, min(current + PAGES_PER_SCREEN, MAX_PAGE + 1)):
                try:
                    page_button = driver.find_element(By.ID, str(i))
                    page_button.click()
                    print(f"[+] Clicked page {i}")
                    time.sleep(2)
                except NoSuchElementException: # 지정한 요소를 찾을 수 없을 때 발생
                    print(f" Page button {i} not found.")
                    continue
                except Exception as e:
                    print(f" Unexpected error clicking page {i}: {e}")
                    continue

                try:
                    links = driver.find_elements(By.CSS_SELECTOR, "a")
                    for link in links:
                        try:
                            href = link.get_attribute("href")
                            if href:
                                hrefs.append(href)
                        except StaleElementReferenceException: # 이미 찾은 웹 요소가 DOM에서 사라져 더 이상 유요하지 못한 경우
                            continue  # 요소가 사라진 경우 무시
                except Exception as e: # 특정 예외 외에도 예상치 못한 모든 예외를 포괄적으로 처리함.
                    print(f" Failed to extract links on page {i}: {e}")
                    continue

            # 다음 페이지 버튼 클릭
            if current + PAGES_PER_SCREEN <= MAX_PAGE:
                try:
                    next_page_btn = driver.find_element(
                        By.XPATH,
                        '//*[@id="main"]/div[2]/section/div/div/div/div[21]/div/div[3]/button'
                    )
                    next_page_btn.click()
                    print("[→] Clicked next page button")
                    time.sleep(2)
                except NoSuchElementException: # 지정한 요소를 찾을 수 없을 때 발생
                    print(" Next page button not found.")
                except Exception as e: # 지정된 예외 말고도 모든 예외 지정
                    print(f" Failed to click next page button: {e}")

            current += PAGES_PER_SCREEN

        # 중복 제거
        article_links = list(set([href for href in hrefs if "/economy/real_estate/20" in href]))
        print(f"🔗 Total collected article URLs: {len(article_links)}")

        article = {}

        for i, url in enumerate(article_links):
            try:
                driver.get(url)
                time.sleep(2)

                # 본문 추출
                section = driver.find_element(By.CSS_SELECTOR, 'section.article-body')
                paragraphs = section.find_elements(By.TAG_NAME, 'p')
                full_text = "\n".join(p.text.strip() for p in paragraphs if p.text.strip())

                # URL에서 날짜 추출
                parts = url.split('/')
                year, month, day = parts[5], parts[6], parts[7]  # /economy/real_estate/YYYY/MM/DD/
                date = f"{year}-{month}-{day}"

                # 딕셔너리 저장
                article[url] = {
                    'date': date,
                    'content': full_text,
                    'publisher': '조선일보'
                }   
                print(f"[{i+1}/{len(article_links)}] Crawled: {url}")
                
            except NoSuchElementException:
                print(f" Article structure not found in {url}")
            except TimeoutException: # 페이지 로딩 속도가 너무 느릴 때 발생
                print(f" Timeout when loading {url}")
            except Exception as e:
                print(f" Failed to crawl content from {url}: {e}")
                continue

        driver.quit()
        df = pd.DataFrame([
            {"url": url, "date": data["date"], "content": data["content"], "publisher": "중앙일보"}
            for url, data in article.items()
        ])

        csv_buffer = StringIO()
        df.to_csv(csv_buffer, index=False)

        s3_hook = S3Hook(aws_conn_id='s3_conn')
        key = f"news_dataframe/chosun_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"
        s3_hook.load_string(csv_buffer.getvalue(), key=key, bucket_name=BUCKET_NAME, replace=True)
        return ''
    
    @task
    def dong_a():
        options = Options()
        options.add_argument("--headless")
        options.add_argument("--disable-gpu")
        options.add_argument("--no-sandbox")

        try:
            service = Service(executable_path=DRIVER_PATH)
            driver = webdriver.Chrome(service=service, options=options)
            wait = WebDriverWait(driver, 5)
        except WebDriverException as e:
            print(f"[FATAL] Failed to initialize WebDriver: {e}")
            return

        article_links = set()
        cutoff_date = 20200901  # 기준 날짜

        # 페이지 순회
        for page in range(1, MAX_PAGE + 1):
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

        article_links = list(article_links)
        print(f"🔗 Total collected article URLs: {len(article_links)}")

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
            {"url": url, "date": data["date"], "content": data["content"], "publisher": "중앙일보"}
            for url, data in article.items()
        ])

        csv_buffer = StringIO()
        df.to_csv(csv_buffer, index=False)

        s3_hook = S3Hook(aws_conn_id='s3_conn')
        key = f"news_dataframe/dong_a_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"
        s3_hook.load_string(csv_buffer.getvalue(), key=key, bucket_name=BUCKET_NAME, replace=True)
        return ''
    
    @task
    def joonang():

        options = Options()
        options.add_argument("--headless")
        options.add_argument("--disable-gpu")
        options.add_argument("--no-sandbox")

        try:
            service = Service(executable_path=DRIVER_PATH)
            driver = webdriver.Chrome(service=service, options=options)
            wait = WebDriverWait(driver, 5)
        except WebDriverException as e:
            print(f"[FATAL] Failed to initialize WebDriver: {e}")
            return None

        driver.get("https://www.joongang.co.kr/realestate?page=1")
        try:
            wait.until(EC.presence_of_element_located((
                By.CSS_SELECTOR,
                '#container > section > div.contents_bottom.float_left > section:nth-child(2) > nav > ul > li.page_next > a'
            )))
        except TimeoutException:
            print(" Initial page load timeout")
            driver.quit()
            return None

        article_links = []

        # 기사 목록 수집
        for page in range(MAX_PAGE):
            try:
                print(f" ({page+1}/{MAX_PAGE}) 페이지 수집 중...")
                time.sleep(2)
                a_tags = driver.find_elements(By.CSS_SELECTOR, '#story_list a')
                for a in a_tags:
                    href = a.get_attribute('href')
                    if href:
                        article_links.append(href)

                next_page_btn = driver.find_element(
                    By.CSS_SELECTOR,
                    '#container > section > div.contents_bottom.float_left > section:nth-child(2) > nav > ul > li.page_next > a'
                )
                next_page_btn.click()
            except (ElementClickInterceptedException, NoSuchElementException) as e:
                print(f" 다음 페이지 없음: {e}")
                break
            except Exception as e:
                print(f" 페이지 {page+1} 처리 중 오류: {e}")
                continue

        article_links = list(set(article_links))
        print(f" 총 {len(article_links)}개의 기사 링크 수집 완료")

        # 기사 내용 수집
        article = {}
        for i, url in enumerate(article_links):
            try:
                driver.get(url)
                time.sleep(2)
                article_section = driver.find_element(By.CSS_SELECTOR, "#article_body")
                paragraphs = article_section.find_elements(By.TAG_NAME, 'p')
                full_text = "\n".join(p.text.strip() for p in paragraphs if p.text.strip())

                time_element = driver.find_element(By.CSS_SELECTOR, 'time[itemprop="datePublished"]')
                published_date = time_element.get_attribute('datetime')

                article[url] = {
                    "content": full_text,
                    "date": published_date,
                    "publisher" : '중앙일보'
                }
                print(f" ({i+1}/{len(article_links)}) Crawled: {url} | {len(full_text)}자 추출")


            except Exception as e:
                print(f" ({i+1}/{len(article_links)}) URL 접근 실패: {url} | 에러: {e}")
                article[url] = {
                    "content": "접근 실패",
                    "date": None
                }

        driver.quit()
        print(" 중앙일보 크롤링 완료")

        df = pd.DataFrame([
            {"url": url, "date": data["date"], "content": data["content"], "publisher": "중앙일보"}
            for url, data in article.items()
        ])

        csv_buffer = StringIO()
        df.to_csv(csv_buffer, index=False)

        s3_hook = S3Hook(aws_conn_id='s3_conn')
        key = f"news_dataframe/joonang_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"
        s3_hook.load_string(csv_buffer.getvalue(), key=key, bucket_name=BUCKET_NAME, replace=True)
        return ''

    @task
    def korea_eco():
    # Chrome headless 설정
        options = Options()
        options.add_argument("--headless")
        options.add_argument("--disable-gpu")
        options.add_argument("--no-sandbox")
        options.add_argument("--disable-dev-shm-usage")

        service = Service(executable_path=DRIVER_PATH)
        driver = webdriver.Chrome(service=service, options=options)
        wait = WebDriverWait(driver, 5)

        article_links = []

        def collect_links_from_category(url, category_name):
            driver.get(url)
            for i in range(MAX_PAGE):
                try:
                    print(f"[{category_name}] {i+1}번째 페이지 링크 수집중...")
                    # 페이지 선택
                    wait.until(EC.presence_of_element_located(
                        (By.CSS_SELECTOR, '#contents > div.select-paging > div.page-select.txt-num > div > select')
                    ))
                    select_element = driver.find_element(
                        By.CSS_SELECTOR, '#contents > div.select-paging > div.page-select.txt-num > div > select'
                    )
                    select = Select(select_element)
                    select.select_by_value(str(i + 1))
                    time.sleep(2)

                    # 기사 링크 추출
                    wait.until(EC.presence_of_all_elements_located((By.CSS_SELECTOR, '#contents > ul a')))
                    a_tags = driver.find_elements(By.CSS_SELECTOR, '#contents > ul a')
                    for a in a_tags:
                        href = a.get_attribute('href')
                        if href:
                            article_links.append(href)

                except (NoSuchElementException, StaleElementReferenceException, TimeoutException) as e:
                    print(f"페이지 {i+1} 수집 중 오류: {e}")
                    break

        # 카테고리별 수집
        categories = {
            '경제정책': 'https://www.hankyung.com/economy/economic-policy?page=1',
            '거시경제': 'https://www.hankyung.com/economy/macro',
            '외환시장': 'https://www.hankyung.com/economy/forex',
            '세금': 'https://www.hankyung.com/economy/tax',
            '고용복지': 'https://www.hankyung.com/economy/job-welfare'
        }

        for cat_name, cat_url in categories.items():
            collect_links_from_category(cat_url, cat_name)

        # 중복 제거
        article_list = list(set(article_links))
        print(f"\n총 {len(article_list)}개의 기사 링크를 수집했습니다.")

        # 본문 수집
        article = {}
        for i, link in enumerate(article_list):
            try:
                driver.get(link)
                print(f"[{i+1}/{len(article_list)}] 기사 크롤링 중: {link}")
                time.sleep(2)

                article_element = wait.until(
                    EC.presence_of_element_located((By.CSS_SELECTOR, '#articletxt'))
                )
                text = article_element.text.strip()
                if not text:
                    print(f"본문 비어 있음: {link}")
                    text = "본문 없음"

                # URL에서 날짜 추출 (ex: /article/2025082017461)
                date_match = re.search(r'/article/(\d{8})', link)
                if date_match:
                    published_date = f"{date_match.group(1)[:4]}-{date_match.group(1)[4:6]}-{date_match.group(1)[6:8]}"
                else:
                    published_date = None

            except (TimeoutException, NoSuchElementException, StaleElementReferenceException) as e:
                print(f"본문 수집 실패: {link} | 에러: {e}")
                text = "본문 수집 실패"
                published_date = None
            except Exception as e:
                print(f"알 수 없는 오류: {link} | 에러: {e}")
                text = "접근 실패"
                published_date = None

            article[link] = {"text": text, "date": published_date, "publisher": "한국경제"}

        driver.quit()
        print(f"\n[INFO] 총 {len(article)}개의 기사 수집 완료.")

        # # 데이터프레임 변환
        df = pd.DataFrame.from_dict(article, orient='index').reset_index()
        df.rename(columns={'index': 'url'}, inplace=True)

        csv_buffer = StringIO()
        df.to_csv(csv_buffer, index=False)

        s3_hook = S3Hook(aws_conn_id='s3_conn')
        key = f"news_dataframe/korea_eco_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"
        s3_hook.load_string(csv_buffer.getvalue(), key=key, bucket_name=BUCKET_NAME, replace=True)
        return ''

    @task
    def save_to_s3(*news_list):
        rows = []
        for news in news_list:
            for url, data in news.items():
                rows.append({
                    "url": url,
                    "date": data.get("date"),
                    "content": data.get("content", ""),
                    "publisher": data.get("publisher", "")
                })

        df = pd.DataFrame(rows)
        csv_buffer = StringIO()
        df.to_csv(csv_buffer, index=False)

        s3_hook = S3Hook(aws_conn_id='s3_conn')
        s3_hook.load_string(
            string_data=csv_buffer.getvalue(),
            key="news_dataframe/news_dataframe.csv",
            bucket_name="ian-geonewsapt",
            replace=True
        )

    start = EmptyOperator(task_id='start')
    end = EmptyOperator(task_id='end')
    
    chosun_task = chosun()
    dong_a_task = dong_a()
    joonang_task = joonang()
    korea_task = korea_eco()

    #merged_task = save_to_s3(chosun_task, dong_a_task,joonang_task, korea_task)

    start >> [chosun_task, dong_a_task, joonang_task, korea_task] >> end #>> merged_task >> end