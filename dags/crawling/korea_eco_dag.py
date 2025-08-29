from airflow import DAG
from airflow.decorators import task
from airflow.sdk import Variable
from datetime import datetime, timedelta 
from selenium.webdriver.support.ui import Select, WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import (
    NoSuchElementException,
    TimeoutException,
    StaleElementReferenceException
)
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from selenium import webdriver
#from airflow.providers.amazon.aws.hooks.s3 import S3Hook
import re
import time
import pandas as pd
#from io import StringIO
from tasks.db import save_to_db

MAX_PAGE = int(Variable.get("MAX_PAGE"))
DRIVER_PATH = Variable.get("DRIVER_PATH")

yesterday_str = (datetime.today() - timedelta(days=1)).strftime("%Y%m%d")  # 어제 날짜 "YYYYMMDD"

dag_owner = 'Ian_Kim'

default_args = {'owner': dag_owner,
        #'depends_on_past': False,
        #'retries': 2,
        #'retry_delay': timedelta(minutes=5)
        }

with DAG(dag_id='korea_eco',
        default_args=default_args,
        description='한국경제 클롤링',
        start_date=datetime(2020,2,2),
        schedule='10 8 * * *',
        catchup=False,
        tags=['crawling']
):

    @task
    def korea_eco():
    # Chrome headless 설정
        options = Options()
        options.add_argument("--headless")
        options.add_argument("--disable-gpu")
        options.add_argument("--no-sandbox")
        options.add_argument("--disable-dev-shm-usage")
        options.add_argument("--remote-debugging-port=9222")

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
                    print(f"Total collected links is {len(article_links)}")
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

        filtered_links = []
        #https://www.hankyung.com/article/2025060189501
        for link in article_links:
            match = re.search(r'/article/(\d{8})', link)  # /article/뒤 8자리 숫자 추출
            if match and match.group(1) == yesterday_str:
                filtered_links.append(link)

        article_list = filtered_links
        print(f"총 {len(article_list)}개의 오늘 기사 링크 수집 완료")

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
                    text = None

                # URL에서 날짜 추출 (ex: /article/2025082017461)
                date_match = re.search(r'/article/(\d{8})', link)
                if date_match:
                    published_date = f"{date_match.group(1)[:4]}-{date_match.group(1)[4:6]}-{date_match.group(1)[6:8]}"
                else:
                    published_date = None

            except (TimeoutException, NoSuchElementException, StaleElementReferenceException) as e:
                print(f"본문 수집 실패: {link} | 에러: {e}")
                text = None
                published_date = None
            except Exception as e:
                print(f"알 수 없는 오류: {link} | 에러: {e}")
                text = None
                published_date = None

            article[link] = {"content": text, "date": published_date, "publisher": "한국경제"}

        driver.quit()
        print(f"\n[INFO] 총 {len(article)}개의 기사 수집 완료.")

        # # 데이터프레임 변환
        df = pd.DataFrame.from_dict(article, orient='index').reset_index()
        df.rename(columns={'index': 'url'}, inplace=True)
        return df

    korea_eco_task = korea_eco()
    save_to_db_task = save_to_db(korea_eco_task)

    korea_eco_task >> save_to_db_task