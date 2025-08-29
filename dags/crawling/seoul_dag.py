from airflow import DAG
from airflow.decorators import task
from airflow.models import Variable
from datetime import datetime, timedelta 
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.common.exceptions import (
    NoSuchElementException,
    TimeoutException,
    StaleElementReferenceException,
    WebDriverException,
    ElementClickInterceptedException,
)
from datetime import datetime, timedelta 
import time
import pandas as pd
from tasks.db import save_to_db

dag_owner = 'Ian_Kim'

default_args = {'owner': dag_owner,
        'depends_on_past': False,
        # 'retries': 2,
        # 'retry_delay': timedelta(minutes=5)
        }

Variable.set("SEOUL_ECO", "2")
MAX_PAGE = int(Variable.get("SEOUL_ECO"))
DRIVER_PATH = Variable.get("DRIVER_PATH")
yesterday_str = (datetime.today() - timedelta(days=1)).strftime("%Y.%m.%d")
hrefs = []

with DAG(dag_id='seoul',
        default_args=default_args,
        description='서울경제 크롤링',
        start_date=datetime(2020,2,2),
        schedule='5 8 * * *',
        catchup=False,
        tags=['crawling']
):
    @task
    def seoul_eco():
        options = Options()
        options.add_argument("--headless")
        options.add_argument("--disable-gpu")
        options.add_argument("--no-sandbox")

        try:
            service = Service(executable_path=DRIVER_PATH)
            driver = webdriver.Chrome(service=service, options=options)
            print("[INFO] WebDriver 초기화 완료")
        except WebDriverException as e:
            print(f"[FATAL] WebDriver 초기화 실패: {e}")
            raise

        news_data = []

        # 페이지 순회
        for i in range(1, MAX_PAGE + 1):
            page_url = f'https://www.sedaily.com/NewsMain/GB/{i}'
            try:
                driver.get(page_url)
                print(f"[INFO] 페이지 로드: {page_url}")
                time.sleep(1)
            except Exception as e:
                print(f"[ERROR] 페이지 로드 실패 {page_url}: {e}")
                continue

            # 뉴스 목록 수집
            li_elements = driver.find_elements(
                By.CSS_SELECTOR,
                '#container > div > div.sub_left > div:nth-child(1) > ul > li'
            )

            for li in li_elements:
                try:
                    a_tag = li.find_element(By.CSS_SELECTOR, 'div.text_area > div.article_tit > a')
                    href = a_tag.get_attribute('href')
                except NoSuchElementException:
                    href = None

                try:
                    date_span = li.find_element(By.CSS_SELECTOR, 'div.text_area > div.text_info > span.date')
                    date = date_span.text
                except NoSuchElementException:
                    date = '방금전'

                if date == yesterday_str and href:
                    date = date.replace('.','-')
                    news_data.append({'url': href, 'date': date})
                    print(f"[+] 수집됨: {href} ({date})")

        # 본문 수집
        for idx, news in enumerate(news_data):
            try:
                driver.get(news['url'])
                time.sleep(1)
                article_body = driver.find_element(By.CSS_SELECTOR, 'div.article_view[itemprop="articleBody"]')
                full_text = article_body.text.strip()
                news['content'] = full_text
                news['publisher'] = '서울경제'
                print(f"[{idx + 1}/{len(news_data)}] 본문 수집 완료: {news['url']}")
            except NoSuchElementException:
                print(f"[WARN] 본문 요소 없음: {news['url']}")
                news['content'] = ''
            except TimeoutException:
                print(f"[WARN] 타임아웃: {news['url']}")
                news['content'] = ''
            except Exception as e:
                print(f"[ERROR] 본문 수집 실패 {news['url']}: {e}")
                news['content'] = ''

        driver.quit()

        df = pd.DataFrame(news_data)
        return df
    
    seoul_eco_task = seoul_eco()
    save_to_db_task = save_to_db(seoul_eco_task)

    seoul_eco_task >> save_to_db_task