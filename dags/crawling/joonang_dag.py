from airflow import DAG
from airflow.decorators import task
from airflow.sdk import Variable
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import (
    NoSuchElementException,
    TimeoutException,
    StaleElementReferenceException,
    WebDriverException,
    ElementClickInterceptedException,
)
from datetime import datetime, timedelta
import time
import re
import pandas as pd
from io import StringIO
from tasks.db import save_to_db

MAX_PAGE = int(Variable.get("MAX_PAGE"))
DRIVER_PATH = Variable.get("DRIVER_PATH")

dag_owner = 'Ian_Kim'

default_args = {'owner': dag_owner,
        'depends_on_past': False,
        'retries': 2,
        'retry_delay': timedelta(minutes=5)
        }

with DAG(dag_id='joonang',
        default_args=default_args,
        description='중앙일보 크롤링, 중앙일보는 링크에 날짜가 없어 추가적인 구현 필요',
        start_date=datetime(2020,2,2),
        schedule='* 8 * * *',
        catchup=False,
        tags=['crawling']
):
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
        #https://www.joongang.co.kr/article/25360704
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
                # article[url] = {
                #     "content": "접근 실패",
                #     "date": None
                # }

        driver.quit()
        print(" 중앙일보 크롤링 완료")

        df = pd.DataFrame([
            {"url": url, "date": data["date"], "content": data["content"], "publisher": "중앙일보"}
            for url, data in article.items()
        ])

        return df
    
    
    joonang_task = joonang()
    save_to_db_task = save_to_db(joonang_task)

    joonang_task >> save_to_db_task