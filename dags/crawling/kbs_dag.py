from airflow import DAG
from airflow.decorators import task
from airflow.sdk import Variable
from datetime import datetime, timedelta 
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
import time, re
import pandas as pd
from tasks.db import save_to_db

dag_owner = 'Ian Kim'

default_args = {'owner': dag_owner,
        'depends_on_past': False,
        'retries': 2,
        'retry_delay': timedelta(seconds=10)
        }

DRIVER_PATH = Variable.get("DRIVER_PATH")
# 어제 날짜 "YYYYMMDD"
yesterday_str = (datetime.today() - timedelta(days=1)).strftime("%Y%m%d")  


with DAG(dag_id='KBS',
        default_args=default_args,
        description='KSB 크롤링하는 DAG입니다.',
        start_date=datetime(2020,2,2),
        schedule='* 7 * * *',
        catchup=False,
        tags=['crawling']
):
    @task
    def crawling_kbs():
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

        article_links = list()
        cutoff_date = 20200901  # 기준 날짜
        article = {}
        url = f'https://news.kbs.co.kr/news/pc/category/category.do?ctcd=0004&ref=pSiteMap#{yesterday_str}&1'
        driver.get(url)

        next_btn = driver.find_element(By.CSS_SELECTOR, '#contents > div.box.padding-24.field-contents-wrapper.category-main-list > div.common-pagination > button.next-button')

        while True:
            try:
                time.sleep(4)
                # 5초 내에 next_btn이 클릭 가능해지면 클릭
                next_btn = WebDriverWait(driver, 5).until(
                    EC.element_to_be_clickable((By.CSS_SELECTOR, "#contents > div.box.padding-24.field-contents-wrapper.category-main-list > div.common-pagination > button.next-button"))
                )
                next_btn.click()
                container = driver.find_element(By.CSS_SELECTOR, "#contents > div.box.padding-24.field-contents-wrapper.category-main-list > div.box-contents.has-wrap")
                links = container.find_elements(By.TAG_NAME, "a")
                
                for link in links:
                    href = link.get_attribute("href")
                    if href:
                        article_links.append(href)
            except (ElementClickInterceptedException, TimeoutException):
                print("마지막 페이지에 도달했습니다.")
                break

        print(f"article_links is {article_links}")
        article = {}

        for url in article_links:
            driver.get(url)
            
            # 기사 내용
            content = driver.find_element(By.CSS_SELECTOR, '#cont_newstext').text
            
            # 날짜
            date_text = driver.find_element(
                By.CSS_SELECTOR, 
                '#contents > div > div.view-contents-wrapper > div.view-headline.view-box > div.dates > em.input-date'
            ).text
            
            m = re.search(r'(\d{4})\.(\d{2})\.(\d{2})', date_text)
            if m:
                date_str = f"{m.group(1)}-{m.group(2)}-{m.group(3)}"
            else:
                date_str = None  # 날짜 정보 없으면 None 처리
            
            # 내용이 있는 경우만 저장
            if content.strip():
                article[url] = {
                    'date': date_str,
                    'content': content,
                    'publisher': 'KBS'
                }

        driver.quit()
        # # 데이터프레임 변환
        df = pd.DataFrame.from_dict(article, orient='index').reset_index()
        df.rename(columns={'index': 'url'}, inplace=True)
        return df


    crawling_kbs_task = crawling_kbs()
    save_to_db_task = save_to_db(crawling_kbs_task)

    crawling_kbs_task >> save_to_db_task

