from airflow import DAG
from airflow.decorators import task
from airflow.sdk import Variable
#from airflow.providers.postgres.hooks.postgres import PostgresHook
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
#from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import (
    NoSuchElementException,
    TimeoutException,
    StaleElementReferenceException,
    WebDriverException,
    #ElementClickInterceptedException,
)
from datetime import datetime, timedelta 
import time
import re
#from io import StringIO
import pandas as pd
from tasks.db import save_to_db

MAX_PAGE = int(Variable.get("MAX_PAGE"))
DRIVER_PATH = Variable.get("DRIVER_PATH")
# 오늘발행된 신문기사와 비교를 하기 위한 변수
yesterday_str = (datetime.today() - timedelta(days=1)).strftime("%Y/%m/%d")

dag_owner = 'Ian_Kim'

default_args = {'owner': dag_owner,
        'depends_on_past': False,
        'retries': 3,
        'retry_delay': timedelta(minutes=5)
        }

with DAG(dag_id='chosun',
        default_args=default_args,
        description='조선일보 크롤링',
        start_date=datetime(2022,2,2),
        schedule='30 7 * * *',
        catchup=False,
        tags=['crawling']
):

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
        #https://www.chosun.com/economy/real_estate/2025/08/22/7XO543AOTM4IRLMPABDCAMDUQA/:
        article_links = list(set([href for href in hrefs if "/economy/real_estate/20" in href]))
        filtered_links = []
        

        for link in article_links:
            # 링크에서 날짜 추출
            match = re.search(r'/(\d{4}/\d{2}/\d{2})/', link)
            if match:
                link_date = match.group(1)
                if link_date == yesterday_str:
                    filtered_links.append(link)

        # filtered_links에는 오늘 날짜 기사만 남음
        article_links = filtered_links

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

        return df
    

    chosun_task = chosun()
    save_to_db_task = save_to_db(chosun_task)

    chosun_task >> save_to_db_task