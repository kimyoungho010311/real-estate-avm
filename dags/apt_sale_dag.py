from airflow import DAG
from airflow.decorators import task
from airflow.operators.empty import EmptyOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.models import Variable
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
)
import csv, time, os, requests, glob
import pandas as pd
import numpy as np
from pathlib import Path
from io import StringIO
from tqdm import tqdm
import psycopg2
from tasks.apt_processing import remove_price_outliers, calculate_alpha_from_age_count,representative_price,calculate_alpha_row

dag_owner = 'Ian_Kim'

default_args = {'owner': dag_owner,
        'depends_on_past': False,
        }

# DAG를 실행하면서 사용되는 대표 변수들
DRIVER_PATH = "/usr/bin/chromedriver"
download_dir = "/tmp/apt_sale_downloads"
os.makedirs(download_dir, exist_ok=True)
os.chmod(download_dir, 0o777)  # 권한 충분히 열기

host_dir = "/opt/airflow/data/apt_sale"   # Docker volume로 매핑
yesterday = datetime.today() - timedelta(days=1)
yesterday_str = yesterday.strftime("%Y-%m-%d")
sido = '11000' # 서울 특별시

# CSV 다운을 누르면 연결되는 url
url = 'https://rt.molit.go.kr/pt/xls/ptXlsCSVDown.do;jsessionid=OvMuHixqWmS3QGgERLARxrR-Mwu7T7Fl8UcTJH5f.RT_DN10'
columns_to_use = [
    '시군구', '단지명', '전용면적(㎡)', '계약년월', '계약일',
    '거래금액(만원)', '동', '층', '건축년도', '도로명'
]
#folder_path = "/Users/kim-youngho/git/GeoNewsApt/data/raw/apt_sale/"
OUTPUT_PATH = Path('../data/interim/apt/apt_with_long_lat.csv')  
KAKAO_API_KEY = Variable.get("KAKAO_KEY")


with DAG(dag_id='apt_sale',
        default_args=default_args,
        #description='',
        start_date=datetime(2019,1,1),
        schedule='* 8 * * *',
        catchup=False,
        tags=['.'],
        doc_md="""
        ### apt_data
        국토교통부에서 정해진 기간의 아파트 거래 내역을 크롤링합니다.
        """
):

    start = EmptyOperator(task_id='start')

    @task
    def apt_data():
        download_dir = Path("/tmp/apt_sale_downloads")
        download_dir.mkdir(parents=True, exist_ok=True)
        output_file = download_dir / "apt_sale.csv"

        url_main = "https://rt.molit.go.kr/pt/xls/xls.do"
        url_down = "https://rt.molit.go.kr/pt/xls/ptXlsCSVDown.do"

        payload = {
            "srhThingNo": "A",
            "srhDelngSecd": "1",
            "srhAddrGbn": "1",
            "srhLfstsSecd": "1",
            "sidoNm": "서울특별시",
            "sggNm": "전체",
            "emdNm": "전체",
            "loadNm": "전체",
            "areaNm": "전체",
            "hsmpNm": "전체",
            "mobileAt": "",
            "srhFromDt": yesterday_str,
            "srhToDt": yesterday_str,
            "srhNewRonSecd": "",
            "srhSidoCd": "11000",
            "srhSggCd": "",
            "srhEmdCd": "",
            "srhRoadNm": "",
            "srhLoadCd": "",
            "srhHsmpCd": "",
            "srhArea": "",
            "srhFromAmount": "",
            "srhToAmount": ""
        }

        headers = {
            "Content-Type": "application/x-www-form-urlencoded",
            "User-Agent": "Mozilla/5.0"
        }

        with requests.Session() as session:
            # 1. 검색 페이지 먼저 GET → 세션 쿠키 확보
            session.get(url_main, headers=headers)

            # 2. 다운로드 POST
            response = session.post(url_down, data=payload, headers=headers)
            response.raise_for_status()
            output_file.write_bytes(response.content)

        return str(output_file)
       
    @task
    def preprocess(csv_path: str):
        # 해당 경로의 모든 .csv 파일 리스트 얻기
        #csv_files = glob.glob(os.path.join(download_dir, "*.csv"))
        df = pd.read_csv(csv_path, encoding='cp949', skiprows=15, usecols=columns_to_use)
        print("Columns:", df.columns.tolist())
        # 면적당 단가 계산
        df['거래금액(만원)'] = df['거래금액(만원)'].str.replace(',', '').astype(int)
        df['면적당 단가(만원)'] = df['거래금액(만원)'] / df['전용면적(㎡)']

        # 아파트 나이 계산
        df['계약년도'] = df['계약년월'].astype(str).str[:4].astype(int)
        df['아파트 나이'] = df['계약년도'] - df['건축년도']

        #거래 순으로 나열 및 필요 없는 컬럼 삭제
        # df['구'] = df['시군구'].str.extract(r'(\S+구)')
        # 계약연-월-일을 기준으로 시계열 정렬
        df['계약일자'] = df['계약년월'].astype(str) + df['계약일'].astype(str).str.zfill(2)
        df['계약일자'] = pd.to_datetime(df['계약일자'], format='%Y%m%d')

        df = df.sort_values('계약일자').reset_index(drop=True)
        df.drop(['시군구','계약년월','계약일','동','계약년도'], axis=1, inplace=True)


        # 1. 이상치 제거
        df_filtered = df.groupby(['도로명', '단지명', '전용면적(㎡)'], group_keys=False)\
                        .apply(remove_price_outliers)\
                        .reset_index(drop=True)

        # 2. 가중치 α 계산 및 대표 row 추출
        df = df.groupby(['도로명', '단지명', '전용면적(㎡)'], group_keys=False)\
                        .apply(calculate_alpha_row)\
                        .reset_index(drop=True)

        df.drop('거래금액(만원)', axis=1, inplace=True)
        df = df.sort_values('계약일자').reset_index(drop=True)
        return df
    
    @task
    def get_log_lat(df):

        # === 좌표 변환 === #
        headers = {'Authorization': f'KakaoAK {KAKAO_API_KEY}'}
        
        def get_coords(address):
            res = requests.get(
                "https://dapi.kakao.com/v2/local/search/address.json",
                headers=headers,
                params={'query': address}
            )
            if res.status_code == 200 and res.json()['documents']:
                doc = res.json()['documents'][0]
                return doc['x'], doc['y']
            return None, None

        longitudes, latitudes = [], []
        for address in tqdm(df['도로명'], desc="좌표 변환 중"):
            x, y = get_coords(address)
            longitudes.append(x)
            latitudes.append(y)

        df['경도'] = longitudes
        df['위도'] = latitudes

        # === 최종 정제 및 저장 === #

        df['면적당 단가(만원)'] = np.log(df['면적당 단가(만원)'])

        # === 수집 못한 위도 경도는 삭제 === #
        #df.dropna(inplace=True)
        output_path = Path("/tmp/apt_sale_downloads/apt_with_long_lat.csv")
        df.to_csv(output_path, index=False)
        return str(output_path)
    
    @task
    def insert_to_db(csv_path):
        import chardet

        with open("/tmp/apt_sale_downloads/apt_with_long_lat.csv", "rb") as f:
            rawdata = f.read()
            result = chardet.detect(rawdata)
            print(result)

        df = pd.read_csv(csv_path, encoding='utf-8')
        print(df.head)
        pg_hook = PostgresHook(postgres_conn_id='pg_conn')
        col_name = ['complex_name','area_m2','floor','built_year',
           'street_name','price_per_m2','apartment_age','contract_day',
            'alpha', 'longitude','latitude']

        df.columns=col_name
        print(df.head)

        insert_sql = """
            INSERT INTO apt (
                complex_name, area_m2, floor, built_year,
                street_name, price_per_m2, apartment_age,
                contract_day, alpha, longitude, latitude
            )
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s) 
            """
        try:
            for _, row in df.iterrows():
                pg_hook.run(insert_sql, parameters=(
                    row['complex_name'],
                    row['area_m2'],
                    row['floor'],
                    row['built_year'],
                    row['street_name'],
                    row['price_per_m2'],
                    row['apartment_age'],
                    row['contract_day'],
                    row['alpha'],
                    row['longitude'],
                    row['latitude']
                ))
        except Exception as e:
            print(f"{e}")

    end = EmptyOperator(task_id='end')
    

    apt_data_task = apt_data()
    preprocess_task = preprocess(apt_data_task)
    get_log_lat_task = get_log_lat(preprocess_task)
    insert_to_db_task = insert_to_db(get_log_lat_task)

    start >> apt_data_task >> preprocess_task >> get_log_lat_task >> insert_to_db_task >> end