from airflow import DAG
from airflow.decorators import task
from airflow.models import Variable
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.sensors.external_task import ExternalTaskSensor
from datetime import datetime, timedelta 
import ee  # Google Earth Engine 파이썬 API
import requests  # 웹 요청 (썸네일 이미지 다운로드)
import pandas as pd
import numpy as np
from PIL import UnidentifiedImageError, Image  # 이미지 파일 열고 저장
import io
from io import BytesIO  # 이미지 바이트 데이터를 PIL로 읽기 위한 버퍼
from datetime import datetime, timedelta  # 날짜 처리용
from concurrent.futures import ThreadPoolExecutor, as_completed
import os 
import logging
from logging.handlers import RotatingFileHandler
from tasks.gee import process_transaction

import torchvision.transforms as transforms
import torchvision.transforms.functional as TF
from torchvision.transforms import ToPILImage

LOG_PATH = "logs/image_download/image_download.log"
BUCKET_NAME = 'ian-geonewsapt'
yesterday_str = (datetime.today() - timedelta(days=1)).strftime("%Y-%m-%d")

# Google Earth Engine 인증을 위한 변수들
project = Variable.get("GOOGLE_PROJECT")
SERVICE_ACCOUNT = "gee-service-account@aptprice-464102.iam.gserviceaccount.com"
KEY_PATH = "/opt/airflow/keys/gee-service-account.json"

# 원본 위성 지도 이미지가 있는 디렉토리
PREFIX = 'gee/raw/'
# 전처리가 완료된 위성사진이 저장되는 디렉토리
PREFIX_OUTPUT = 'gee/preprocess/'

# 위성사진을 다양한 각도로 돌려보기
degrees = [0, 90, 180, 270]

base_transform = transforms.Compose([
    transforms.Resize((224, 224)),
    transforms.ColorJitter(brightness=0.2, contrast=0.2),
    transforms.ToTensor(),
    transforms.Normalize(
            mean=[0.485, 0.456, 0.406],
            std=[0.229, 0.224, 0.225]
    )
])


# === 경로 보장 ===
os.makedirs(os.path.dirname(LOG_PATH), exist_ok=True)

# === 로거 설정 ===
LOG_FORMAT = "[%(asctime)s] [%(levelname)s] %(message)s"
logger = logging.getLogger()
logger.setLevel(logging.INFO)

formatter = logging.Formatter(LOG_FORMAT)
file_handler = RotatingFileHandler(LOG_PATH, maxBytes=5*1024*1024, backupCount=5)
file_handler.setFormatter(formatter)
logger.addHandler(file_handler)

# === 병렬처리 설정 ===
# 원래는 30이었음 일단은 1로 설정
MAX_WORKERS = 1

dag_owner = 'Ian_Kim'

default_args = {'owner': dag_owner,
        # 'depends_on_past': False,
        # 'retries': 2,
        # 'retry_delay': timedelta(minutes=5)
        }

with DAG(dag_id='GEE',
        default_args=default_args,
        description='위성지도를 받아오고 전처리 하는 DAG입니다.',
        start_date=datetime(2020,2,2),
        schedule='* 9 * * *',
        catchup=False,
        tags=['satellite']
):
    @task
    def read_apt_data():
        pg_hook = PostgresHook(postgres_conn_id = 'pg_conn')
        read_sql = f"""
                SElECT * FROM apt
                WHERE contract_day = '{yesterday_str}';
        """
        df = pg_hook.get_pandas_df(read_sql)
        df.dropna(inplace=True)
        print(df.head())
        return df

    @task
    def gee(df):
        credentials = ee.ServiceAccountCredentials(SERVICE_ACCOUNT, KEY_PATH)
        ee.Initialize(credentials)
        s3_hook = S3Hook(aws_conn_id='s3_conn')
        print("Google Earth Engine 초기화 완료")

            # === 거래 데이터 리스트화 ===
        apt_transactions = df[['latitude', 'longitude', 'contract_day']].apply(
                lambda row: {
                'lat': row['latitude'],
                'lon': row['longitude'],
                'date': row['contract_day']
                }, axis=1
        ).tolist()
        print("아파트 거래 데이터 정의 완료")
        # === 병렬 처리 실행 ===
        with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
                futures = [executor.submit(process_transaction, idx, tx) for idx, tx in enumerate(apt_transactions)]
                for future in futures:
                        result = future.result()
                        if result:
                                print(result)

    @task
    def preprocess():
        buf = io.BytesIO()
        to_pil = ToPILImage()
        s3_hook = S3Hook(aws_conn_id='s3_conn')
        image_keys = s3_hook.list_keys(bucket_name=BUCKET_NAME, prefix=PREFIX)
        
        for key in image_keys:
            # S3에서 파일 읽기
            obj = s3_hook.get_key(key, bucket_name=BUCKET_NAME)
            body = obj.get()['Body'].read()
            if not key.lower().endswith((".jpg", ".jpeg", ".png")):
                continue
            try:
                img = Image.open(io.BytesIO(body)).convert("RGB")
            except UnidentifiedImageError:
                print(f"Skipping non-image file: {key}")
                continue
            # 이미지 열기
            # .convert("RGB") : 이미지의 색상 모드를 바꾸는 함수

            for degree in degrees:
                # PIL Image -> 전처리 완료
                rotated_img = TF.rotate(img, degree, fill=(255, 255, 255))
                augmented_img = base_transform(rotated_img)
                augmented_img_pil = to_pil(augmented_img)

                # degree별 폴더 포함 S3 key
                filename = key.split("/")[-1]
                s3_key = f"{PREFIX_OUTPUT}{degree}/{filename}"

                # S3 업로드
                buf = io.BytesIO()
                augmented_img_pil.save(buf, format="JPEG")
                buf.seek(0)
                s3_hook.load_file_obj(
                    buf,
                    bucket_name=BUCKET_NAME,
                    key=s3_key,
                    replace=True
                )
                            

    read_apt_data_task = read_apt_data()
    gee_task = gee(read_apt_data_task)
    preprocess_task = preprocess()

    read_apt_data_task >> gee_task >> preprocess_task
