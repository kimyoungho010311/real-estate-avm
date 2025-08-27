import ee  # Google Earth Engine 파이썬 API
import requests  # URL 요청
from PIL import Image  # 이미지 열기/저장
from io import BytesIO  # 이미지 바이트 스트림 처리
import logging  # 로그 기록
from datetime import datetime, timedelta
from logging.handlers import RotatingFileHandler  # 파일 로그 핸들러
from airflow.providers.amazon.aws.hooks.s3 import S3Hook  # S3 업로드

LOG_PATH = "logs/image_download/image_download.log"
BUCKET_NAME = 'ian-geonewsapt'

# === 로거 설정 ===
LOG_FORMAT = "[%(asctime)s] [%(levelname)s] %(message)s"
logger = logging.getLogger()
logger.setLevel(logging.INFO)

formatter = logging.Formatter(LOG_FORMAT)
file_handler = RotatingFileHandler(LOG_PATH, maxBytes=5*1024*1024, backupCount=5)
file_handler.setFormatter(formatter)
logger.addHandler(file_handler)

s3_hook = S3Hook(aws_conn_id='s3_conn')

def process_transaction(idx, tx):
    try:
        lat = tx['lat']
        lon = tx['lon']
        tx_date = tx['date']
        start_date = tx_date - timedelta(days=183)
        end_date = tx_date + timedelta(days=183)

        center = ee.Geometry.Point([lon, lat])
        roi = center.buffer(1500).bounds()

        collection = (
            ee.ImageCollection('COPERNICUS/S2_SR_HARMONIZED')
            .filterBounds(center)
            .filterDate(start_date, end_date)
            .filter(ee.Filter.lt('CLOUDY_PIXEL_PERCENTAGE', 5))
        )

        if collection.size().getInfo() == 0:
            logger.warning(f"이미지 없음: lat={lat}, lon={lon}, idx={idx}, date={tx_date}")
            return f"[X] 이미지 없음 (index {idx})"

        image = collection.sort('system:time_start', False).first()
        stats = image.reduceRegion(
            reducer=ee.Reducer.percentile([2, 98]),
            geometry=roi,
            scale=10,
            maxPixels=1e8
        ).getInfo()

        if not stats:
            logger.warning(f"통계 없음: lat={lat}, lon={lon}, idx={idx}, date={tx_date}")
            return f"[X] 통계 없음 (index {idx})"

        url = image.getThumbURL({
            'region': roi,
            'format': 'jpg',
            'bands': ['B4', 'B3', 'B2'],
            'min': [stats.get('B4_p2', 500), stats.get('B3_p2', 500), stats.get('B2_p2', 500)],
            'max': [stats.get('B4_p98', 3500), stats.get('B3_p98', 3500), stats.get('B2_p98', 3500)],
            'scale': 10
        })

        if not url.startswith("https://"):
            logger.warning(f"URL 생성 실패: lat={lat}, lon={lon}, idx={idx}, date={tx_date}")
            return f"[X] URL 생성 실패 (index {idx})"

        response = requests.get(url)
        if response.status_code != 200:
            logger.warning(f"이미지 요청 실패: lat={lat}, lon={lon}, idx={idx}, date={tx_date}, status={response.status_code}")
            return f"[X] 이미지 요청 실패 (index {idx})"

        img = Image.open(BytesIO(response.content))
        img_buffer = BytesIO()
        img.save(img_buffer, format='JPEG')
        img_buffer.seek(0)

        s3_key = f'gee/raw/apt_image_{lat}_{lon}_{tx_date}.jpg'
        s3_hook.load_file_obj(file_obj=img_buffer, key=s3_key, bucket_name=BUCKET_NAME, replace=True)

    except Exception as e:
        logger.warning(f"예외 발생: lat={lat}, lon={lon}, idx={idx}, date={tx_date}, error={e}")
        return f"[X] 예외 발생 (index {idx}): {e}"