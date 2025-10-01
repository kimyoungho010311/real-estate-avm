from airflow import DAG
from airflow.decorators import task
from airflow.exceptions import AirflowFailException
from airflow.models import Variable
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from datetime import datetime, timedelta, date
import pandas as pd
import ee  # Google Earth Engine 파이썬 API
import requests  # 웹 요청 (썸네일 이미지 다운로드)
from PIL import Image  # 이미지 파일 열고 저장
from io import BytesIO  # 이미지 바이트 데이터를 PIL로 읽기 위한 버퍼
from concurrent.futures import ThreadPoolExecutor, as_completed
import io, os
import shutil

dag_owner = 'Ian Kim'

default_args = {'owner': dag_owner,
        'depends_on_past': False,
        #'retries': 2,
        #'retry_delay': timedelta(minutes=5)
        }

today = date.today()
today_str = today.strftime("%Y-%m-%d")
today_str = '2025-09-24'
# 전처리된 데이터를 임시로 저장하는 디렉토리 입니다.
download_csv_path = "/tmp/fetch_processed_apt_txn_data"
download_img_dir = "/tmp/satellite_img"
try:
    print("다운로드에 사용되는 임시 디렉토리를 생성합니다.")
    os.makedirs(download_img_dir, exist_ok=True)
    os.makedirs(download_csv_path, exist_ok=True)
except Exception as e:
    print("임시 디렉토리를 생성하는데 실패했습니다. {e}")

with DAG(dag_id='fetch_satellite_image',
        default_args=default_args,
        description='전처리된 아파트 매매 데이터를 기준으로 위성지도를 가져오는 DAG입니다.',
        start_date=datetime(2023,2,2),
        #schedule_interval='',
        catchup=False,
        tags=['Satellite']
        
):
    @task
    def fetch_processed_apt_txn_data(prev_ds=None):
        print(f"검색되는 날짜는 {today_str}입니다.")
        s3_hook = S3Hook(aws_conn_id='s3_conn')
        bucket_name = 'real-estate-avm'
        object_key = f'processed/preprocessed_apt_txn/dt={today_str}/{today_str}.csv'
        try:
            obj_str = s3_hook.read_key(key=object_key, bucket_name=bucket_name)
            df = pd.read_csv(io.StringIO(obj_str))
            print("S3에서 전처리된 아파트 거래 데이터를 성공적으로 받았습니다.")
        except Exception as e:
            print(f"S3에서 전처리 된 아파트 데이터를 가져오는데 실패했습니다. {e}")
        
        try:
            df.to_csv(download_csv_path+'data.csv', index=False, encoding='utf-8-sig')
            print(f"파일이 {download_csv_path}에 성공적으로 저장되었습니다.")
        except Exception as e:
            print(f"전처리된 아파트 거래 데이터를 임시 디렉토리에 저장하는데 실패했습니다. {e}")
        
        return download_csv_path

    @task
    def fetch_satellite_img(download_csv_path):
        df = pd.read_csv(download_csv_path+'data.csv')
        print(df.head)

            # === Earth Engine 초기화 (수정) ===
        try:
            # Airflow Variable에서 필요한 정보들을 가져옵니다.
            gcp_project_id = Variable.get("GOOGLE_PROJECT")
            gcp_service_account_email = Variable.get("GCP_SERVICE_ACCOUNT_EMAIL")
            
            # Worker 컨테이너 내부에 저장된 서비스 계정 키 파일의 경로입니다.
            key_file_path = '/opt/airflow/keys/gee-service-account.json'
            
            # 1. 인증 객체를 생성합니다. (이메일, 키파일 경로 순서)
            credentials = ee.ServiceAccountCredentials(gcp_service_account_email, key_file_path)
            
            # 2. 생성된 인증 객체를 사용하여 초기화합니다.
            ee.Initialize(credentials, project=gcp_project_id)
            print("Google Earth Engine 초기화 완료")
        except Exception as e:
            raise AirflowFailException(f"Google Earth Engine 초기화 실패: {e}")

        print("Google Earth Engine 초기화 완료")

        # --- 1. 데이터프레임 순회 ---
        # DataFrame의 각 행을 순회하며 위성 이미지를 가져옵니다.
        for idx, row in df.iterrows():
            
            # --- 2. 이미지 검색을 위한 정보 설정 ---
            lat = row['위도']
            lon = row['경도']
            tx_date = datetime.strptime(row['계약일자'], "%Y-%m-%d")
            
            # 검색할 날짜 범위 설정 (계약일 기준 앞뒤 6개월)
            start_date = (tx_date - timedelta(days=183)).strftime('%Y-%m-%d')
            end_date = (tx_date + timedelta(days=183)).strftime('%Y-%m-%d')

            # 검색할 지역(ROI, Region of Interest) 설정 (중심점에서 1500m 반경)
            center = ee.Geometry.Point([lon, lat])
            roi = center.buffer(1500).bounds()

            print(f"\n[{idx}] 처리 중: {row['계약일자']} / lat={lat:.4f}, lon={lon:.4f}")

            # --- 3. Earth Engine에서 이미지 컬렉션 검색 ---
            # 구름이 5% 미만인 Sentinel-2 위성 이미지 검색
            collection = (
                ee.ImageCollection('COPERNICUS/S2_SR_HARMONIZED')
                .filterBounds(center)
                .filterDate(start_date, end_date)
                .filter(ee.Filter.lt('CLOUDY_PIXEL_PERCENTAGE', 5))
            )

            # 검색된 이미지가 없으면 건너뛰기
            if collection.size().getInfo() == 0:
                print(f" -> [실패] 해당 조건의 위성 이미지를 찾을 수 없습니다.")
                continue

            # 가장 최신 이미지를 선택
            image = collection.sort('system:time_start', False).first()

            # --- 4. 이미지 시각화 파라미터 설정 ---
            # 이미지의 밝기 등을 조절하기 위해 픽셀 값의 최소/최대 범위를 동적으로 계산
            stats = image.reduceRegion(
                reducer=ee.Reducer.percentile([2, 98]),
                geometry=roi, scale=10, maxPixels=1e8
            ).getInfo()

            if not stats:
                print(f" -> [실패] 이미지 통계 정보를 계산할 수 없습니다.")
                continue
                
            vis_params = {
                'region': roi,
                'format': 'jpg',
                'bands': ['B4', 'B3', 'B2'], # RGB 밴드
                'min': [stats.get('B4_p2', 500), stats.get('B3_p2', 500), stats.get('B2_p2', 500)],
                'max': [stats.get('B4_p98', 3500), stats.get('B3_p98', 3500), stats.get('B2_p98', 3500)],
                'scale': 10
            }

            # --- 5. 이미지 다운로드 및 저장 ---
            try:
                # 이미지 다운로드 URL 생성
                url = image.getThumbURL(vis_params)
                
                # URL로부터 이미지 데이터 요청
                response = requests.get(url)
                response.raise_for_status() # 요청 실패 시 에러 발생
                
                # 이미지 데이터를 PIL Image 객체로 변환
                img = Image.open(BytesIO(response.content))
                
                # 파일로 저장
                img.save(download_img_dir + f"/apt_image_{idx}.jpg")
                print(f" -> [성공] apt_image_{idx}.jpg 로 저장 완료.")

            except Exception as e:
                print(f" -> [실패] 이미지 다운로드 또는 저장 중 예외 발생: {e}")

        return download_img_dir
        
    @task
    def upload_image_dir_to_s3(download_img_dir: str):
        """로컬에 저장된 이미지 파일들을 하나씩 S3 Raw Zone에 업로드합니다. (개별 파일 업로드로 수정됨)"""
        
        s3_hook = S3Hook(aws_conn_id='s3_conn')
        bucket_name = 'real-estate-avm'
        s3_prefix = f'raw/satellite-imagery/dt={today_str}/'
        
        # 디렉토리 내의 모든 파일 목록을 가져옵니다.
        files_to_upload = [f for f in os.listdir(download_img_dir) if os.path.isfile(os.path.join(download_img_dir, f))]
        
        if not files_to_upload:
            print("업로드할 이미지가 없습니다. Task를 스킵합니다.")
            return

        uploaded_count = 0
        print(f"총 {len(files_to_upload)}개의 이미지를 s3://{bucket_name}/{s3_prefix} 로 개별 업로드합니다.")

        for file_name in files_to_upload:
            local_path = os.path.join(download_img_dir, file_name)
            # S3 키는 접두사(Prefix)와 파일 이름을 결합하여 생성합니다.
            s3_key = s3_prefix + file_name
            
            try:
                # 개별 파일 업로드 (load_file 사용)
                s3_hook.load_file(
                    filename=local_path,
                    key=s3_key,
                    bucket_name=bucket_name,
                    replace=True,  # 동일한 경로에 파일이 있으면 덮어씁니다.
                )
                print(f" -> [성공] '{file_name}' 업로드 완료.")
                uploaded_count += 1
            except Exception as e:
                print(f" -> [실패] '{file_name}' 업로드 중 예외 발생: {e}")
                
        if uploaded_count == 0 and len(files_to_upload) > 0:
             # 파일은 있었지만 모두 업로드에 실패했을 경우 태스크 실패 처리
             raise AirflowFailException(f"모든 이미지 ({len(files_to_upload)}개) 업로드에 실패했습니다.")

        print(f"총 {uploaded_count}개의 이미지 업로드 완료.")
        return f"s3://{bucket_name}/{s3_prefix}"

    @task
    def cleanup_local_directory(download_csv_path: str, download_img_dir: str):
        """임시로 사용한 이미지 디렉토리와 CSV 디렉토리를 삭제합니다."""
        
        # 다운로드된 CSV 파일 디렉토리 삭제
        try:
            if os.path.exists(download_csv_path):
                shutil.rmtree(download_csv_path)
                print(f"임시 CSV 디렉토리 '{download_csv_path}' 삭제 완료.")
            else:
                print(f"임시 CSV 디렉토리 '{download_csv_path}'가 존재하지 않습니다.")
        except Exception as e:
            print(f"임시 CSV 디렉토리 삭제 실패: {e}")

        # 다운로드된 이미지 파일 디렉토리 삭제
        try:
            if os.path.exists(download_img_dir):
                # shutil.rmtree를 사용하여 디렉토리와 모든 내용을 재귀적으로 삭제
                shutil.rmtree(download_img_dir)
                print(f"임시 이미지 디렉토리 '{download_img_dir}' 삭제 완료.")
            else:
                print(f"임시 이미지 디렉토리 '{download_img_dir}'가 존재하지 않습니다.")
        except Exception as e:
            print(f"임시 이미지 디렉토리 삭제 실패: {e}")




    fetch_processed_apt_txn_data_task = fetch_processed_apt_txn_data()
    fetch_satellite_img_task = fetch_satellite_img(fetch_processed_apt_txn_data_task)
    upload_image_dir_to_s3_task = upload_image_dir_to_s3(fetch_satellite_img_task)

    cleanup_task = cleanup_local_directory(download_csv_path, download_img_dir) # 새로 추가된 태스크

    # --- 태스크 의존성 설정 ---
    fetch_processed_apt_txn_data_task >> fetch_satellite_img_task >> upload_image_dir_to_s3_task >> cleanup_task
