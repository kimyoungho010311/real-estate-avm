from airflow import DAG
from airflow.decorators import task
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.models import Variable
from airflow.exceptions import AirflowFailException
from datetime import datetime, timedelta, date
import pandas as pd
import os, requests

dag_owner = 'Ian Kim'

default_args = {'owner': dag_owner,
        'depends_on_past': False,
        'retries': 2,
        'retry_delay': timedelta(minutes=5)
        }

try:
    print("KAKAO API KEY를 가져옵니다.")
    KAKAO_API_KEY = Variable.get("KAKAO_KEY")
except Exception as e:
    print("KAKAO API KEY를 가져오는데 실패했습니다. : {e}")


today = date.today()
today_month = today.strftime("%Y-%m-%d")

columns_to_use = [
    '시군구', '단지명', '전용면적(㎡)', '계약년월', '계약일',
    '거래금액(만원)', '동', '층', '건축년도', '도로명'
]

with DAG(dag_id='apt_processing_dag',
        default_args=default_args,
        description='아파트 거래 데이터를 전처리 하는 DAG 입니다.',
        start_date=datetime(2020, 2 ,2),
        schedule='* 8 * * 1-5',
        catchup=False,
        tags=['Preprocessing']
):
    @task
    def fetch_apt_data_from_s3():
        """S3 Raw Zone에서 원본 아파트 거래 데이터를 다운로드합니다.

        Airflow의 실행 날짜(ds)를 기준으로 S3 경로를 동적으로 구성하여
        해당 날짜의 원본 CSV 파일을 Airflow Worker의 임시 디렉토리에 저장합니다.

        Args:
            ds (str, optional): Airflow가 자동으로 제공하는 실행 날짜 (YYYY-MM-DD).
                                Defaults to None.

        Returns:
            str: 다운로드된 로컬 파일의 전체 경로.
        """
        print(today_month)
        local_download_path = '/tmp/apt_sale_downloads'
        os.makedirs(local_download_path, exist_ok=True)

        s3_hook = S3Hook(aws_conn_id='s3_conn')
        bucket_name = 'real-estate-avm'
        key = f"raw/real-estate/df={today_month}/data.csv"

        downloaded_file = s3_hook.download_file(
            key=key,
            bucket_name=bucket_name,
            local_path=local_download_path
        )

        print(f"저장된 디렉토리는 : {downloaded_file} 입니다.")
        return downloaded_file
    
    @task
    def read_csv_and_preprocessing(downloaded_file):
        """다운로드된 원본 CSV를 전처리하고 새로운 로컬 경로에 저장합니다.

        전처리 과정에는 파생 변수 계산, 날짜 형식 변환, 좌표 변환 등이 포함됩니다.
        모든 과정이 완료된 DataFrame은 CSV 포맷으로 임시 저장됩니다.

        Args:
            downloaded_file (str): 이전 Task에서 전달받은 원본 CSV 파일 경로.
            ds (str, optional): Airflow 실행 날짜. 파일명 생성에 사용됩니다.

        Returns:
            str: 전처리 후 새로 저장된 로컬 파일(CSV)의 경로.

        Raises:
            AirflowFailException: 전처리 후 유효한 데이터가 없을 경우 DAG를 실패 처리합니다.
        """
    # --- 1단계: CSV 파일 읽기 ---
        try:
            print("1단계: CSV 파일 읽기를 시작합니다...")
            df = pd.read_csv(
                downloaded_file,
                encoding='cp949',
                skiprows=15,
                usecols=columns_to_use 
            )
            print("CSV 파일 읽기 완료.")
        except FileNotFoundError:
            print(f"에러: CSV 파일을 찾을 수 없습니다. 경로: {downloaded_file}")
            return None
        except Exception as e:
            print(f"에러: CSV 파일을 읽는 중 문제가 발생했습니다: {e}")
            return None

        # --- 2단계: 파생 변수 계산 (면적당 단가, 아파트 나이) ---
        try:
            print("2단계: 파생 변수 계산을 시작합니다...")
            df['거래금액(만원)'] = df['거래금액(만원)'].str.replace(',', '').astype(int)
            df['면적당 단가(만원)'] = df['거래금액(만원)'] / df['전용면적(㎡)']
            
            df['계약년도'] = df['계약년월'].astype(str).str[:4].astype(int)
            df['아파트 나이'] = df['계약년도'] - df['건축년도']
            print("파생 변수 계산 완료.")
        except Exception as e:
            print(f"에러: '면적당 단가' 또는 '아파트 나이' 계산 중 문제가 발생했습니다: {e}")
            return None

        # --- 3단계: 날짜 변환 및 정렬 ---
        try:
            print("3단계: 날짜 변환 및 데이터 정렬을 시작합니다...")
            df['계약일자'] = df['계약년월'].astype(str) + df['계약일'].astype(str).str.zfill(2)
            df['계약일자'] = pd.to_datetime(df['계약일자'], format='%Y%m%d')
            df = df.sort_values('계약일자').reset_index(drop=True)
            print("날짜 변환 및 정렬 완료.")
        except Exception as e:
            print(f"에러: '계약일자' 변환 또는 정렬 중 문제가 발생했습니다: {e}")
            return None
            
        # --- 4단계: 불필요한 컬럼 삭제 ---
        try:
            print("4단계: 불필요한 컬럼을 삭제합니다...")
            df.drop(['시군구','계약년월','계약일','동','계약년도','거래금액(만원)'], axis=1, inplace=True)
            print("컬럼 삭제 완료.")
        except KeyError as e:
            print(f"에러: 존재하지 않는 컬럼을 삭제하려고 시도했습니다: {e}")
            return None


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
        for address in df['도로명']:
            x, y = get_coords(address)
            longitudes.append(x)
            latitudes.append(y)

        df['경도'] = longitudes
        df['위도'] = latitudes

        df.dropna(inplace=True)
        
        print("==========================================================")
        print("============최종적으로 전처리 완료된 데이터의 형태입니다.============")
        print(df.head())
        print("==========================================================")
        print("==========================================================")

        # 전처리 완료된 데이터를 CSV로 저장


        # 1. 저장할 디렉토리와 전체 파일 경로를 정의
        output_dir = '/tmp/apt_sale_processed'
        os.makedirs(output_dir, exist_ok=True)
        output_file_path = os.path.join(output_dir, f"{today_month}_processed.csv") # 동적 파일명

        print(f"전처리된 데이터를 다음 경로에 저장합니다: {output_file_path}")
        df.to_csv(output_file_path, index=False, encoding='utf-8-sig')

        print(f"파일 저장이 완료되었습니다.")
        
        
        return output_file_path

    @task
    def save_df_to_s3(local_download_path: str):
        """전처리된 로컬 파일을 S3 Processed Zone에 업로드합니다.

        Args:
            local_processed_path (str): 이전 Task에서 전달받은 전처리 완료 파일 경로.
            ds (str, optional): Airflow 실행 날짜. S3 경로 생성에 사용됩니다.
        """
        s3_hook = S3Hook(aws_conn_id = 's3_conn')
        bucket_name = 'real-estate-avm'
        key = f"processed/preprocessed_apt_txn/dt={today_month}/{today_month}.csv"
        s3_hook.load_file(
            filename = local_download_path,
            bucket_name = bucket_name,
            key = key,
            replace = True
        )

        return f"s3://{key}에 저장되었습니다."

    # Task 실행
    downloaded_file_path = fetch_apt_data_from_s3()
    read_csv_task = read_csv_and_preprocessing(downloaded_file_path)
    save_df_to_s3_task = save_df_to_s3(read_csv_task)

    downloaded_file_path >> read_csv_task >> save_df_to_s3_task