from airflow import DAG
from airflow.decorators import task
from airflow.models import Variable
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.operators.trigger_dagrun import TriggerDagRunOperator

from datetime import datetime, timedelta, date
import os, requests
import pandas as pd
import numpy as np
from pathlib import Path
from tqdm import tqdm
import chardet
dag_owner = 'Ian Kim'

default_args = {'owner': dag_owner,
        'depends_on_past': False,
        'retries': 2,
        'retry_delay': timedelta(minutes=1)
        }

# DAG를 실행하면서 사용되는 대표 변수들
DRIVER_PATH = "/usr/bin/chromedriver"
download_dir = "/tmp/apt_sale_downloads"
os.makedirs(download_dir, exist_ok=True)
os.chmod(download_dir, 0o777)  # 권한 충분히 열기

host_dir = "/opt/airflow/data/apt_sale"   # Docker volume로 매핑
yesterday = datetime.today() - timedelta(days=1)
yesterday_str = yesterday.strftime("%Y-%m-%d")
today = date.today()

sido = '11000' # 서울 특별시
gang_nam_gu = '11680' # 강남구

# CSV 다운을 누르면 연결되는 url
url = 'https://rt.molit.go.kr/pt/xls/ptXlsCSVDown.do;jsessionid=OvMuHixqWmS3QGgERLARxrR-Mwu7T7Fl8UcTJH5f.RT_DN10'

with DAG(dag_id='real_estate_ingestion_dag',
        default_args=default_args,
        description='아파트 매매 데이터를 수집하는 DAG입니다.',
        start_date=datetime(2025,8,11),
        schedule='0 8 * * 1-5',
        catchup=False,
        tags=['APT']
):
    @task
    def apt_data():
        """국토교통부 아파트 실거래가 데이터를 다운로드하여 CSV로 저장합니다.

        이 함수는 국토교통부 사이트에 POST 요청을 보내어
        서울 강남구 어제 날짜 기준 아파트 실거래 데이터를 조회합니다.
        다운로드된 CSV 파일은 다음 TASK에 전달하기위해 임시 디렉토리에 저장됩니다.

        Returns:
            str: 다운로드된 CSV 파일의 경로
        
        
        """
        download_dir = Path("/tmp/apt_sale_downloads")
        download_dir.mkdir(parents=True, exist_ok=True)
        output_path = download_dir / "apt_sale.csv"

        url_main = "https://rt.molit.go.kr/pt/xls/xls.do"
        url_down = "https://rt.molit.go.kr/pt/xls/ptXlsCSVDown.do"

        payload = {
            "srhThingNo": "A",
            "srhDelngSecd": "1",
            "srhAddrGbn": "1",
            "srhLfstsSecd": "1",
            "sidoNm": "서울특별시",
            "sggNm": "강남구",
            "emdNm": "전체",
            "loadNm": "전체",
            "areaNm": "전체",
            "hsmpNm": "전체",
            "mobileAt": "",
            "srhFromDt": yesterday_str,
            "srhToDt": yesterday_str,
            "srhNewRonSecd": "",
            "srhSidoCd": "11000",
            "srhSggCd": "11680",
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
            output_path.write_bytes(response.content)

        return str(output_path)

    @task
    def save_df_to_s3(output_path: str):
        """apt_data TASK에서 다운로드한 CSV를 S3에 업로드합니다.

        이 함수는 apt_data 에서 전달받은 임시로 저장된 디렉토리의 경로를 받아
        해당 CSV파일을 S3에 업로드합니다.

        Arguments:
            output_path (str): 임시 디렉토리에 저장된 CSV 파일의 전체 경로
        
        Returns:
            str: 업로드된 파일의 S3 URI
        
        Exceptions:
            ...
        """
        s3_hook = S3Hook(aws_conn_id = 's3_conn')
        bucket_name = 'real-estate-avm'
        s3_key = os.path.basename(output_path)
        s3_hook.load_file(
            filename=output_path,
            bucket_name=bucket_name,
            key=f"raw/real-estate/df={today}/data.csv",
            replace=True
        )

        return f"s3://{bucket_name}/tmp/{s3_key}에 저장되었습니다."
    
    trigger_apt_processing_dag = TriggerDagRunOperator(
        task_id = 'trigger_apt_processing_dag',
        trigger_dag_id='apt_processing_dag',
        wait_for_completion=False
    )

    apt_data_task = apt_data()
    save_df_to_s3_task = save_df_to_s3(apt_data_task)

    apt_data_task >> save_df_to_s3_task >> trigger_apt_processing_dag