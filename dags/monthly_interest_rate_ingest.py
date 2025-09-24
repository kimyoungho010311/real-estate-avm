from airflow import DAG
from airflow.decorators import task
from airflow.models import Variable
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.exceptions import AirflowFailException
import requests
from datetime import datetime, timedelta, date
import os, json

dag_owner = 'Ian Kim'

default_args = {'owner': dag_owner,
        'depends_on_past': False,
        'retries': 2,
        'retry_delay': timedelta(minutes=5)
        }

try:
    API_KEY = Variable.get("ECOS_KEY")
    print(f"ECOS API 키를 가져오는데 성공했습니다.")
except Exception as e:
    print(f"ECOS API키를 가져오는데 실패했습니다. : {e}")

download_dir = "/tmp/monthly_interest_rate"

STAT_CODE = "722Y001"
ITEM_CODE = "0101000"
FREQ = "M"
today = date.today()
today_str = today.strftime("%Y%m")

# S3에 저장될때 사용되는 폴더 명입니다. 예시 : dt={today_month}
today_month = today.strftime("%Y-%m")
url = f"https://ecos.bok.or.kr/api/StatisticSearch/{API_KEY}/json/kr/1/10000/{STAT_CODE}/{FREQ}/{today_str}/{today_str}/{ITEM_CODE}"
#test_url = f"https://ecos.bok.or.kr/api/StatisticSearch/{API_KEY}/json/kr/1/10000/{STAT_CODE}/{FREQ}/{202409}/{202409}/{ITEM_CODE}"


with DAG(dag_id='monthly_interest_rate_ingest',
        default_args=default_args,
        description='매달 ECOS에서 금리 데이터를 수집하는 DAG 입니다.',
        start_date=datetime(2020,2,2),
        schedule='@weekly', # 매주 월요일 00:00에 실행
        catchup=False,
        tags=['Economy indicators']
):
    @task
    def fetch_interest_rate_data():
        """
        ECOSE에서 월별 금리 데이터를 수집하고 로컬에 JSON 파일로 저장합니다.

        Returns:
            str: 로컬에 저장된 JSON 파일 경로

        Raises:
            AirflowFailException: API 응답이 200이 아니거나 데이터가 비정상인 경우 DAG 실채 처리
        """
        os.makedirs(download_dir, exist_ok = True)
        # 파일명 : 202509_rate.json
        output_path = os.path.join(download_dir, f"{today_str}_rate.json")    

        response = requests.get(url)

        if response.status_code == 200:
            data = response.json()
            if 'StatisticSearch' in data:
                # StatisticSearch가 존재하면 정상적인 데이터이므로 그대로 저장 진행
                example_data = data['StatisticSearch']['row']
                print("===== 정상적인 데이터 =====")
                for i, item in enumerate(example_data, start=1):
                    print(f"[i] STAT_CODE  : {item.get('STAT_CODE')}")
                    print(f"    STAT_NAME  : {item.get('STAT_NAME')}")
                    print(f"    ITEM_CODE1 : {item.get('ITEM_CODE1')}")
                    print(f"    ITEM_NAME1 : {item.get('ITEM_NAME1')}")
                    print(f"    UNIT_NAME  : {item.get('UNIT_NAME')}")
                    print(f"    TIME       : {item.get('TIME')}")
                    print(f"    DATA_VALUE : {item.get('DATA_VALUE')}")
                    print("-----------------------------")
            else:
                # StatisticSearch가 없으면 비정상적인 데이터이므로 출력하여 데이터 확인 및 에러 발생
                code = data['RESULT'].get('CODE', 'N/A')
                message = data['RESULT'].get('MESSAGE', 'N/A')
                print("===== ECOS API 에러 발생 =====")
                print(f"Code   : {code}")
                print(f"Message: {message}")
                print("==========================")
                print(f"데이터가 존재하지 않거나 비정상적입니다. DAG를 실패로 처리합니다.")
                raise AirflowFailException
        else:
            # 만약 응답코드가 200이 아니라면 그대로 실패로 DAG를 종료
            print(f"ECOSE API의 반환값이 {response.status_code}입니다. DAG를 종료합니다.")
            raise AirflowFailException
        
        # JSON 파일로 저장
        with open(output_path, "w", encoding="utf-8") as f:
            json.dump(data, f, ensure_ascii=False, indent=4)

        # JSON 파일이 저장된 경로를 다음 Task에 전달
        return str(output_path)
    
    @task
    def save_df_to_s3(output_path: str):
        """
        로컬 JSON 파일을 S3 버킷으로 업로드 합니다.

        Args:
            output_path (str): 로컬에 저장된 JSON 파일 경로
        
        Returns:
            str: 업로드된 S3 파일의 전체 경로
        """
        s3_hook = S3Hook(aws_conn_id = 's3_conn')
        bucket_name = 'real-estate-avm'
        #s3_key = os.path.basename(output_path)
        key = f"raw/economic-indicators/monthly-interest-rate/dt={today_month}/{today_str}_rate.json"
        s3_hook.load_file(
            filename = output_path,
            bucket_name = bucket_name,
            key = key,
            replace = True
        )

        return f"s3://{key}에 저장되었습니다."
    
    fetch_interest_rate_data_task = fetch_interest_rate_data()
    save_df_to_s3_task = save_df_to_s3(fetch_interest_rate_data_task)

    fetch_interest_rate_data_task >> save_df_to_s3_task