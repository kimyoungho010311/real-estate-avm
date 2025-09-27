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
        #'retries': 2,
        #'retry_delay': timedelta(minutes=5)
        }

try:
    API_KEY = Variable.get("KOREA_REAL_ESTATE_API_KEY")
    print("한국 부동산 API키를 가져오는데 성공했습니다.")
except Exception as e:
    print("한국 부동산 API키를 가져오는데 실패했습니다. : {e}")

download_dir = "tmp/land_price_change_by_region"

# 부동산 시장 소비 심리지수 테이블 코드
STATBL_ID = 'T235013129634707'
# 월별 데이터를 수집
DTACYCLE_CD = 'MM'
# 강남구 지역번호
CLS_ID = '50004'

today = date.today()

today_str = today.strftime("%Y%m")

# S3에 저장될때 사용되는 폴더 명입니다. 예시 : dt={today_month}
today_month = today.strftime("%Y-%m")

url = (
    f'https://www.reb.or.kr/r-one/openapi/SttsApiTblData.do'
    f'?KEY={API_KEY}&Type=json&STATBL_ID={STATBL_ID}'
    f'&DTACYCLE_CD={DTACYCLE_CD}&CLS_ID={CLS_ID}'
    f'&START_WRTTIME={today_str}&END_WRTTIME={today_str}'
)

with DAG(dag_id='real_estate_sentiment',
        default_args=default_args,
        description='부동산 시장 소비 심리지수 데이터를 수집하는 DA 입니다.',
        start_date=datetime(2020,2,2),
        schedule='@weekly',
        catchup=False,
        tags=['Economy indicators']
):
    @task
    def fetch_real_estate_sentiment():
        os.makedirs(download_dir, exist_ok=True)
        # 파일명 : YYYY-MM.json
        output_path = os.path.join(download_dir, f"{today_month}.json")

        response = requests.get(url)

        if response.status_code == 200:
            data = response.json()
            # 만약 아래 키워드가 있으면 정상적인 데이터이므로 Json으로 저장 후 S3에 업로드합니다.
            if 'SttsApiTblData' in data:
                print("===== 정상적인 데이터 =====")
                rows = data['SttsApiTblData'][1]['row']
                for i, item in enumerate(rows, start=1):
                    print(f"[{i}] STATBL_ID   : {item.get('STATBL_ID')}")
                    print(f"    DTACYCLE_CD : {item.get('DTACYCLE_CD')}")
                    print(f"    WRTTIME_IDTFR_ID      : {item.get('WRTTIME_IDTFR_ID')}")
                    print(f"    GRP_ID      : {item.get('GRP_ID')}")
                    print(f"    GRP_NM      : {item.get('GRP_NM')}")
                    print(f"    CLS_ID      : {item.get('CLS_ID')}")
                    print(f"    CLS_NM       : {item.get('CLS_NM')}")
                    print(f"    ITM_ID        : {item.get('ITM_ID')}")
                    print(f"    ITM_NM  : {item.get('ITM_NM')}")
                    print(f"    DTA_VAL  : {item.get('DTA_VAL')}")
                    print(f"    UI_NM  : {item.get('UI_NM')}")
                    print(f"    GRP_FULLNM  : {item.get('GRP_FULLNM')}")
                    print(f"    CLS_FULLNM  : {item.get('CLS_FULLNM')}")
                    print(f"    ITM_FULLNM  : {item.get('ITM_FULLNM')}")
                    print(f"    WRTTIME_DESC  : {item.get('WRTTIME_DESC')}")
                    print("-----------------------------")
            else:
                # 해당 키워드가 없으면 아직 집계되지 않거나 에러가 발생한 경우입니다.
                code = data['RESULT'].get('CODE', 'N/A')
                message = data['RESULT'].get("MESSAGE", "N/A")
                print(f"Code    : {code}")
                print(f"Message    : {message}")
                print("=========================")
                print("데이터가 존재하지 않거나 비정상적입니다. DAG를 실패로 처리합니다.")
                raise AirflowFailException
        else:
            # 만약 응답코드가 200이 아니라면 실패로 DAG를 종료합니다.
            print(f"(월) 지역별 아파트 매매 현황 API의 반환값이 {response.status_code}입니다. DAG를 종료합니다.")
            raise AirflowFailException
        with open(output_path, "w", encoding='utf-8') as f:
            json.dump(data, f, ensure_ascii=False, indent=4)

        return str(output_path)
    
    @task
    def save_df_to_s3(output_path: str):
        s3_hook = S3Hook(aws_conn_id = 's3_conn')
        bucket_name = 'real-estate-avm'
        key = f"raw/economic-indicators/real-estate-consumer-sentiment/dt={today_month}/{today_month}.json"
        s3_hook.load_file(
            filename = output_path,
            bucket_name = bucket_name,
            key = key,
            replace = True
        )

        return f"s3://{key}에 저장되었습니다."
    
    fetch_real_estate_sentiment_task = fetch_real_estate_sentiment()
    save_df_to_s3_task = save_df_to_s3(fetch_real_estate_sentiment_task)

    fetch_real_estate_sentiment_task >> save_df_to_s3_task