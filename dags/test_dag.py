from airflow import DAG
from airflow.decorators import task
from airflow.operators.empty import EmptyOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.utils.log.logging_mixin import LoggingMixin
from datetime import datetime, timedelta 
from io import StringIO
import pandas as pd

dag_owner = 'Ian_Kim'

default_args = {'owner': dag_owner,
        'depends_on_past': False,
        }
BUCKET_NAME = "ian-geonewsapt"
log = LoggingMixin().log
PREFIX = "news_dataframe/"  # 폴더 경로

with DAG(dag_id='Test_DAG',
        default_args=default_args,
        start_date=datetime(2010,1,1),
        catchup=False,
        tags=['test'],
        doc_md="""
        ## 문서화를 추가하는 DAG 예제

        코드에서 DAG 인스턴스를 생성할 때 `doc_md` 인자에 문자열을 넘겨주면 됩니다.
        이처럼 마크다운으로 쓰는것이 가능합니다.
    """,
) as dag:

    start = EmptyOperator(task_id='start')

    @task
    def task_1():
        s3_hook = S3Hook(aws_conn_id='s3_conn')
        bucket_name = BUCKET_NAME   
        # 폴더 안 모든 파일 목록 가져오기
        keys = s3_hook.list_keys(bucket_name=bucket_name, prefix=PREFIX)
        if not keys:
            print("폴더에 파일 없음")
            return

        #각 파일 읽기
        dfs = []
        for key in keys:
            data_str = s3_hook.read_key(key=key, bucket_name=bucket_name)
            print(f"== {key} ==")
            print(data_str[:200])  # 처음 200자만 출력 예시
            df = pd.read_csv(StringIO(data_str))
            df = df[['date', 'url', 'content', 'publisher']]  # 순서 통일
            df['date'] = pd.to_datetime(df['date'], errors='coerce')  # timestamp 변환
            dfs.append(df)
        final_df = pd.concat(dfs, ignore_index=True)
        csv_str = final_df.to_csv(index=False)

        return csv_str

    @task
    def task_2():
        pg_hook = PostgresHook(postgres_conn_id='pg_conn')
        insert_sql = """
            INSERT INTO news (date, url, content, publisher)
            VALUES (%s, %s, %s, %s)
        """
        s3_hook = S3Hook(aws_conn_id='s3_conn')
        keys = s3_hook.list_keys(bucket_name=BUCKET_NAME, prefix=PREFIX)
        # keys리스트 맨 앞에 이상한 경로가 포함되어 있어 제거함
        keys = keys[1:]

        #각 파일 읽기
        dfs = []
        for key in keys:
            data_str = s3_hook.read_key(key=key, bucket_name=BUCKET_NAME)
            print(f"== {key} ==")
            print(data_str[:200])  # 처음 200자만 출력 예시
            df = pd.read_csv(StringIO(data_str))
            df = df[['date', 'url', 'content', 'publisher']]  # 순서 통일
            df['date'] = pd.to_datetime(df['date'], errors='coerce')  # timestamp 변환
            dfs.append(df)
        final_df = pd.concat(dfs, ignore_index=True)

        # Insert to db each row
        for _, row in final_df.iterrows():
            pg_hook.run(insert_sql, parameters=(row['date'], row['url'], row['content'], row['publisher']))

        return ''
    
    end = EmptyOperator(task_id='end')

    task1 = task_1()
    task2 = task_2()

    start >> task2 >> end