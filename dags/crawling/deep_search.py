from airflow import DAG
from airflow.decorators import task
from airflow.models import Variable
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.exceptions import AirflowSkipException
from datetime import datetime, timedelta 
import pandas as pd
import requests

dag_owner = 'Ian Kim'

default_args = {'owner': dag_owner,
        'depends_on_past': False,
        # 'retries': 2,
        # 'retry_delay': timedelta(minutes=5)
        }
try:
    DEEP_SEARCH_API = Variable.get("DEEP_SEARCH_API")
    print(f"DEEP_SEARCH_API 키를 가져오는데 성공했습니다.\n{DEEP_SEARCH_API}")
except Exception as e:
    print("DEEP_SEARCH_API키를 가져오는데 실패했습니다. : ", {e})

yesterday_str = (datetime.today() - timedelta(days=1)).strftime("%Y-%m-%d")
#DATE_FROM, DATE_TO를 모두 어제 날자로 설정하여 어제 발행된 기사만 수집해옵니다.
#DATE_FROM = yesterday_str
#DATE_TO = yesterday_str
print(f"{yesterday_str}일자의 뉴스 데이터를 받아옵니다.")

DATE_FROM = yesterday_str
DATE_TO = yesterday_str
url = f"https://api-v2.deepsearch.com/v1/articles/economy?page=100&page_size=100&&date_from={DATE_FROM}&date_to={DATE_TO}&api_key={DEEP_SEARCH_API}"

# JSON으로 입력받은 데이터를 저장하는 변수 입니다.
articles = []

with DAG(dag_id='deep_search',
        default_args=default_args,
        description='deep search에서 제공되는 기사들을 DB에 저장하는 DAG입니다.',
        start_date=datetime(2020,2,2),
        schedule='* 6 * * *',
        catchup=False,
        tags=['crawling']
):

    @task
    def get_articles():
        try:
            response = requests.get(url)
            response.raise_for_status()
            data = response.json()
            # API 응답에서 data['data'] 안 리스트를 그대로 사용
            articles = []
            for item in data.get('data', []):
                articles.append({
                    "title": item.get('title'),
                    "publisher": item.get('publisher'),
                    "published_at": item.get('published_at'),
                    "url": item.get('content_url'),
                    "summary": item.get('summary')
                })
            print(f"데이터 예시 : {articles}")
        except requests.exceptions.RequestException as e:
            print(f"요청 중 오류 발생 : {e}")
            articles = []

        df = pd.DataFrame(articles)
        # published_at을 datetime으로 변환
        if not df.empty and 'published_at' in df.columns:
            df['published_at'] = pd.to_datetime(df['published_at']).dt.strftime('%Y-%m-%d')

        if len(df) <= 1:
            print("발행된 뉴스 기사가 없습니다.")
            raise AirflowSkipException("No articles to process")
        else :
            print(f"총 {len(df)}개의 기사가 수집되었습니다. DB에 저장합니다.")
        return df.to_dict(orient='records')  # XCom 직렬화 가능하게 변환


    @task
    def save_to_deep_search(articles):
        pg_hook = PostgresHook(postgres_conn_id='pg_conn')
        insert_sql = """
            INSERT INTO deep_search (title,publisher,published_at,url,summary)
            VALUES (%s, %s, %s, %s, %s)
        """
        for row in articles:
            pg_hook.run(insert_sql, parameters=(
                row['title'], row['publisher'],
                row['published_at'], row['url'], row['summary']
            ))

    get_articles_task = get_articles()
    save_to_deep_search_task = save_to_deep_search(get_articles_task)

    get_articles_task >> save_to_deep_search_task