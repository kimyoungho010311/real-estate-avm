from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.decorators import task
import pandas as pd

@task
def save_to_db(df):
    """
    크롤링된 뉴스 기사를 DB에 넣는 함수입니다.
    """
    pg_hook = PostgresHook(postgres_conn_id='pg_conn')
    insert_sql = """
        INSERT INTO news (date, url, content, publisher)
        VALUES (%s, %s, %s, %s)
    """

    df = df[['date', 'url', 'content', 'publisher']]  # 순서 통일
    df['date'] = pd.to_datetime(df['date'], errors='coerce')  # timestamp 변환

    for _, row in df.iterrows():
        pg_hook.run(insert_sql, parameters=(row['date'], row['url'], row['content'], row['publisher']))

    return ''   