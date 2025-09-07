from airflow import DAG
from airflow.decorators import task
from airflow.providers.postgres.hooks.postgres import PostgresHook
from datetime import datetime, timedelta 

dag_owner = 'Ian Kim'

default_args = {'owner': dag_owner,
        'depends_on_past': False,
        #'retries': 2,
        #'retry_delay': timedelta(seconds=3)
        }

with DAG(dag_id='db_dag',
        default_args=default_args,
        description='DB 관련된 DAG임',
        start_date=datetime(2018,2,2),
        #schedule='* * * * *',
        catchup=False,
        tags=['.']
):

    @task
    def create_news_table():
        pg_hook = PostgresHook(postgres_conn_id='pg_conn')
                            # Postgres_conn_id 로 하니깐 오류났음
        create_news_table_sql = """
        CREATE TABLE IF NOT EXISTS news (
            id INTEGER PRIMARY KEY GENERATED ALWAYS AS IDENTITY,
            date TIMESTAMP,
            url TEXT,
            content TEXT,
            publisher TEXT
        );
        """
        pg_hook.run(create_news_table_sql)

    @task
    def create_apt_table():
        pg_hook = PostgresHook(postgres_conn_id='pg_conn')

        create_apt_table_sql = """
        CREATE TABLE IF NOT EXISTS apt(
            id INTEGER PRIMARY KEY GENERATED ALWAYS AS IDENTITY,
            area_m2 DOUBLE PRECISION,
            complex_name TEXT,
            floor SMALLINT,
            contract_day TIMESTAMP,
            street_name TEXT,
            built_year SMALLINT,
            price_per_m2 DOUBLE PRECISION,
            apartment_age SMALLINT,
            alpha DOUBLE PRECISION,
            longitude DOUBLE PRECISION,
            latitude DOUBLE PRECISION
        );
        """
        pg_hook.run(create_apt_table_sql)

    @task
    def create_deep_search_table():
        pg_hook = PostgresHook(postgres_conn_id='pg_conn')

        create_deep_search_table_sql = """
        CREATE TABLE IF NOT EXISTS deep_search(
        id INTEGER PRIMARY KEY GENERATED ALWAYS AS IDENTITY,
        title TEXT,
        publisher TEXT,
        published_at TIMESTAMP,
        url TEXT,
        summary TEXT
        )
        """
        pg_hook.run(create_deep_search_table_sql)


    news_table_task = create_news_table()
    apt_table_task = create_apt_table()
    deep_search_task = create_deep_search_table()

    [news_table_task, apt_table_task, deep_search_task]