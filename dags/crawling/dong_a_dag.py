from airflow import DAG
from airflow.decorators import task
from airflow.operators.empty import EmptyOperator
from datetime import datetime, timedelta 

dag_owner = 'Ian_Kim'

default_args = {'owner': dag_owner,
        'depends_on_past': False,
        'retries': 2,
        'retry_delay': timedelta(minutes=5)
        }

with DAG(dag_id='dong_a',
        default_args=default_args,
        description='동아일보 크롤링',
        start_date=datetime(2020,2,2),
        schedule='* 8 * * *',
        catchup=False,
        tags=['crawling']
):
