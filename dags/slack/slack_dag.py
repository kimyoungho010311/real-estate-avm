from airflow import DAG
from airflow.decorators import task
from airflow.models import Variable, DagRun
from airflow.utils.state import DagRunState
from airflow.utils.session import provide_session
from slack_sdk import WebClient
from datetime import datetime, timedelta 

dag_owner = 'Ian_Kim'

default_args = {'owner': dag_owner,
        # 'depends_on_past': False,
        # 'retries': 2,
        # 'retry_delay': timedelta(minutes=5)
        }

try:
    SLACK_API = Variable.get("SLACK_API")
    print(f"슬랙키를 가져오는데 성공했습니다.{SLACK_API[0:10]}...")    
except Exception as e:
    print(f"슬랙키를 가져오는데 실패했습니다. : {e}")

@provide_session
def dag_success_bool(dag_id: str, execution_date, session=None) -> bool:
    dagrun = (
        session.query(DagRun)
        .filter(
            DagRun.dag_id == dag_id,
            DagRun.execution_date == execution_date
        )
        .first()
    )
    return dagrun is not None and dagrun.state == DagRunState.SUCCESS


with DAG(dag_id='slack',
        default_args=default_args,
        description='슬랙과 연관된 DAG입니다.',
        start_date=datetime(2020,2,2),
        schedule='* 8 * * *',
        catchup=False,
        tags=['']
):
    @task
    def msg():
        client = WebClient(token=SLACK_API)

        client.chat_postMessage(channel="pipeline-status", text='hello slack!')
        return ''
    
    @task
    def check_success():
        dag_success_bool('chosun', )