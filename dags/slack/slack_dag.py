from airflow import DAG
from airflow.decorators import task
from airflow.models import Variable
from airflow.providers.postgres.hooks.postgres import PostgresHook
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

try:
    SLACK_WEBHOOK = Variable.get("SLACK_WEBHOOK_URL")
    print(f"Slack Webhook URL을 가져오는데 성공했습니다. {SLACK_WEBHOOK[0:15]}...")
except Exception as e:
    print(f"Slack Webhook URL을 가져오는데 실패했습니다. {e}")



with DAG(dag_id='META_DAG',
        default_args=default_args,
        description='슬랙과 연관된 DAG입니다.',
        start_date=datetime(2020,2,2),
        schedule='* 10 * * *',
        catchup=False,
        tags=['slack']
):
    @task
    def get_dag_count():
        pg_hook = PostgresHook(postgres_conn_id='pg_conn')
        get_dag_count_sql = """
            select count(*) as DAG_count
            from dag
            where 
                is_paused = false
                and dag_id != 'META_DAG'
                and dag_id != 'Test_DAG'
                and dag_id != 'slack'
        """

        dag_count = pg_hook.get_first(get_dag_count_sql)
        dag_count = dag_count[0] # 튜플로 반환되기에 맨 처음요소로 인덱싱합니다.
        print(f"현재 실행중인 DAG의 총 갯수는 {dag_count}개 입니다.")
        return dag_count

    @task
    def show_dags_state(dag_count):
        pg_hook = PostgresHook(postgres_conn_id='pg_conn')
        client = WebClient(token=SLACK_API)
        get_dag_sate_sql = f"""
            SELECT DISTINCT ON (dag_id)
                dag_id,
                state
            FROM dag_run
            WHERE 
                dag_id != 'META_DAG'
                and dag_id != 'slack'
                and dag_id != 'Test_DAG'
                and dag_id != 'db_dag'
                and dag_id != 's3_file_upload_dag'
            ORDER BY dag_id asc, end_date DESC;
        """

        records = pg_hook.get_records(get_dag_sate_sql)
        summary_lines = [
            f"{dag_id}: ✅" if state.lower() == "success" else f"{dag_id}: ❌"
            for dag_id, state in records
        ]
        text = "*오늘 DAG 실행 요약*\n" + "\n".join(summary_lines)
        client.chat_postMessage(channel="pipeline-status", text=text)
        return ''


    get_dag_count_task = get_dag_count()
    show_dags_state_task = show_dags_state(get_dag_count_task)