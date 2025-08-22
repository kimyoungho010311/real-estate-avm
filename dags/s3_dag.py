from airflow import DAG
from airflow.decorators import task
from airflow.operators.empty import EmptyOperator
from datetime import datetime, timedelta
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
import os

dag_owner = 'Ian Kim'

default_args = {
    'owner': dag_owner,
    'depends_on_past': False,
}

with DAG(
    dag_id='s3_file_upload_dag',
    default_args=default_args,
    start_date=datetime(2025, 8, 21),
    catchup=False,
    schedule=None,
) as dag:

    start = EmptyOperator(task_id='start')
    end = EmptyOperator(task_id='end')

    @task
    def create_local_file():
        file_path = '/tmp/airflow_s3_test.txt'
        with open(file_path, 'w', encoding='utf-8') as f:
            f.write("This is a test file for S3 upload.")
        return file_path

    @task
    def upload_file_to_s3(file_path: str):
        s3_hook = S3Hook(aws_conn_id='s3_conn')
        bucket_name = 'ian-geonewsapt'
        s3_key = os.path.basename(file_path)
        s3_hook.load_file(
            filename=file_path,
            bucket_name=bucket_name,
            key=f'tmp/{s3_key}',
            replace=True
        )
        return f"s3://{bucket_name}/tmp/{s3_key}"

    @task
    def list_s3_files():
        s3_hook = S3Hook(aws_conn_id='s3_conn')
        bucket_name = 'ian-geonewsapt'
        files = s3_hook.list_keys(bucket_name=bucket_name, prefix='tmp/')
        for f in files:
            print(f)
        return files

    local_file = create_local_file()
    s3_path = upload_file_to_s3(local_file)
    files_listed = list_s3_files()

    start >> local_file >> s3_path >> files_listed >> end