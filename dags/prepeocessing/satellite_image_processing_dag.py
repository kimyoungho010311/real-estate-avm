from airflow import DAG
from airflow.decorators import task
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.models import Variable
from airflow.exceptions import AirflowFailException
from airflow.operators.trigger_dagrun import TriggerDagRunOperator

from datetime import datetime, timedelta, date
import pandas as pd
import os, requests, random
from PIL import Image
import io
import torchvision.transforms as transforms
import torchvision.transforms.functional as TF
from torchvision.transforms import ToPILImage
import shutil

dag_owner = 'Ian Kim'

default_args = {'owner': dag_owner,
        'depends_on_past': False,
        #'retries': 2,
        #'retry_delay': timedelta(minutes=5)
        }

yesterday = date.today() - timedelta(days=1)
YESTERDAY_YMD = yesterday.strftime("%Y-%m-%d")

# 아래 YESTERDAY_YMD은 개발용 날짜임
#YESTERDAY_YMD = '2025-09-24'

local_download_path = 'tmp/satellite_image'
preprocessed_image_path = 'tmp/preprocessed_satellite_image'
os.makedirs(local_download_path, exist_ok=True)
os.makedirs(preprocessed_image_path, exist_ok=True)

with DAG(dag_id='satellite_image_processing_dag',
        default_args=default_args,
        description='위성 이미지 전처리를 하는 DAG입니다.',
        start_date=datetime(2020,2,2),
        schedule='0 8 * * 1-5',
        catchup=False,
        tags=['Preprocessing']
):

    @task
    def fetch_satellite_image_from_s3():
        print(f"{YESTERDAY_YMD}날짜의 위성 이미지를 전처리하기위해 다운로드합니다.")

        s3_hook = S3Hook(aws_conn_id='s3_conn')
        bucket_name = 'real-estate-avm'
        prefix = f'raw/satellite-imagery/dt={YESTERDAY_YMD}/'

        keys = s3_hook.list_keys(bucket_name=bucket_name, prefix=prefix)
        
        file_keys = [k for k in keys if not k.endswith('/')]
        #raw/satellite-imagery/dt=2025-09-24/apt_image_0.jpg

        download_image_path_list = []

        for key in file_keys:
            download_image_path = s3_hook.download_file(
                key = key,
                bucket_name=bucket_name,
                local_path=local_download_path
            )
        
        # 다운로드 후 확장자 없는 파일 .jpg로 변경 및 깨진 이미지 제거
        for f in os.listdir(local_download_path):
            file_path = os.path.join(local_download_path, f)
            if not os.path.splitext(f)[1]:
                new_file_path = file_path + '.jpg'
                os.rename(file_path, new_file_path)
                file_path = new_file_path
            try:
                with Image.open(file_path) as img:
                    img.verify()
            except Exception as e:
                print(f"이미지 열기 실패: {file_path}, {e}")
                os.remove(file_path)
        
        return local_download_path
    
    @task
    def preprocessing_satellite_images(local_download_path):

        print(f"download_image_path : {local_download_path}")
        base_transform = transforms.Compose([
            transforms.Resize((224,224)),
            transforms.ColorJitter(brightness=0.2, contrast=0.2),
            transforms.ToTensor(),
            transforms.Normalize(
                mean=[0.0, 0.0, 0.0],
                std=[1.0, 1.0, 1.0]
            )
        ])
        print(f"local download path 의 모든 파일들 :{os.listdir(local_download_path)}")
        to_pil = ToPILImage()
        filenames = [f for f in os.listdir(local_download_path) if f.lower().endswith(('.png', '.jpg', '.jpeg'))]
        print(f"filenames : {filenames}")

        saved_files = []
        for filename in filenames:
            img_path = os.path.join(local_download_path,filename)
            img = Image.open(img_path).convert("RGB")

            degree = random.uniform(-90, 90)
            rotated_img = TF.rotate(img, degree, fill=(255, 255, 255))

            augmented_img = base_transform(rotated_img)
            augmented_img_pil = to_pil(augmented_img)

            save_path = os.path.join(preprocessed_image_path, filename)
            os.makedirs(os.path.dirname(save_path), exist_ok=True)
            augmented_img_pil.save(save_path)

            saved_files.append(save_path)

        print(f"saved_files : {saved_files}")

        return saved_files
    
    @task
    def save_dt_to_s3(saved_files: str):
        idx = 0
        print(f"S3에 저장되는 파일의 리스트 : {saved_files}")
        for saved_file in saved_files:
            idx += 1
            s3_hook = S3Hook(aws_conn_id = 's3_conn')
            bucket_name = 'real-estate-avm'
            key = f'processed/normalized-images/dt={YESTERDAY_YMD}/{YESTERDAY_YMD}_{idx}'
            s3_hook.load_file(
                filename = saved_file,
                bucket_name = bucket_name,
                key = key,
                replace = True
            )
        print(f"s3://{key}에 모든 전처리된 이미지가 저장되었습니다.")
        return 0

    @task
    def cleanup_local_directory(local_download_path: str, preprocessed_image_path: str):
        """임시로 사용한 이미지 디렉토리와 CSV 디렉토리를 삭제합니다."""
        
        # 다운로드된 CSV 파일 디렉토리 삭제
        try:
            if os.path.exists(local_download_path):
                shutil.rmtree(local_download_path)
                print(f"임시 CSV 디렉토리 '{local_download_path}' 삭제 완료.")
            else:
                print(f"임시 CSV 디렉토리 '{local_download_path}'가 존재하지 않습니다.")
        except Exception as e:
            print(f"임시 CSV 디렉토리 삭제 실패: {e}")

        # 다운로드된 이미지 파일 디렉토리 삭제
        try:
            if os.path.exists(preprocessed_image_path):
                # shutil.rmtree를 사용하여 디렉토리와 모든 내용을 재귀적으로 삭제
                shutil.rmtree(preprocessed_image_path)
                print(f"임시 이미지 디렉토리 '{preprocessed_image_path}' 삭제 완료.")
            else:
                print(f"임시 이미지 디렉토리 '{preprocessed_image_path}'가 존재하지 않습니다.")
        except Exception as e:
            print(f"임시 이미지 디렉토리 삭제 실패: {e}")
    
    trigger_vectorization_image = TriggerDagRunOperator(
        task_id = 'trigger_vectorization_image',
        trigger_dag_id = 'satellite_image_vectorization',
        wait_for_completion=False
    )




    fetch_satellite_image_from_s3_task = fetch_satellite_image_from_s3()
    preprocessing_satellite_images_task = preprocessing_satellite_images(fetch_satellite_image_from_s3_task)
    save_dt_to_s3_task = save_dt_to_s3(preprocessing_satellite_images_task)
    cleanup_local_directory_task = cleanup_local_directory(local_download_path, preprocessed_image_path)

    fetch_satellite_image_from_s3_task >> preprocessing_satellite_images_task >> save_dt_to_s3_task >> cleanup_local_directory_task >> trigger_vectorization_image