from airflow import DAG
from airflow.decorators import task
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from datetime import datetime, timedelta, date

from sklearn.preprocessing import MinMaxScaler
from sklearn.decomposition import PCA

import tensorflow as tf
from tensorflow.keras.layers import Input, Dense
from tensorflow.keras.models import Model
from tensorflow.keras.preprocessing.image import load_img, img_to_array
from tensorflow.keras.applications import ResNet50
from tensorflow.keras.applications.resnet50 import preprocess_input

import os
import numpy as np
import pandas as pd
import shutil

dag_owner = 'Ian Kim'

default_args = {'owner': dag_owner,
        'depends_on_past': False,
        #'retries': 2,
        #'retry_delay': timedelta(minutes=5)
        }

yesterday = date.today() - timedelta(days=1)
YESTERDAY_YMD = yesterday.strftime("%Y-%m-%d")

# 벡터화 하기 위해서 S3에서 다운 받은 파일을 저장하는 경로
local_download_path = 'tmp/apt_txn_to_vector'
# 벡터화 한 데이터를 저장하는 경로
apt_txn_vector_path = 'tmp/apt_txn_vector'
os.makedirs(local_download_path, exist_ok=True)
os.makedirs(apt_txn_vector_path, exist_ok=True)

# --- 하이퍼파라미터 ---
TABULAR_MLP_COMPONENTS = 64

with DAG(dag_id='apt_vectorization',
        default_args=default_args,
        description='전처리된 아파트 매매 데이터를 벡터화 합니다.',
        start_date=datetime(2020, 2 ,2),
        schedule='0 8 * * 1-5',
        catchup=False,
        tags=['Vectorization','APT']
):
    @task
    def fetch_preprocessed_apt_txn_from_s3():
        print(f"{YESTERDAY_YMD}날짜의 전처리된 아파트 매매 데이터를 벡터화하기 위해 다운로드합니다.")

        s3_hook = S3Hook(aws_conn_id='s3_conn')
        bucket_name = 'real-estate-avm'
        prefix = f"processed/preprocessed_apt_txn/dt={YESTERDAY_YMD}/"

        keys = s3_hook.list_keys(bucket_name=bucket_name, prefix=prefix)

        file_keys = [k for k in keys if not k.endswith('/')]

        for key in file_keys:
            filename = os.path.basename(key)  # S3 객체 이름에서 파일명 추출
            local_file_path = os.path.join(local_download_path, filename)
            s3_hook.download_file(
                key=key,
                bucket_name=bucket_name,
                local_path=local_download_path
            )
        # 다운로드 후 확장자 없는 파일 .jpg로 변경 및 깨진 이미지 제거
        for f in os.listdir(local_download_path):
            file_path = os.path.join(local_download_path, f)
            if not os.path.splitext(f)[1]:
                new_file_path = file_path + '.csv'
                os.rename(file_path, new_file_path)
                file_path = new_file_path

        return local_download_path
    
    @task
    def vectorization_apt_txn():
        
        paths = [os.path.join(local_download_path, f) for f in os.listdir(local_download_path)]
        print(f"paths : {paths}")
        df = pd.read_csv(paths[0])
        print(df.columns)


        tabular_cols = ['전용면적(㎡)', '층', '건축년도', '아파트 나이']

        scaler = MinMaxScaler()
        tabular_scaled = scaler.fit_transform(df[tabular_cols]) 

        mlp_input = Input(shape=(len(tabular_cols),), name='tabular_input')
        embedding_layer = Dense(TABULAR_MLP_COMPONENTS, activation='relu')(mlp_input)
        mlp_extractor = Model(inputs=mlp_input, outputs=embedding_layer)

        apt_txn_features_64d = mlp_extractor.predict(tabular_scaled)
        print(apt_txn_features_64d.shape)
       
        save_path = f"{apt_txn_vector_path}/dt={YESTERDAY_YMD}.npy"
        np.save(save_path, apt_txn_features_64d)

        return save_path
    
    @task
    def save_vector_to_s3(vector_file_path:str):
        s3_hook = S3Hook(aws_conn_id='s3_conn')
        bucket_name = 'real-estate-avm'
        key = f"processed/apt_txn_vectors/dt={YESTERDAY_YMD}/{YESTERDAY_YMD}.npy"
        s3_hook.load_file(filename=vector_file_path, key=key, bucket_name=bucket_name, replace=True)
        return f"s3://{key}에 저장되었습니다."
    
    @task
    def cleanup_local_directory(local_download_path:str, apt_txn_vector_path:str):
        print(f"Local_download_path, apt_txn_vector_path를 모두 삭제합니다.")
        shutil.rmtree(local_download_path)
        shutil.rmtree(apt_txn_vector_path)

    fetch_preprocessed_apt_txn_from_s3_task = fetch_preprocessed_apt_txn_from_s3()
    vectorization_apt_txn_task = vectorization_apt_txn()
    save_vector_to_s3_task = save_vector_to_s3(vectorization_apt_txn_task)
    cleanup_local_directory_task = cleanup_local_directory(local_download_path, apt_txn_vector_path)

    fetch_preprocessed_apt_txn_from_s3_task >> vectorization_apt_txn_task >> save_vector_to_s3_task >> cleanup_local_directory_task