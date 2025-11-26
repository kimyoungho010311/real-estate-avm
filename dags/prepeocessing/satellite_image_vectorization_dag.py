from airflow import DAG
from airflow.decorators import task
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from datetime import datetime, timedelta, date

import tensorflow as tf
from tensorflow.keras.layers import Input, Dense
from tensorflow.keras.models import Model
from tensorflow.keras.preprocessing.image import load_img, img_to_array
from tensorflow.keras.applications import ResNet50
from tensorflow.keras.applications.resnet50 import preprocess_input

import os
import numpy as np
import shutil



dag_owner = 'Ian Kim'

default_args = {'owner': dag_owner,
        'depends_on_past': False,
        # 'retries': 2,
        # 'retry_delay': timedelta(minutes=5)
        }

yesterday = date.today() - timedelta(days=1)
YESTERDAY_YMD = yesterday.strftime("%Y-%m-%d")
#YESTERDAY_YMD = '2025-09-24'
BATCH_SIZE = 64
IMAGE_PCA_COMPONENTS = 32

local_download_path = 'tmp/satellite_image_to_vector'
image_vector_path = 'tmp/image_vector_path'
os.makedirs(local_download_path, exist_ok=True)
os.makedirs(image_vector_path, exist_ok=True)

with DAG(dag_id='satellite_image_vectorization',
        default_args=default_args,
        description='전처리된 위성 이미지를 벡터화 합니다.',
        start_date=datetime(2020, 2, 2),
        schedule='0 8 * * 1-5',
        catchup=False,
        tags=['Vectorization']
):
    @task
    def fetch_preprocessed_image_from_s3():
        print(f"{YESTERDAY_YMD}날짜의 전처리된 이미지를 벡터화하기 위해 다운로드합니다.")

        s3_hook = S3Hook(aws_conn_id='s3_conn')
        bucket_name = 'real-estate-avm'
        prefix = f"processed/normalized-images/dt={YESTERDAY_YMD}/"

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
                new_file_path = file_path + '.jpg'
                os.rename(file_path, new_file_path)
                file_path = new_file_path

        return local_download_path
    
    @task
    def vectorization_image():

        def extract_image_features_2048d(image_paths, batch_size=BATCH_SIZE):
            """ResNet50으로 고차원 특징 벡터를 추출하는 헬퍼 함수"""
            image_input = Input(shape=(224, 224, 3), name='image_input')
            base_cnn = ResNet50(weights='imagenet', include_top=False, pooling='avg', input_tensor=image_input)
            base_cnn.trainable = False
            extractor_model = Model(inputs=image_input, outputs=base_cnn.output, name='resnet50_feature_extractor')
            
            def image_generator(paths, b_size):
                for i in range(0, len(paths), b_size):
                    batch_paths = paths[i:i+b_size]
                    batch_images = []
                    for p in batch_paths:
                        try:
                            img = load_img(p, target_size=(224, 224))
                            img_array = img_to_array(img)
                            batch_images.append(img_array)
                        except:
                            batch_images.append(np.zeros((224, 224, 3)))
                    batch_array = preprocess_input(np.array(batch_images))
                    yield (batch_array,)
            num_batches = int(np.ceil(len(image_paths) / batch_size))
            features = extractor_model.predict(image_generator(image_paths, batch_size), steps=num_batches, verbose=1)
            return features

        # .jpg 파일 전체 경로 불러오기
        image_paths = [
            os.path.join(local_download_path, f)
            for f in os.listdir(local_download_path)
            if os.path.isfile(os.path.join(local_download_path, f)) and f.lower().endswith(".jpg")
        ]
        image_features_2048d = extract_image_features_2048d(image_paths)
        # 2048차원 이미지 특징을 원하는 크기로 축소
        image_input = Input(shape=(2048,), name="image_feature_input")
        image_embedding = Dense(IMAGE_PCA_COMPONENTS, activation="relu")(image_input)
        image_extractor = Model(inputs=image_input, outputs=image_embedding, name="mlp_image_feature_extractor")


        # 이미지 특징 벡터 변환
        vector_32d = image_extractor.predict(image_features_2048d)  # train/test 구분 없이 사용 가능

        print(vector_32d.shape)
        vector_file_path = f'{image_vector_path}/dt={YESTERDAY_YMD}.npy'
        np.save(vector_file_path, vector_32d)

        return vector_file_path

    @task
    def save_vector_to_s3(vector_file_path:str):
        s3_hook = S3Hook(aws_conn_id='s3_conn')
        bucket_name = 'real-estate-avm'
        key = f"processed/image-vectors/dt={YESTERDAY_YMD}/{YESTERDAY_YMD}.npy"
        s3_hook.load_file(filename=vector_file_path, key=key, bucket_name=bucket_name, replace=True)
        return f"s3://{key}에 저장되었습니다."

    @task
    def cleanup_local_directory(local_download_path:str , image_vector_path:str):
        shutil.rmtree(local_download_path)
        shutil.rmtree(image_vector_path)



    fetch_preprocessed_image_from_s3_task = fetch_preprocessed_image_from_s3()
    vectorization_image_task = vectorization_image()
    save_vector_to_s3_task = save_vector_to_s3(vectorization_image_task)
    cleanup_local_directory_task = cleanup_local_directory(local_download_path, image_vector_path)
    
    fetch_preprocessed_image_from_s3_task >> vectorization_image_task >> save_vector_to_s3_task >> cleanup_local_directory_task