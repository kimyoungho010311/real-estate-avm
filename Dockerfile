FROM apache/airflow:3.0.4

# 패키지 설치는 root로
USER root

RUN apt-get update && \
    apt-get install -y chromium chromium-driver wget unzip && \
    apt-get clean && rm -rf /var/lib/apt/lists/*

USER airflow
# Selenium 설치
RUN pip install --no-cache-dir selenium
