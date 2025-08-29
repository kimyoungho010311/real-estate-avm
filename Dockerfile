FROM apache/airflow:3.0.4-python3.10

# root 권한으로 전환
USER root

# 크롬 및 드라이버 설치
RUN apt-get update && \
    apt-get install -y wget unzip xvfb libxi6 libgconf-2-4 chromium chromium-driver && \
    wget -q https://dl.google.com/linux/direct/google-chrome-stable_current_amd64.deb && \
    apt-get install -y ./google-chrome-stable_current_amd64.deb || apt-get -fy install && \
    rm -f google-chrome-stable_current_amd64.deb && \
    apt-get clean && rm -rf /var/lib/apt/lists/*

RUN mkdir -p /opt/airflow/keys
COPY gee-service-account.json /opt/airflow/keys/gee-service-account.json
RUN chown airflow: /opt/airflow/keys/gee-service-account.json

# 서울 시간대 설정
ENV TZ=Asia/Seoul
RUN ln -snf /usr/share/zoneinfo/$TZ /etc/localtime && echo $TZ > /etc/timezone

# airflow 계정으로 복귀
USER airflow


# 파이썬 패키지
RUN pip uninstall -y ee && \
    pip install --no-cache-dir \
        selenium \
        numpy \
        earthengine-api \
        Pillow \
        torch \
        torchvision