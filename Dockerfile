FROM apache/airflow:3.0.4

# 패키지 설치는 root로
USER root

RUN apt-get update && \
    apt-get install -y chromium chromium-driver wget unzip && \
    apt-get clean && rm -rf /var/lib/apt/lists/*

USER airflow
# Selenium 설치
RUN pip install --no-cache-dir selenium

RUN pip install --no-cache-dir numpy

RUN apt-get update && apt-get install -y \
    wget unzip xvfb libxi6 libgconf-2-4 \
    && wget -O /tmp/chromedriver.zip https://chromedriver.storage.googleapis.com/116.0.5845.96/chromedriver_linux64.zip \
    && unzip /tmp/chromedriver.zip -d /usr/local/bin/ \
    && chmod +x /usr/local/bin/chromedriver
RUN wget -q https://dl.google.com/linux/direct/google-chrome-stable_current_amd64.deb \
    && dpkg -i google-chrome-stable_current_amd64.deb || apt-get -fy install