# 🏙️ 위성 이미지와 경제지표를 활용한 아파트 시세 예측 데이터 파이프라인

[![Python](https://img.shields.io/badge/Python-3.9+-blue?logo=python)](https://www.python.org)
[![Docker](https://img.shields.io/badge/Docker-20.10+-blue?logo=docker)](https://www.docker.com/)
[![Apache Airflow](https://img.shields.io/badge/Airflow-2.8-blue?logo=apache-airflow)](https://airflow.apache.org/)
[![dbt](https://img.shields.io/badge/dbt-1.7-orange?logo=dbt)](https://www.getdbt.com/)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)

## 1. 프로젝트 개요

본 프로젝트는 [**"위성 이미지와 경제지표를 활용한 TCN 기반 아파트 매매가격 예측"**](링크-투-논문-PDF) 연구를 실제 데이터 프로덕트로 구현하기 위한 **데이터 엔지니어링 파이프라인**입니다.

다양한 형태의 원천 데이터를 수집, 정제, 가공하여 ML 모델 학습에 필요한 최종 피처(Feature) 테이블을 안정적이고 자동화된 방식으로 생성하는 것을 목표로 합니다.

---

## 2. 시스템 아키텍처



본 파이프라인은 다음과 같은 흐름으로 데이터를 처리합니다.

1.  **수집 (Extract)**: Airflow 스케줄러가 매일/매월 정해진 시간에 각 API(공공데이터포털, GEE 등)를 호출하여 원천 데이터를 가져옵니다.
2.  **적재 (Load)**: 수집된 모든 원천 데이터는 원본 그대로 **AWS S3** 데이터 레이크에 저장됩니다.
3.  **변환 및 모델링 (Transform)**:
    * **정형 데이터**: dbt를 통해 S3의 데이터를 **Google BigQuery**로 가져와 정제 및 비즈니스 로직에 맞는 데이터 모델(Marts)로 변환합니다.
    * **비정형 데이터**: S3에 저장된 위성 이미지를 가져와 ResNet-50 모델로 특징 벡터를 추출하고, Parquet 형태로 다시 S3에 저장합니다.
4.  **품질 검증 (Data Quality)**: Great Expectations를 통해 변환된 데이터의 품질(예: Null 값, 데이터 형식)을 자동으로 검증합니다.
5.  **서빙 (Serve)**: 최종적으로 정형 데이터와 이미지 특징 벡터가 결합된 ML 모델 학습용 테이블을 BigQuery에 생성합니다.

---

## 3. 기술 스택

| 구분                  | 기술                                                               |
| --------------------- | ------------------------------------------------------------------ |
| **Data Orchestration**| `Apache Airflow`                                                   |
| **Data Lake** | `AWS S3`                                                           |
| **Data Warehouse** | `Google BigQuery`                                                  |
| **Data Transformation** | `dbt (data build tool)`                                            |
| **Data Quality** | `Great Expectations`                                               |
| **Infrastructure** | `Docker`, `Docker Compose`                                         |
| **Primary Language** | `Python`, `SQL`                                                    |

---

## 4. 프로젝트 실행 방법

### 사전 요구사항

* Python 3.9+
* Docker 및 Docker Compose

### 설치 및 실행

1.  **Git Repository 클론**
    ```bash
    git clone [https://github.com/](https://github.com/)[Your-Username]/[Your-Repo-Name].git
    cd [Your-Repo-Name]
    ```

2.  **환경 변수 설정**
    `.env.example` 파일을 복사하여 `.env` 파일을 생성하고, 각 API 키 및 클라우드 서비스 계정 정보를 입력합니다.
    ```bash
    cp .env.example .env
    ```

3.  **Docker Compose 실행**
    프로젝트 루트 디렉토리에서 다음 명령어를 실행하여 모든 서비스(Airflow, Postgres 등)를 실행합니다.
    ```bash
    docker-compose up -d
    ```

4.  **Airflow 접속 및 DAG 실행**
    * 웹 브라우저에서 `http://localhost:8080` 으로 접속합니다. (ID/PW: airflow/airflow)
    * `real_estate_pipeline` DAG를 활성화하고 실행합니다.

---

## 5. 디렉토리 구조

```
.
├── dags/                  # Airflow DAG 파이썬 파일
│   └── real_estate_pipeline.py
├── dbt/                   # dbt 프로젝트
│   ├── models/
│   │   ├── staging/
│   │   └── marts/
│   └── dbt_project.yml
├── great_expectations/    # Great Expectations 설정 및 결과
├── scripts/               # 데이터 수집, 이미지 처리 등 보조 스크립트
├── docker-compose.yml     # Docker 서비스 정의
├── Dockerfile             # Airflow 서비스용 Docker 이미지 정의
└── README.md
```

---

## 6. 문제 해결 과정 (Troubleshooting)

### 챌린지 1: 공공데이터포털 API의 잦은 타임아웃 및 호출 제한

**문제**: 실거래가 API는 일일 트래픽 제한이 있고, 특정 시간에는 응답이 매우 느려 파이프라인이 자주 실패했습니다.

**해결**:
1.  **Rate Limiter 적용**: API 호출 사이에 `time.sleep()`을 동적으로 적용하여 트래픽 제한을 넘지 않도록 제어했습니다.
2.  **Airflow Retry 설정**: `retries=3`, `retry_delay=timedelta(minutes=10)` 옵션을 사용하여 일시적인 네트워크 오류 발생 시 10분 간격으로 3번 재시도하도록 설정하여 파이프라인 안정성을 높였습니다.

### 챌린지 2: 대용량 위성 이미지 처리로 인한 메모리 부족

**문제**: 강남구 전체의 고해상도 위성 이미지를 한 번에 메모리에 로드하여 특징 벡터를 추출하는 과정에서 Airflow Worker의 메모리가 부족해지며 작업이 중단되었습니다.

**해결**:
1.  **Chunk Processing**: 이미지를 일정 크기의 작은 구역(Chunk)으로 나누고, 순차적으로 처리한 뒤 결과를 합치는 방식으로 변경하여 메모리 사용량을 최적화했습니다.
2.  **전용 처리 환경 분리**: (향후 개선점) 이미지 처리처럼 리소스를 많이 사용하는 작업은 별도의 AWS Batch나 ECS 같은 서비스로 분리하여 Airflow의 부담을 줄이는 아키텍처를 고려하고 있습니다.

---

## 7. 향후 개선 계획

* **CI/CD 도입**: GitHub Actions를 사용하여 코드 변경 시 자동으로 테스트하고 배포하는 CI/CD 파이프라인을 구축할 계획입니다.
* **Infrastructure as Code (IaC)**: Terraform을 도입하여 AWS, GCP 인프라를 코드로 관리하고 재생산성을 높일 것입니다.
* **Feature Store 도입**: Feast 또는 Tecton 같은 Feature Store를 도입하여 온라인/오프라인 피처의 일관성을 보장하고, 모델 서빙 시 지연 시간을 단축시킬 것입니다.

---

## 8. License

[MIT](https://choosealicense.com/licenses/mit/)
