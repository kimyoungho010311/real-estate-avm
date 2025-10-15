# 모든 DAG에서 사용되는 변수를 모아둔 스크립트 입니다.

from datetime import date, timedelta

today = date.today()
TODAY_YMD = today.strftime("%Y-%m-%d")
TODAY_YM = today.strftime("%Y-%m")

yesterday = date.today() - timedelta(days=1)
YESTERDAY_YMD = yesterday.strftime("%Y-%m-%d")
YESTERDAY_YM = yesterday.strftime("%Y-%m")