
def save_news_to_db(bucket_name, prefix):
    """
    S3에 저장된 뉴스 CSV 파일들을 DB에 저장하는 태스크
    """
    from airflow.providers.postgres.hooks.postgres import PostgresHook
    from airflow.providers.amazon.aws.hooks.s3 import S3Hook
    import pandas as pd
    from io import StringIO

    pg_hook = PostgresHook(postgres_conn_id='pg_conn')
    insert_sql = """
        INSERT INTO news (date, url, content, publisher)
        VALUES (%s, %s, %s, %s)
        ON CONFLICT (url) DO NOTHING;
    """
    s3_hook = S3Hook(aws_conn_id='s3_conn')
    keys = s3_hook.list_keys(bucket_name=bucket_name, prefix=prefix)
    
    # keys리스트 맨 앞에 이상한 경로가 포함되어 있어 제거함
    if keys and keys[0] == prefix:
        keys = keys[1:]

    #각 파일 읽기
    dfs = []
    for key in keys:
        if not key.endswith('.csv'):
            continue
        data_str = s3_hook.read_key(key=key, bucket_name=bucket_name)
        print(f"== {key} ==")
        print(data_str[:200])  # 처음 200자만 출력 예시
        df = pd.read_csv(StringIO(data_str))
        df = df[['date', 'url', 'content', 'publisher']]  # 순서 통일
        df['date'] = pd.to_datetime(df['date'], errors='coerce')  # timestamp 변환
        df.dropna(subset=['date', 'url'], inplace=True) # Drop rows with null date or url
        dfs.append(df)

    if not dfs:
        print("No news dataframes found in S3 to save to DB.")
        return ''
        
    final_df = pd.concat(dfs, ignore_index=True)

    # Insert to db each row
    for _, row in final_df.iterrows():
        pg_hook.run(insert_sql, parameters=(row['date'], row['url'], row['content'], row['publisher']))

    return ''
