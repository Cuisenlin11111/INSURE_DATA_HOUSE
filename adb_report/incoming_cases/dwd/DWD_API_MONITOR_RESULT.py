import pandas as pd
from sqlalchemy import create_engine
import numpy as np
from database import DatabaseConnection
import time
from datetime import datetime, date, timedelta

today = date.today()
yesterday = today - timedelta(days=1)

now = datetime.now()
# 获取小时数
hour = now.hour
# print(hour)
if hour > 8:
    fromdate = str(today)
else:
    fromdate = str(yesterday)


def calculate_execution_time(func):
    def wrapper(*args, **kwargs):
        start_time = time.time()
        result = func(*args, **kwargs)
        end_time = time.time()
        print(f"{func.__name__} 函数的执行时间为: {end_time - start_time} 秒")
        return result
    return wrapper


@calculate_execution_time
def main():
    # 数据库连接信息
    host = 'rr-uf636kzrob17osr7j.mysql.rds.aliyuncs.com'
    port = 3306
    user = 'prd_readonly'
    password = '7MmY^nEJ3fQhysj=B'
    db_name = 'claim_prd'
    # 建立数据库连接
    engine = create_engine(f'mysql+pymysql://{user}:{password}@{host}:{port}/{db_name}')
    try:
        # 从数据库中读取数据
        query = f"""
        SELECT app_name, request_uri, request_time, response_status, time_taken
        FROM api_monitor
        WHERE substr(request_time,1,10) = '{fromdate}'
        """
        df = pd.read_sql(query, engine)
        # 确保 request_time 列是字符串类型
        df['request_time'] = df['request_time'].astype(str)
        df['request_time_date'] = df['request_time'].str[:10]
        # 计算 num, error_num, error_rate 和 avg_time
        t = df.groupby(['app_name', 'request_uri', 'request_time_date']).agg(
            num=('app_name', 'count'),
            error_num=('response_status', lambda x: (x!= 200).sum()),
            avg_time=('time_taken', 'mean')
        ).reset_index()
        t['error_rate'] = t['error_num'] / t['num']
        # 计算 95 线和 99 线
        def calculate_percentile(group, percentile):
            sorted_group = group.sort_values(by='time_taken')
            index = int(len(sorted_group) * percentile)
            if index < len(sorted_group):
                return sorted_group.iloc[index]['time_taken']
            else:
                return None
        t1 = df.groupby(['request_uri', 'request_time_date']).apply(lambda x: pd.Series({
            'time_taken_99': calculate_percentile(x, 0.99),
            'time_taken_95': calculate_percentile(x, 0.95)
        })).reset_index()
        # 合并结果
        result = pd.merge(t, t1, on=['request_uri', 'request_time_date'], how='left')
        # 将结果写入数据库表
        truncate_table()
        insert_data(result)
        print("success")
    finally:
        engine.dispose()


def truncate_table():
    with DatabaseConnection() as conn:
        truncate_sql = f"""  delete from   CLAIM_DWD.DWD_API_MONITOR_RESULT  where request_time= '{fromdate}' """
        with conn.cursor() as cursor:
            cursor.execute(truncate_sql)
            conn.commit()


def insert_data(result):
    with DatabaseConnection() as conn:
        # 准备插入数据
        data_to_insert = []
        for index, row in result.iterrows():
            data_to_insert.append((row['app_name'], row['request_uri'], row['request_time_date'], row['num'], row['error_num'],
                                 row['error_rate'], row['avg_time'], row['time_taken_99'], row['time_taken_95']))
        insert_sql = """
        INSERT INTO CLAIM_DWD.DWD_API_MONITOR_RESULT (app_name, request_uri, request_time, num, error_num, error_rate, avg_time, `time_taken_99`, `time_taken_95`)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
        """
        with conn.cursor() as cursor:
            cursor.executemany(insert_sql, data_to_insert)
            conn.commit()


if __name__ == "__main__":
    main()