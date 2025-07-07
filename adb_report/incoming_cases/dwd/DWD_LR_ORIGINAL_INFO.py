import pymysql
import pandas as pd
import json
from datetime import datetime, date, timedelta

# 获取当前时间
current_time = datetime.now()
# 计算上一个月的时间
last_month_time = current_time - timedelta(days=current_time.day)
last_month_time = last_month_time.replace(day=1)
last_month = last_month_time.strftime('%Y-%m')

# 阿里云 AnalyticDB for MySQL 的相关参数 
# rr-uf636kzrob17osr7j.mysql.rds.aliyuncs.com
source_host = 'rr-uf636kzrob17osr7j.mysql.rds.aliyuncs.com'
source_port = 3306
source_user = 'prd_readonly'
source_password = '7MmY^nEJ3fQhysj=B'
source_db_name = 'claim_prd'




# 创建源数据库连接
source_connection = pymysql.connect(host=source_host,
                                     port=source_port,
                                     user=source_user,
                                     password=source_password,
                                     db=source_db_name,
                                     charset='utf8mb4')

# 连接目标数据库
target_host = 'am-uf61afo16ust6f600167320.ads.aliyuncs.com'
target_port = 3306
target_user = 'claim_all'
target_password = 'S#5DH1ar%*1n'
target_db_name = 'claim_all'

target_connection = pymysql.connect(host=target_host,
                                    port=target_port,
                                    user=target_user,
                                    password=target_password,
                                    db=target_db_name,
                                    charset='utf8mb4')

with target_connection.cursor() as target_cursor:
    delete_sql = f"""DELETE FROM CLAIM_DWD.DWD_LR_ORIGINAL_INFO 
                    WHERE date_format(update_time,'%Y-%m') = '{last_month}' """
    target_cursor.execute(delete_sql)
    target_connection.commit()



now = datetime.now()
start_time = now.strftime("%Y-%m-%d %H:%M:%S")
print("程序开始时间：", start_time)

batch_size = 3000  # 每次查询的数量
offset = 0

try:
    while True:
        # 创建游标对象
        with source_connection.cursor() as source_cursor:
            # 构造 SQL 语句
            sql = f"""SELECT
                            third_party_code,
                            create_time,
                            update_time,
                            message
                        FROM
                            claim_prd.pret_file
                        WHERE
                            message IS NOT NULL
                            AND delete_flag = '0'
                            AND date_format(update_time,'%Y-%m') = '{last_month}'
                            AND file_status = '99'
                        ORDER BY
                            update_time DESC
                    LIMIT {batch_size} OFFSET {offset};"""
            df = pd.read_sql_query(sql, source_connection)

            if df.empty:
                break

            data = []
            for index, row in df.iterrows():
                message_json = json.loads(row['message'])
                application_list = message_json.get('applicationList', [])
                if isinstance(application_list, list):
                    for item in application_list:
                        acceptNum = item.get('acceptNum', [])
                    for item in application_list:
                        bill_list = item.get('billList', [])
                        for bill in bill_list:
                            bill_no = bill.get('billNo')
                            billName = bill.get('billName')
                            visitTime = bill.get('visitTime')  # 就诊日期
                            sepcialNeed = bill.get('sepcialNeed')  # 是否特需
                            outpatientSpecial = bill.get('outpatientSpecial')  # 是否门特
                            billType = bill.get('billType')  # 账单类型
                            data.append([row['third_party_code'], row['create_time'], row['update_time'], acceptNum, bill_no, billName, visitTime, sepcialNeed, outpatientSpecial, billType])

            columns = ['third_party_code', 'create_time', 'update_time', 'acceptNum', 'bill_no', 'billName', 'visitTime', 'sepcialNeed', 'outpatientSpecial', 'billType']
            result_df = pd.DataFrame(data, columns=columns)



        with target_connection.cursor() as target_cursor:
            values = []
            for _, row in result_df.iterrows():
                values.append(tuple(row))
                if len(values) >= batch_size:
                    insert_sql = """INSERT INTO CLAIM_DWD.DWD_LR_ORIGINAL_INFO (third_party_code, create_time, update_time, acceptNum, bill_no, billName, visitTime, sepcialNeed, outpatientSpecial, billType) 
                                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"""
                    target_cursor.executemany(insert_sql, values)
                    target_connection.commit()
                    values = []

            # 插入剩余数据
            if values:
                insert_sql = """INSERT INTO CLAIM_DWD.DWD_LR_ORIGINAL_INFO (third_party_code, create_time, update_time, acceptNum, bill_no, billName, visitTime, sepcialNeed, outpatientSpecial, billType) 
                                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"""
                target_cursor.executemany(insert_sql, values)
                target_connection.commit()

        offset += batch_size

finally:
    # 关闭数据库连接
    source_connection.close()
    if 'target_connection' in locals():
        target_connection.close()
    end = datetime.now()
    end_time = end.strftime("%Y-%m-%d %H:%M:%S")
    print("程序结束时间：", end_time)