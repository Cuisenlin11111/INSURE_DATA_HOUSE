# import sys
# sys.path.append(r"E:\pycharm\database")
from database import DatabaseConnection
from datetime import datetime, date, timedelta

today = date.today()

# 计算昨天的日期
yesterday = today - timedelta(days=1)

sql_query = f"""
  -- @description: 项目回传录入商信息表
  -- @param:
  -- @return:
  -- @alter:
  -- @time: 2024-06-20 15:01:06
  -- @author: 01
  -- @version: 1.0.0


insert into  CLAIM_DWD.DWD_POSTBACK_INPUT_INFO
SELECT
    INSURE_COMPANY_CHANNEL,
    ACCEPT_NUM,
    app_no,
      CASE substr( app_no, 13, 4 )
  WHEN 'SPIC' THEN '施博'
  WHEN 'KNVS' THEN '成都视觉'
  WHEN 'GTRS' THEN '广纳'
  WHEN 'ZZTN' THEN '智在'
  WHEN 'YSJU' THEN '因朔桔'
  ELSE '未知' END  input_company,
    claim_source,
    bill_no,
    treatment_date,
    CASE
        WHEN NOT EXISTS (
            SELECT 1
            FROM CLAIM_DWD.DWD_POSTBACK_TOTAL sub
            WHERE sub.ACCEPT_NUM = CLAIM_DWD.DWD_POSTBACK_TOTAL.ACCEPT_NUM
              AND sub.treatment_date IS NULL
        )
        THEN '门诊'
        ELSE '住院'
    END AS treatment_type,
    back_time,
    postback_way,
    is_back
FROM CLAIM_DWD.DWD_POSTBACK_TOTAL;
"""


def truncate_table(table_name='CLAIM_DWD.DWD_POSTBACK_INPUT_INFO'):
    with DatabaseConnection() as conn:
        truncate_sql = f"TRUNCATE TABLE {table_name}"
        with conn.cursor() as cursor:
            cursor.execute(truncate_sql)
            conn.commit()


def insert_data(sql_query):
    with DatabaseConnection() as conn:
        with conn.cursor() as cursor:
            cursor.execute(sql_query)
            conn.commit()


if __name__ == "__main__":
    start_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    print("程序开始时间：", start_time)
    truncate_table()
    insert_data(sql_query)
    end_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    print("程序结束时间：", end_time)
