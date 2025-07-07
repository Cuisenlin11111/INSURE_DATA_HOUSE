# import sys
# sys.path.append(r"E:\pycharm\database")
from database import DatabaseConnection
from datetime import datetime, timedelta

# 获取当前日期时间
current_date = datetime.now()
# 计算6个月前的日期
six_months_ago_date = current_date - timedelta(days=15)
six_months = six_months_ago_date.strftime('%Y-%m')



sql_query = f"""
  -- @description: 广纳录入费用统计
  -- @param:
  -- @return:
  -- @alter:
  -- @time: 2024-06-20 15:01:06
  -- @author: 01
  -- @version: 1.0.0
 INSERT INTO CLAIM_DWD.DWD_GN_INPUT_FEE_COUNT
                (insure_company_channel,
                 year_month,
                 mz_online_jls,
                 mz_online_aj,
                 mz_online_dj,
                 mz_online_price,
                 zy_online_jls,
                 zy_online_aj,
                 zy_online_dj,
                 zy_online_price,
                 price_count,
                 data_dt)
WITH a1 AS (
    SELECT INSURE_COMPANY_CHANNEL,
           SUBSTR(back_time, 1, 7) AS dt_month,
           COUNT(DISTINCT CASE WHEN treatment_type = '门诊' THEN bill_no ELSE NULL END) AS mz_online_jls,
           COUNT(DISTINCT CASE WHEN treatment_type = '门诊' THEN ACCEPT_NUM ELSE NULL END) AS mz_online_aj
    FROM CLAIM_DWD.DWD_POSTBACK_INPUT_INFO
    WHERE ACCEPT_NUM NOT LIKE '%BL%' AND input_company = '广纳' and  SUBSTR(back_time, 1, 7)>='{six_months}'
    GROUP BY INSURE_COMPANY_CHANNEL,
             SUBSTR(back_time, 1, 7)
),
a2 AS (
    SELECT insure_company_channel,
           dt_month,
           COUNT(DISTINCT ACCEPT_NUM) + SUM(zy_online_zhs) AS zy_online_jls,
           COUNT(DISTINCT ACCEPT_NUM) AS zy_online_aj
    FROM (
        SELECT insure_company_channel,
               dt_month,
               ACCEPT_NUM,
               CASE
                   WHEN zy_bill_count <= 8 THEN 0
                   ELSE FLOOR(zy_bill_count / 8) - 1 + MOD(zy_bill_count, 8) * 0.125
               END AS zy_online_zhs
        FROM (
            SELECT INSURE_COMPANY_CHANNEL,
                   ACCEPT_NUM,
                   SUBSTR(back_time, 1, 7) AS dt_month,
                   COUNT(DISTINCT bill_no) AS zy_bill_count
            FROM CLAIM_DWD.DWD_POSTBACK_INPUT_INFO
            WHERE ACCEPT_NUM NOT LIKE '%BL%' AND input_company = '广纳'
              AND treatment_type = '住院'
              and  SUBSTR(back_time, 1, 7)>='{six_months}'
            GROUP BY INSURE_COMPANY_CHANNEL,
                     ACCEPT_NUM,
                     SUBSTR(back_time, 1, 7)
        ) ff
    )
    GROUP BY insure_company_channel,
             dt_month
)
SELECT insure_company_channel,
       dt_month,
       mz_online_jls,
       mz_online_aj,
       CAST(mz_online_dj AS DECIMAL(10, 2)) AS mz_online_dj,
       CAST(mz_online_jls * mz_online_dj AS DECIMAL(10, 2)) AS mz_online_price,
       CAST(zy_online_jls AS BIGINT) AS zy_online_jls,
       zy_online_aj,
       CAST(zy_online_dj AS DECIMAL(10, 2)) AS zy_online_dj,
       CAST(zy_online_jls * zy_online_dj AS DECIMAL(10, 2)) AS zy_online_price,
       CAST(mz_online_jls * mz_online_dj + zy_online_jls * zy_online_dj AS DECIMAL(10, 2)) AS price_count,
       REPLACE(CURDATE(), '-', '')
FROM (
    SELECT dim.CHANNEL_VALUE AS insure_company_channel,
           a1.dt_month,
           COALESCE(a1.mz_online_jls, 0) AS mz_online_jls,
           COALESCE(a1.mz_online_aj, 0) AS mz_online_aj,
           CASE
               WHEN a1.insure_company_channel IS NOT NULL THEN 0.8
               ELSE 0
           END AS mz_online_dj,
           COALESCE(a2.zy_online_jls, 0) AS zy_online_jls,
           COALESCE(a2.zy_online_aj, 0) AS zy_online_aj,
           CASE
               WHEN a1.insure_company_channel IS NOT NULL THEN 8.73
               ELSE 0
           END AS zy_online_dj
    FROM a1
    LEFT JOIN a2 ON a1.insure_company_channel = a2.insure_company_channel
                   AND a1.dt_month = a2.dt_month
    INNER JOIN claim_ods.dim_insure_company_channel dim ON a1.insure_company_channel = dim.CHANNEL_KEY
);
"""
def truncate_table(table_name='CLAIM_DWD.DWD_GN_INPUT_FEE_COUNT'):
    with DatabaseConnection() as conn:
        truncate_sql = f"delete from  {table_name}  where  `YEAR_MONTH`>='{six_months}' "
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
