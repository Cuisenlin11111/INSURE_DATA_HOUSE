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
  -- @description: 施博录入费用统计
  -- @param:
  -- @return:
  -- @alter:
  -- @time: 2024-06-20 15:01:06
  -- @author: 01
  -- @version: 1.0.0

INSERT INTO CLAIM_DWD.DWD_SH_INPUT_FEE_COUNT (
    insure_company_channel,
    year_month,
    hospital_claim_count,
    hospital_jsjl_count,
    hospital_price,
    hospital_fee,
    mz_online_claim_count,
    mz_online_jsjl_count,
    mz_online_price,
    mz_online_fee,
    mz_offline_claim_count,
    mz_offline_jsjl_count,
    mz_offline_price,
    mz_offline_fee,
    total_fee,
    data_dt
)
WITH a1 AS (
    -- 门诊线上
    -- 中智的线上案件自动分配给成都视觉，施博录入中智线上为0
    SELECT
        insure_company_channel,
        dt_month,
        COUNT(DISTINCT ACCEPT_NUM) + SUM(mz_online_jsjl) AS mz_online_jls,
        COUNT(DISTINCT ACCEPT_NUM) AS mz_online_aj
    FROM (
        SELECT
            insure_company_channel,
            dt_month,
            ACCEPT_NUM,
            CASE
                WHEN mz_online_bill_count <= 8 THEN 0
                WHEN mz_online_bill_count > 8 THEN FLOOR(mz_online_bill_count / 8) - 1 + MOD(mz_online_bill_count, 8) * 0.125
            END AS mz_online_jsjl
        FROM (
            SELECT
                INSURE_COMPANY_CHANNEL,
                ACCEPT_NUM,
                SUBSTR(back_time, 1, 7) AS dt_month,
                COUNT(DISTINCT bill_no) AS mz_online_bill_count
            FROM CLAIM_DWD.DWD_POSTBACK_INPUT_INFO
            WHERE ACCEPT_NUM NOT LIKE '%BL%'
              AND input_company = '施博'
              AND treatment_type = '门诊'
              AND claim_source IN ('2', '4')
              and  SUBSTR(back_time, 1, 7)>='{six_months}'
            GROUP BY INSURE_COMPANY_CHANNEL,
                     ACCEPT_NUM,
                     SUBSTR(back_time, 1, 7)
        )
    ) ff
    GROUP BY insure_company_channel, dt_month
),
a2 AS (
    -- 门诊线下原始案件数（回传成功）
    -- 非中智线下案件
    SELECT
        insure_company_channel,
        dt_month,
        COUNT(DISTINCT ACCEPT_NUM) + SUM(mz_online_jsjl) AS mz_offline_jls,
        COUNT(DISTINCT ACCEPT_NUM) AS mz_offline_aj
    FROM (
        SELECT
            insure_company_channel,
            dt_month,
            ACCEPT_NUM,
            CASE
                WHEN mz_online_bill_count <= 8 THEN 0
                WHEN mz_online_bill_count > 8 THEN FLOOR(mz_online_bill_count / 8) - 1 + MOD(mz_online_bill_count, 8) * 0.125
            END AS mz_online_jsjl
        FROM (
            SELECT
                INSURE_COMPANY_CHANNEL,
                ACCEPT_NUM,
                SUBSTR(back_time, 1, 7) AS dt_month,
                COUNT(DISTINCT bill_no) AS mz_online_bill_count
            FROM CLAIM_DWD.DWD_POSTBACK_INPUT_INFO
            WHERE ACCEPT_NUM NOT LIKE '%BL%'
              AND input_company = '施博'
              AND treatment_type = '门诊'
              AND claim_source IN ('1', '3')
              and  SUBSTR(back_time, 1, 7)>='{six_months}'
            GROUP BY INSURE_COMPANY_CHANNEL,
                     ACCEPT_NUM,
                     SUBSTR(back_time, 1, 7)
        )
    ) ff
    GROUP BY insure_company_channel, dt_month
),
a3 AS (
    -- 住院原始案件数
    SELECT
        insure_company_channel,
        dt_month,
        COUNT(DISTINCT ACCEPT_NUM) + SUM(zy_online_zhs) AS zy_online_jls,
        COUNT(DISTINCT ACCEPT_NUM) AS zy_online_aj
    FROM (
        SELECT
            insure_company_channel,
            dt_month,
            ACCEPT_NUM,
            CASE
                WHEN zy_bill_count <= 8 THEN 0
                ELSE FLOOR(zy_bill_count / 8) - 1 + MOD(zy_bill_count, 8) * 0.125
            END AS zy_online_zhs
        FROM (
            SELECT
                INSURE_COMPANY_CHANNEL,
                ACCEPT_NUM,
                SUBSTR(back_time, 1, 7) AS dt_month,
                COUNT(DISTINCT bill_no) AS zy_bill_count
            FROM CLAIM_DWD.DWD_POSTBACK_INPUT_INFO
            WHERE ACCEPT_NUM NOT LIKE '%BL%'
              AND input_company = '施博'
              AND treatment_type = '住院'
              and  SUBSTR(back_time, 1, 7)>='{six_months}'
            GROUP BY INSURE_COMPANY_CHANNEL,
                     ACCEPT_NUM,
                     SUBSTR(back_time, 1, 7)
        )
    ) ff
    GROUP BY insure_company_channel, dt_month
)
SELECT
    insure_company_channel,
    `YEAR_MONTH`,
    hospital_claim_count,
    hospital_jsjl_count,
    hospital_price,
    hospital_fee,
    mz_online_claim_count,
    mz_online_jsjl_count,
    mz_online_price,
    mz_online_fee,
    mz_offline_claim_count,
    mz_offline_jsjl_count,
    mz_offline_price,
    mz_offline_fee,
    CAST(hospital_fee + mz_online_fee + mz_offline_fee AS DECIMAL(10, 2)) AS total_fee,
    REPLACE(CURDATE(), '-', '')
FROM (
    SELECT
        dim.CHANNEL_VALUE AS insure_company_channel,
        dim.dt_month AS `YEAR_MONTH`,
        COALESCE(a3.zy_online_aj, 0) AS hospital_claim_count,
        CAST(COALESCE(a3.zy_online_jls, 0) AS BIGINT) AS hospital_jsjl_count,
        CAST(11 AS DECIMAL(10, 2)) AS hospital_price,
        CAST(COALESCE(a3.zy_online_aj, 0) * 11 + COALESCE(a3.zy_online_jls - a3.zy_online_aj, 0) * 4.9 AS DECIMAL(10, 2)) AS hospital_fee,
        CAST(COALESCE(a1.mz_online_aj, 0) AS BIGINT) AS mz_online_claim_count,
        CAST(COALESCE(a1.mz_online_jls, 0) AS BIGINT) AS mz_online_jsjl_count,
        CAST( 2.60 AS DECIMAL(10, 2)) AS mz_online_price,
        CAST(COALESCE(a1.mz_online_jls, 0) * 2.6 AS DECIMAL(10, 2)) AS mz_online_fee,
        COALESCE(a2.mz_offline_aj, 0) AS mz_offline_claim_count,
        CAST(COALESCE(a2.mz_offline_jls, 0) AS BIGINT) AS mz_offline_jsjl_count,
        5.90 AS mz_offline_price,
        CAST(COALESCE(a2.mz_offline_jls, 0) * 5.9 AS DECIMAL(10, 2)) AS mz_offline_fee
    FROM claim_dim.dim_insure_company_channel dim
    LEFT JOIN a1 ON dim.CHANNEL_KEY = a1.insure_company_channel
                  AND dim.dt_month = a1.dt_month
    LEFT JOIN a2 ON dim.CHANNEL_KEY = a2.insure_company_channel
                  AND dim.dt_month = a2.dt_month
    LEFT JOIN a3 ON dim.CHANNEL_KEY = a3.insure_company_channel
                  AND dim.dt_month = a3.dt_month
    WHERE dim.dt_month <= SUBSTRING(DATE_SUB(CURDATE(), INTERVAL 1 DAY), 1, 7)
);
"""
def truncate_table(table_name='CLAIM_DWD.DWD_SH_INPUT_FEE_COUNT'):
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
