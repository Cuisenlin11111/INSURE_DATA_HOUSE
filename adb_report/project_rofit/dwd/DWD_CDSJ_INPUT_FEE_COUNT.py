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
  -- @description: 成都视觉录入费用统计
  -- @param:
  -- @return:
  -- @alter:
  -- @time: 2024-06-20 15:01:06
  -- @author: 01
  -- @version: 1.0.0
-- 插入数据到 CLAIM_DWD.DWD_CDSJ_INPUT_FEE_COUNT 表
INSERT INTO CLAIM_DWD.DWD_CDSJ_INPUT_FEE_COUNT (
    insure_company_channel,
    `YEAR_MONTH`,
    mz_online_jls,
    mz_online_aj,
    mz_online_dj,
    mz_online_price,
    zy_online_jls,
    zy_online_aj,
    zy_online_dj,
    zy_online_price,
    price_count,
    data_dt
)
WITH a1 AS (
    -- 成都视觉--线上--线上门诊发票结算记录数，线上门诊发票案件数
    -- 从 CLAIM_DWD.DWD_POSTBACK_INPUT_INFO 表查询部分数据并分组统计
    SELECT
        INSURE_COMPANY_CHANNEL,
        SUBSTR(back_time, 1, 7) AS dt_month,
        COUNT(DISTINCT CASE WHEN treatment_type = '门诊' THEN bill_no ELSE NULL END) AS mz_online_jls,
        COUNT(DISTINCT CASE WHEN treatment_type = '门诊' THEN ACCEPT_NUM ELSE NULL END) AS mz_online_aj
    FROM CLAIM_DWD.DWD_POSTBACK_INPUT_INFO
    WHERE ACCEPT_NUM NOT LIKE "%BL%" AND input_company = '成都视觉'
    and  SUBSTR(back_time, 1, 7)>='{six_months}'
    GROUP BY INSURE_COMPANY_CHANNEL,
             SUBSTR(back_time, 1, 7)
),
a2 AS (
    -- 成都视觉--线上--线上住院案件结算记录数，线上住院案件数
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
            -- 从 CLAIM_DWD.DWD_POSTBACK_INPUT_INFO 表查询部分住院相关数据并分组统计
            SELECT
                INSURE_COMPANY_CHANNEL,
                ACCEPT_NUM,
                SUBSTR(back_time, 1, 7) AS dt_month,
                COUNT(DISTINCT bill_no) AS zy_bill_count
            FROM CLAIM_DWD.DWD_POSTBACK_INPUT_INFO
            WHERE ACCEPT_NUM NOT LIKE "%BL%" AND input_company = '成都视觉'
              AND treatment_type = '住院'
            and  SUBSTR(back_time, 1, 7)>='{six_months}'
            GROUP BY INSURE_COMPANY_CHANNEL,
                     ACCEPT_NUM,
                     SUBSTR(back_time, 1, 7)
            UNION ALL
            -- 从 CLAIM_DWD.DWD_CP07_DETAIL 表查询部分住院相关数据并分组统计
            SELECT
                INSURE_COMPANY_CHANNEL,
                ACCEPT_NUM,
                SUBSTR(DATE_MON, 1, 7) AS dt_month,
                SUM(INVOICE_NUMBER) AS zy_bill_count
            FROM CLAIM_DWD.DWD_CP07_DETAIL
            WHERE TREATMENT_TYPE = '住院'
            GROUP BY INSURE_COMPANY_CHANNEL,
                     SUBSTR(DATE_MON, 1, 7),
                     ACCEPT_NUM
        )
    ) ff
    GROUP BY insure_company_channel,
             dt_month
)
-- 从子查询结果中选择数据并进行类型转换、计算等操作后插入目标表
SELECT
    insure_company_channel,
    dt_month,
    mz_online_jls,
    mz_online_aj,
    CAST(mz_online_dj AS DECIMAL(10, 2)) AS mz_online_dj,
    CAST(mz_online_jls * mz_online_dj AS DECIMAL(10, 2)) AS mz_online_price,
    CAST(zy_online_jls AS BIGINT) AS zy_online_jls,
    CAST(zy_online_aj AS BIGINT) AS zy_online_aj,
    CAST(zy_online_dj AS DECIMAL(10, 2)) AS zy_online_dj,
    CAST(zy_online_jls * zy_online_dj AS DECIMAL(10, 2)) AS zy_online_price,
    CAST(mz_online_jls * mz_online_dj + zy_online_jls * zy_online_dj AS DECIMAL(10, 2)) AS price_count,
    REPLACE(CURDATE(), '-', '')
FROM (
    SELECT
        dim.CHANNEL_VALUE AS insure_company_channel,
        a1.dt_month,
        COALESCE(a1.mz_online_jls, 0) AS mz_online_jls,
        COALESCE(a1.mz_online_aj, 0) AS mz_online_aj,
        -- 根据不同的 insure_company_channel 设置 mz_online_dj 的值
        CASE
            WHEN a1.insure_company_channel IN ('CP01', 'TK01', 'TK02', 'TK03', 'TK04', 'TK06', 'TK07', 'TK08', 'TK09', 'GS01', 'DJ01', 'CP08', 'DJ01', 'BH01', 'CP02', 'ZA01', 'CP07', '大连分公司', '苏州分公司') THEN
                0.95
            WHEN a1.insure_company_channel = 'PA02' THEN
                1
            ELSE
                0
        END AS mz_online_dj,
        COALESCE(a2.zy_online_jls, 0) AS zy_online_jls,
        COALESCE(a2.zy_online_aj, 0) AS zy_online_aj,
        -- 根据不同的 insure_company_channel 设置 zy_online_dj 的值
        CASE
            WHEN a1.insure_company_channel IN ('CP01', 'TK01', 'TK02', 'TK03', 'TK04', 'TK06', 'TK07', 'TK08', 'TK09', 'GS01', 'DJ01', 'CP08', 'DJ01', 'BH01', 'CP02', 'ZA01', 'CP07', '大连分公司', '苏州分公司') THEN
                9.6
            WHEN a1.insure_company_channel = 'PA02' THEN
                9.7
            ELSE
                0
        END AS zy_online_dj
    FROM a1
    LEFT JOIN a2 ON a1.insure_company_channel = a2.insure_company_channel
                   AND a1.dt_month = a2.dt_month
    INNER JOIN claim_ods.dim_insure_company_channel dim ON a1.insure_company_channel = dim.CHANNEL_KEY
) ;

"""
def truncate_table(table_name='CLAIM_DWD.DWD_CDSJ_INPUT_FEE_COUNT'):
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
