# import sys
#
# sys.path.append(r"E:\pycharm\database")
from database import DatabaseConnection
from datetime import datetime, timedelta

# 获取当前日期时间
current_date = datetime.now()
# 计算6个月前的日期
six_months_ago_date = current_date - timedelta(days=30)
six_months = six_months_ago_date.strftime('%Y-%m')

sql_query = f"""
  -- @description: 泰康项目账单汇总
  -- @param:
  -- @return:
  -- @alter:
  -- @time: 2024-06-20 15:01:06
  -- @author: 01
  -- @version: 1.0.0

INSERT INTO CLAIM_DWD.DWD_TK_BILL_TOTAL
(
    insure_company_channel,
    `YEAR_MONTH`,
    claim_type,
    is_back,
    claim_vol,
    c_inv_no,
    price_dj,
    fee,
    data_dt
)
WITH
    hh AS (
        SELECT
            substr(comm_date, 1, 7) AS `YEAR_MONTH`,
            '泰康养老北京分公司' AS INSURE_COMPANY_CHANNEL,
            ACCEPT_NUM,
            CLAIM_TYPE,
            is_back,
            C_INV_NO,
            CASE
                WHEN CLAIM_TYPE = '线上' AND is_back = '结案' THEN 1.55
                WHEN CLAIM_TYPE = '线下' AND is_back = '结案' THEN 1.9
                WHEN CLAIM_TYPE = '线上' AND is_back = '撤案' THEN 0.775
                WHEN CLAIM_TYPE = '线下' AND is_back = '撤案' THEN 0.95
            END AS 单价
        FROM CLAIM_DWD.DWD_TK_BILL_DETAIL_NEW
        WHERE INSURE_COMPANY_CHANNEL = '泰康养老北京分公司'
        and  substr(comm_date, 1, 7)>='{six_months}'
    ),
    b3 AS (
        -- 泰康养老上海分公司
        SELECT
            INSURE_COMPANY_CHANNEL,
            substr(comm_date, 1, 7) AS `YEAR_MONTH`,
            claim_type,
            is_back,
            count(*) AS claim_vol,
            sum(c_inv_no) AS c_inv_no,
            CASE
                WHEN claim_type = '线上' AND is_back = '撤案' AND ACCEPT_NUM NOT LIKE '%BL%' THEN 0.975
                WHEN claim_type = '线上' AND is_back = '结案' THEN 1.95
                WHEN claim_type = '线下' AND is_back = '撤案' AND ACCEPT_NUM NOT LIKE '%BL%' THEN 1.25
                WHEN claim_type = '线下' AND is_back = '结案' THEN 2.5
                ELSE 0
            END AS price_dj
        FROM CLAIM_DWD.DWD_TK_BILL_DETAIL_NEW
        WHERE INSURE_COMPANY_CHANNEL = '泰康养老上海分公司'
        and  substr(comm_date, 1, 7)>='{six_months}'
        GROUP BY
            INSURE_COMPANY_CHANNEL,
            substr(comm_date, 1, 7),
            claim_type,
            is_back
    ),
    t2 AS (
        SELECT
            insure_company_channel,
            substr(COMM_DATE, 1, 7) AS `YEAR_MONTH`,
            CLAIM_TYPE,
            IS_BACK,
            ACCEPT_NUM,
            C_INV_NO,
            CASE
                WHEN claim_type = '线上'  AND is_back = '结案' THEN 7.5
                WHEN claim_type = '线上'  AND is_back = '撤案' THEN 3.75
                WHEN claim_type = '线下' AND is_back = '结案' THEN 12.5
                WHEN claim_type = '线下' AND is_back = '撤案' THEN 6.25
                ELSE 0
            END AS PRICE_DJ,
                CHAO8 * 1 AS PRICE_chao8
        FROM CLAIM_DWD.DWD_TK_BILL_DETAIL_NEW
        WHERE INSURE_COMPANY_CHANNEL = '泰康养老河南分公司'
        and  substr(comm_date, 1, 7)>='{six_months}'
    ),
    t3 AS (
        -- 泰康养老山东分公司
        SELECT
            insure_company_channel,
            substr(COMM_DATE, 1, 7) AS `YEAR_MONTH`,
            CLAIM_TYPE,
            IS_BACK,
            ACCEPT_NUM,
            C_INV_NO,
            CASE
                WHEN CLAIM_TYPE = '线下' THEN 9
                WHEN CLAIM_TYPE = '线上' THEN 6.8
            END AS PRICE_DJ,
            CASE
                WHEN C_INV_NO > 8 THEN CHAO8 * 0.67
                ELSE 0
            END AS PRICE_chao8
        FROM CLAIM_DWD.DWD_TK_BILL_DETAIL_NEW
        WHERE INSURE_COMPANY_CHANNEL = '泰康养老山东分公司'
        AND IS_BACK = '结案'
        and  substr(comm_date, 1, 7)>='{six_months}'
    ),
    t4 AS (
        -- 泰康养老江苏分公司
        SELECT
            insure_company_channel,
            substr(COMM_DATE, 1, 7) AS `YEAR_MONTH`,
            CLAIM_TYPE,
            IS_BACK,
            ACCEPT_NUM,
            C_INV_NO,
            CASE
                WHEN IS_BACK = '结案' THEN 7
                WHEN IS_BACK = '撤案' THEN 3.5
            END AS PRICE_DJ,
            0 AS PRICE_chao8
        FROM CLAIM_DWD.DWD_TK_BILL_DETAIL_NEW
        WHERE INSURE_COMPANY_CHANNEL = '泰康养老江苏分公司'
        and  substr(comm_date, 1, 7)>='{six_months}'
    )
SELECT
    INSURE_COMPANY_CHANNEL,
    `YEAR_MONTH`,
    claim_type,
    is_back,
    count(ACCEPT_NUM) AS claim_vol,
    sum(c_inv_no),
    CAST(单价 AS DECIMAL(10, 2)),
    CAST(sum(单价 * c_inv_no) AS DECIMAL(10, 2)),
    REPLACE(curdate(), '-', '')
FROM hh
GROUP BY insure_company_channel, `YEAR_MONTH`, claim_type, is_back
UNION ALL
SELECT
    insure_company_channel,
    `YEAR_MONTH`,
    claim_type,
    is_back,
    claim_vol,
    c_inv_no,
    CAST(price_dj AS DECIMAL(10, 2)),
    CAST(price_dj * c_inv_no AS DECIMAL(10, 2)),
    REPLACE(curdate(), '-', '')
FROM b3
UNION ALL
SELECT
    insure_company_channel,
    `YEAR_MONTH`,
    claim_type,
    is_back,
    count(ACCEPT_NUM) AS claim_vol,
    c_inv_no,
    CAST(price_dj AS DECIMAL(10, 2)),
    CAST(sum(price_dj) + sum(PRICE_chao8) AS DECIMAL(10, 2)),
    REPLACE(curdate(), '-', '')
FROM t2
GROUP BY insure_company_channel, `YEAR_MONTH`, claim_type, is_back
UNION ALL
SELECT
    insure_company_channel,
    `YEAR_MONTH`,
    claim_type,
    is_back,
    count(ACCEPT_NUM) AS claim_vol,
    sum(c_inv_no),
    CAST(price_dj AS DECIMAL(10, 2)),
    CAST(sum(price_dj) + sum(PRICE_chao8) AS DECIMAL(10, 2)),
    REPLACE(curdate(), '-', '')
FROM t3
GROUP BY insure_company_channel, `YEAR_MONTH`, claim_type
UNION ALL
SELECT
    insure_company_channel,
    `YEAR_MONTH`,
    claim_type,
    is_back,
    count(ACCEPT_NUM) AS claim_vol,
    sum(c_inv_no),
    CAST(price_dj AS DECIMAL(10, 2)),
    CAST(sum(price_dj) + sum(PRICE_chao8) AS DECIMAL(10, 2)),
    REPLACE(curdate(), '-', '')
FROM t4
GROUP BY insure_company_channel, `YEAR_MONTH`, claim_type, is_back

"""


def truncate_table(table_name='CLAIM_DWD.DWD_TK_BILL_TOTAL'):
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
