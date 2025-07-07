import pymysql
# import sys
#
# sys.path.append(r"E:\pycharm\database")
from database import DatabaseConnection
from datetime import datetime
from datetime import datetime, timedelta

current_date = datetime.now()
# 计算6个月前的日期
six_months_ago_date = current_date - timedelta(days=15)
six_months = six_months_ago_date.strftime('%Y-%m')

sql_query = f"""
  -- @description: 泰康江苏分公司账单汇总
  -- @param:
  -- @return:
  -- @alter:
  -- @time: 2024-06-20 15:01:06
  -- @author: 01
  -- @version: 1.0.0

INSERT INTO  CLAIM_DWD.DWD_TK09_TOTAL
WITH tt AS (
    -- 第一个子查询，处理特定条件下的案件数据并进行分组统计
    SELECT DISTINCT
        -- 根据claim_source的值转换为对应的业务来源描述（线下或线上）
        CASE
            WHEN TRIM(alr.claim_source) IN ('1', '3') THEN '线下'
            WHEN TRIM(alr.claim_source) IN ('2', '4') THEN '线上'
        END AS claim_source,
        pr.insurance_claim_no AS 受理编号,
        -- 取时间字段的前7位作为时间维度（可能是按月份等统计）
        SUBSTR(pr.back_time, 1, 7) AS 时间,
        -- 根据回传方式及相关条件判断具体的回传方式描述
        CASE
            WHEN pr.postback_way = 'H' THEN '半流程'
            WHEN pr.postback_way = 'W' THEN '全流程'
            ELSE '未知'
        END AS 回传方式,

        '结案' AS is_back
    FROM claim_ods.postback_record pr
    -- 左连接相关表，用于关联更多案件相关信息
    LEFT JOIN claim_ods.apply_claim ac ON pr.app_no = ac.apply_no AND ac.delete_flag = '0'
    LEFT JOIN claim_ods.claim_policy cp ON cp.policy_no = ac.policy_part_no
    LEFT JOIN claim_ods.claim_policy_customer cpc ON cpc.customer_no = cp.customer_no
    LEFT JOIN claim_ods.ply p ON cp.group_policy_no = p.C_PLY_NO
    LEFT JOIN claim_ods.`clm_visit_inv_info` inv ON pr.app_no = inv.C_CUSTOM_APP_NO AND inv.C_DEL_FLAG = '0'
    LEFT JOIN claim_ods.`accept_list_record` alr ON pr.accept_num = alr.accept_num
    LEFT JOIN claim_ods.claim c ON c.acceptance_no = alr.ACCEPT_NUM
    WHERE pr.INSURE_COMPANY_CHANNEL = 'TK09'
      AND pr.back_status IN ('2', '21')
      AND pr.is_deleted = 'N'
),
-- 使用公共表达式（CTE）创建临时结果集ff，基于tt进行进一步聚合和计算价格等操作
ff AS (
    SELECT
        时间,
        '泰康养老江苏分公司' AS INSURE_COMPANY_CHANNEL,
        回传方式,
        is_back,
        受理编号,
        -- 根据不同的业务来源、回传方式和案件状态等条件确定价格
        CASE
            WHEN 回传方式 = '全流程' AND claim_source = '线上' THEN 7
            WHEN 回传方式 = '全流程' AND claim_source = '线下' THEN 10
            ELSE 0
        END AS price
    FROM tt
)
-- 最终查询，从ff中选择相关字段，并进行类型转换和日期处理等操作后返回结果
SELECT
    INSURE_COMPANY_CHANNEL,
    时间,
    回传方式,
    is_back,
    COUNT(1) AS 案件量,
    CAST(price AS DECIMAL(10, 2)) AS price,
    CAST(SUM(price)  AS DECIMAL(10, 2)),
    REPLACE(CURDATE(), '-', '')
FROM ff
GROUP BY INSURE_COMPANY_CHANNEL, 时间, 回传方式, is_back;

"""


def truncate_table(table_name='CLAIM_DWD.DWD_TK09_TOTAL'):
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