import pymysql
# import sys
#
# sys.path.append(r"E:\pycharm\database")
from database import DatabaseConnection
from datetime import datetime
from datetime import datetime, timedelta


sql_query = f"""
  -- @description: 泰康甘肃分公司账单汇总
  -- @param:
  -- @return:
  -- @alter:
  -- @time: 2024-06-20 15:01:06
  -- @author: 01
  -- @version: 1.0.0

    INSERT INTO  CLAIM_DWD.DWD_TK11_TOTAL
WITH tt AS (
    -- 第一个子查询，处理特定条件下的案件数据并进行分组统计

    SELECT DISTINCT
        pr.ACCEPT_NUM AS 受理编号,
        -- 取时间字段的前7位作为时间维度（可能是按月份等统计）
        SUBSTR(pr.back_time, 1, 7) AS 时间,
        -- 根据回传方式及相关条件判断具体的回传方式描述
        CASE
            WHEN pr.postback_way = 'H' THEN '半流程'
            WHEN pr.postback_way = 'W' THEN '全流程'
            ELSE '未知'
        END AS 回传方式,
        -- 使用 coalesce 函数确保 C_CVRG_DESC 不为 null，若为 null 则设置为 ''
        coalesce(group_concat(DISTINCT inv.C_CVRG_DESC), '') AS 险种描述,
        '结案' AS is_back
    FROM claim_ods.postback_record pr
    LEFT JOIN claim_ods.`clm_visit_inv_info` inv ON pr.app_no = inv.C_CUSTOM_APP_NO AND inv.C_DEL_FLAG = '0'
    LEFT JOIN claim_ods.`accept_list_record` alr ON pr.accept_num = alr.accept_num
    WHERE pr.INSURE_COMPANY_CHANNEL = 'TK11'
      AND pr.back_status IN ('2')
      AND pr.is_deleted = 'N'
    GROUP BY pr.ACCEPT_NUM

),
-- 使用公共表达式（CTE）创建临时结果集ff，基于tt进行进一步聚合和计算价格等操作
ff AS (
    SELECT
        时间,
        '泰康养老甘肃-半流程' AS INSURE_COMPANY_CHANNEL,
        险种描述,
        回传方式,
        is_back,
        受理编号,
        -- 根据不同的业务来源、回传方式和案件状态等条件确定价格
        CASE
            WHEN 险种描述 = '公共账户'  THEN 6.9
            WHEN 险种描述 = '个人账户' THEN 5.4
            WHEN 险种描述 LIKE '%个人账户%' AND 险种描述 LIKE '%公共账户%'  THEN 10.35
            ELSE 0
        END AS price
    FROM tt where 回传方式= '半流程'
)
-- 最终查询，从ff中选择相关字段，并进行类型转换和日期处理等操作后返回结果
SELECT
    INSURE_COMPANY_CHANNEL,
    时间,
    险种描述,
    回传方式,
    is_back,
    COUNT(1) AS 案件量,
    CAST(price AS DECIMAL(10, 2)) AS price,
    CAST(SUM(price)  AS DECIMAL(10, 2)),
    REPLACE(CURDATE(), '-', '')
FROM ff
GROUP BY INSURE_COMPANY_CHANNEL, 时间, 险种描述, 回传方式, is_back;


"""


def truncate_table(table_name='CLAIM_DWD.DWD_TK11_TOTAL'):
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