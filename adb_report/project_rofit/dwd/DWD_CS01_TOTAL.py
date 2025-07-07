import pymysql
# import sys
#
# sys.path.append(r"E:\pycharm\database")
from database import DatabaseConnection
from datetime import datetime
from datetime import datetime, timedelta


sql_query = f"""
  -- @description: 长生人寿 账单汇总
  -- @param:
  -- @return:
  -- @alter:
  -- @time: 2024-06-20 15:01:06
  -- @author: 01
  -- @version: 1.0.0

INSERT INTO  CLAIM_DWD.DWD_CS01_TOTAL
WITH tt AS (
    -- 第一个子查询，处理特定条件下的案件数据并进行分组统计
    SELECT DISTINCT
        pr.ACCEPT_NUM AS 受理编号,
        -- 取时间字段的前7位作为时间维度（可能是按月份等统计）
        SUBSTR(pr.back_time, 1, 7) AS 时间
    FROM claim_ods.postback_record pr
    LEFT JOIN claim_ods.`accept_list_record` alr ON pr.accept_num = alr.accept_num
    WHERE pr.INSURE_COMPANY_CHANNEL = 'CS01'
      AND pr.back_status IN ('2', '21')
      AND pr.is_deleted = 'N'
    AND  pr.task_type=0
    GROUP BY alr.ACCEPT_NUM
)
-- 最终查询，从ff中选择相关字段，并进行类型转换和日期处理等操作后返回结果
SELECT
    '长生人寿' INSURE_COMPANY_CHANNEL,
    时间,
    COUNT(1) AS 案件量,
    CAST(9.15 AS DECIMAL(10, 2)) AS price,
    CAST(COUNT(*)*9.15   AS DECIMAL(10, 2)),
    REPLACE(CURDATE(), '-', '')
FROM tt
GROUP BY  时间

"""


def truncate_table(table_name='CLAIM_DWD.DWD_CS01_TOTAL'):
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