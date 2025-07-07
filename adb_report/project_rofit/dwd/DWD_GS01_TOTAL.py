# import sys
# sys.path.append(r"E:\pycharm\database")
from database import DatabaseConnection
from datetime import datetime

sql_query = f"""
  -- @description: 国寿财 项目汇总表
  -- @param:
  -- @return:
  -- @alter:
  -- @time: 2024-10-09 
  -- @author: 01
  -- @version: 1.0.0
INSERT INTO CLAIM_DWD.DWD_GS01_TOTAL (
    INSURE_COMPANY_CHANNEL, -- 渠道名称：'中国人寿财产保险'
    DATE_MON,               -- 结算月
    CLAIM_NUM,              -- 案件数
    CLAIM_TYPE,             -- 案件类型
    INVOICE_NUMBER,         -- 发票数
    UNIT_PRICE,             -- 单价（固定为 24）
    TOTAL,                  -- 合计金额
    DATA_DT                  -- 调度日期
)
SELECT
    '中国人寿财产保险' AS insure_company_channel,
    SUBSTR(pr.back_time, 1, 7) AS DATE_MON,
    COUNT(DISTINCT pr.app_no) AS CLAIM_NUM,
    '' AS CLAIM_TYPE,
    COUNT(DISTINCT b.bill_no) AS INVOICE_NUMBER,
    68 AS UNIT_PRICE,
    COUNT(DISTINCT pr.app_no) * 68 AS TOTAL,
    REPLACE(CURDATE(), '-', '') AS DATA_DT
FROM claim_ods.postback_record pr
         LEFT JOIN claim_ods.claim c ON pr.app_no = c.claim_no
    AND c.insure_company_channel = 'GS01'
    AND c.delete_flag = '0'
         LEFT JOIN claim_ods.bill b ON b.claim_id = c.id
    AND b.insure_company_channel = 'GS01'
WHERE pr.insure_company_channel = 'GS01'
  AND SUBSTR(pr.back_time, 1, 10) >= '2024-10-01'
  AND pr.is_deleted = 'N'
 and  pr.back_status in ('2', '21')
GROUP BY SUBSTR(pr.back_time, 1, 7);

"""


def truncate_table(table_name='CLAIM_DWD.DWD_GS01_TOTAL'):
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
