# import sys
# sys.path.append(r"E:\pycharm\database")
from database import DatabaseConnection
from datetime import datetime



sql_query = f"""
  -- @description: 人工扣费原因分析表
  -- @param:
  -- @return:
  -- @alter:
  -- @time: 2024-10-12 15:01:06
  -- @author: 01
  -- @version: 1.0.0
INSERT INTO CLAIM_DWD.DWD_CHARG_REASON_ANALY (
    insure_company_channel,             -- 渠道
    comm_date,                          -- 时间
    layer,                              -- 层级
    reason_notes,                       -- 原因
    claim_vol,                          -- 案件数
    claim_no,                           -- 样例案件
    data_dt                             -- 调度日期
)
SELECT
    dim.CHANNEL_VALUE AS insure_company_channel,
    comm_date,
    layer,
    reason_notes,
    claim_vol,
    claim_no,
    REPLACE(comm_date, '-', '')
FROM (
    SELECT
        c.insure_company_channel,
        SUBSTR(c.create_time, 1, 10) AS comm_date,
        '账单层' AS layer,
        b.reason_notes AS reason_notes,
        COUNT(DISTINCT c.id) AS claim_vol,
        MAX(c.claim_no) AS claim_no
    FROM claim_ods.claim c
    JOIN claim_ods.bill b ON c.id = b.claim_id
    WHERE b.reason_notes IS NOT NULL
    GROUP BY c.insure_company_channel, SUBSTR(c.create_time, 1, 10), b.reason_notes
    UNION ALL
    SELECT
        c.insure_company_channel,
        SUBSTR(c.create_time, 1, 10) AS comm_date,
        '账单明细层' AS layer,
        d.reason_notes AS reason_notes,
        COUNT(DISTINCT c.id) AS claim_vol,
        MAX(c.claim_no) AS claim_no
    FROM claim_ods.claim c
    JOIN claim_ods.bill b ON c.id = b.claim_id
    JOIN claim_ods.bill_detail d ON b.id = d.bill_id
    WHERE d.reason_notes IS NOT NULL
    GROUP BY c.insure_company_channel, SUBSTR(c.create_time, 1, 10), d.reason_notes
)
INNER JOIN claim_ods.dim_insure_company_channel dim ON insure_company_channel = dim.CHANNEL_KEY;


"""
def truncate_table(table_name='CLAIM_DWD.DWD_CHARG_REASON_ANALY'):
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
