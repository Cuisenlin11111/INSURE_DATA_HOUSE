# import sys
# sys.path.append(r"E:\pycharm\database")
from database import DatabaseConnection
from datetime import datetime



sql_query = f"""
  -- @description: 总发票数
  -- @param:
  -- @return:
  -- @alter:
  -- @time: 2024-08-29 15:01:06
  -- @author: 01
  -- @version: 1.0.0


INSERT INTO CLAIM_DWD.DWD_BILL_PANEL_1
(
    insure_company_channel,
    create_time,
    total_bill_count,
    data_dt
)
SELECT
    dim.channel_value AS insure_company_channel,
    aa.create_time,
    COALESCE(aa.total_bill_count, 0) AS total_bill_count,
    aa.data_dt
FROM
(
    SELECT
        alr.department_code AS insure_company_channel,
        SUBSTR(c.create_time, 1, 10) AS create_time,
        COUNT(DISTINCT b.id) AS total_bill_count,
        REPLACE(SUBSTR(c.create_time, 1, 10), '-', '') AS data_dt
    FROM
        claim_ods.claim c
    INNER JOIN
        claim_ods.bill b ON b.claim_id = c.id AND b.delete_flag = '0'
    LEFT JOIN
            claim_ods.accept_list_record alr on c.acceptance_no=alr.ACCEPT_NUM
    WHERE
        c.delete_flag = '0'
     and c.insure_company_channel='CP10' AND alr.department_code  in ('大连分公司','苏州分公司')
    GROUP BY
        alr.department_code,
        SUBSTR(c.create_time, 1, 10)
) aa
LEFT JOIN
    claim_ods.dim_insure_company_channel dim ON aa.insure_company_channel = dim.channel_key;

"""
def truncate_table(table_name='CLAIM_DWD.DWD_BILL_PANEL_1'):
    with DatabaseConnection() as conn:
        truncate_sql = f"delete from  {table_name}   where INSURE_COMPANY_CHANNEL in ('太保产险大连分公司','太保产险苏州分公司')  "
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
