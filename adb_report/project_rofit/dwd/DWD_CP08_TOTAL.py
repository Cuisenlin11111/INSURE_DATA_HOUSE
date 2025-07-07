# import sys
# sys.path.append(r"E:\pycharm\database")
from database import DatabaseConnection
from datetime import datetime



sql_query = f"""
  -- @description: 太保健康项目汇总
  -- @param:
  -- @return:
  -- @alter:
  -- @time: 2024-06-20 15:01:06
  -- @author: 01
  -- @version: 1.0.0
  INSERT INTO CLAIM_DWD.DWD_CP08_TOTAL
  (INSURE_COMPANY_CHANNEL, DATE_MON, CLAIM_NUM, CLAIM_UNIT_PRICE, INVOICE_NUMBER, C_INVOICE_NUMBER, C_UNIT_PRICE, TOTAL, DATA_DT)
  SELECT
      INSURE_COMPANY_CHANNEL,
        date_mon,
        案件数,
        cast(案件单价 as decimal(10,2)),
        发票数,
        超发票数,
        超发票单价,
        cast(案件数*案件单价  as decimal(10,2)) AS total,
        replace(date_mon,'-','')
  FROM (
    SELECT
      INSURE_COMPANY_CHANNEL,
        date_mon,
        案件数,
        13 AS 案件单价,
        c.C_INVOICE_UNIT_PRICE 超发票单价,
        发票数,
        超发票数
     FROM(
        SELECT INSURE_COMPANY_CHANNEL,
            substr(date_mon,1,7) date_mon,
            count(claim_no) 案件数,
          sum(INVOICE_NUMBER) 发票数,
          sum(C_INVOICE_NUMBER) 超发票数
      FROM (
            SELECT * FROM CLAIM_DWD.DWD_CP08_DETAIL
        )
        GROUP BY INSURE_COMPANY_CHANNEL,
            substr(date_mon,1,7)

      )
      LEFT JOIN CLAIM_DIM.DIM_CHANNEL_PRICE c
      ON c.CHANNEL_VALUE=INSURE_COMPANY_CHANNEL
  );

"""
def truncate_table(table_name='CLAIM_DWD.DWD_CP08_TOTAL'):
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
