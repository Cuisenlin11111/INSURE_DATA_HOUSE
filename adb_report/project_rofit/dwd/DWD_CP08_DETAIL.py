# import sys
# sys.path.append(r"E:\pycharm\database")
from database import DatabaseConnection
from datetime import datetime



sql_query = f"""
  -- @description: 太保健康明细数据
  -- @param:
  -- @return:
  -- @alter:
  -- @time: 2024-06-20 15:01:06
  -- @author: 01
  -- @version: 1.0.0
 INSERT INTO CLAIM_DWD.DWD_CP08_DETAIL
  (INSURE_COMPANY_CHANNEL, DATE_MON, CLAIM_NO, INVOICE_NUMBER, C_INVOICE_NUMBER, UNIT_PRICE, DATA_DT)

    SELECT
          INSURE_COMPANY_CHANNEL,
          date_mon,
        accept_num,
        count(DISTINCT bill_no) 发票数,
        CASE
          WHEN count(DISTINCT bill_no)>5 THEN  count(DISTINCT bill_no)-5
          ELSE 0
        END 超发票数,
        c.PO_UNIT_PRICE,
        replace(date_mon,'-','')
      FROM
        (
           SELECT
            DISTINCT
            substr(back_time,1,10) date_mon ,
            '太保健康'  INSURE_COMPANY_CHANNEL,
            accept_num,
            bill_no
 from CLAIM_DWD.DWD_POSTBACK_TOTAL  where INSURE_COMPANY_CHANNEL='CP08'
       )
       LEFT JOIN CLAIM_DIM.DIM_CHANNEL_PRICE c
          ON c.CHANNEL_VALUE=INSURE_COMPANY_CHANNEL
      GROUP BY INSURE_COMPANY_CHANNEL,
          date_mon,
          accept_num,
          c.PO_UNIT_PRICE;
"""
def truncate_table(table_name='CLAIM_DWD.DWD_CP08_DETAIL'):
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
