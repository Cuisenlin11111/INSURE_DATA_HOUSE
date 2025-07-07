import sys
sys.path.append(r"D:\因朔桔智能科技有限公司\pycharm\pycharm")
from database import DatabaseConnection
from datetime import datetime



sql_query = f"""
  -- @description: 渤海人寿汇总数据
  -- @param:
  -- @return:
  -- @alter:
  -- @time: 2024-06-20 15:01:06
  -- @author: 01
  -- @version: 1.0.0
INSERT INTO CLAIM_DWD.DWD_BH01_TOTAL
  (
  INSURE_COMPANY_CHANNEL,
   DATE_MON,
  CLAIM_NUM,
  C_INVOICE_NUMBER,
  CASE_UNIT_PRICE,
  BILL_UNIT_PRICE,
  TOTAL,
  DATA_DT)
  SELECT
  渠道,
  date_mon,
  案件数,
  超发票数量,
  cast(案件单价 as decimal(10,2)) AS 案件单价,
  cast(发票单价 as decimal(10,2)) AS 发票单价,
  cast(案件数 * 案件单价 + 超发票数量 * 发票单价 as decimal(10,2)) AS 合计金额
  ,replace(curdate(),'-','')
FROM
  (
  SELECT
    渠道,
    date_mon,
    count(distinct  ACCEPT_NUM) 案件数,
    sum(超发票数量) 超发票数量,
    15.6 案件单价,
    1.95 发票单价

  FROM
    (
    SELECT
      substr(date_mon,1,7) date_mon,
      渠道,
      ACCEPT_NUM,
      发票数,
      CASE
        WHEN  发票数-8 >= 0 THEN 发票数-8
        ELSE
        0
      END 超发票数量
    FROM
      (
      select substr(BACK_TIME, 1,7) date_mon ,
         '渤海人寿'  渠道,
          ACCEPT_NUM,
          count(distinct  bill_no) 发票数
from CLAIM_DWD.DWD_POSTBACK_TOTAL where INSURE_COMPANY_CHANNEL = 'BH01'
group by ACCEPT_NUM
    )
)
  GROUP BY
    date_mon,
    渠道)

"""
def truncate_table(table_name='CLAIM_DWD.DWD_BH01_TOTAL'):
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
