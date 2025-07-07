# import sys
# sys.path.append(r"E:\pycharm\database")
from database import DatabaseConnection
from datetime import datetime



sql_query = f"""
  -- @description: 太保宁波项目汇总
  -- @param:
  -- @return:
  -- @alter:
  -- @time: 2024-06-20 15:01:06
  -- @author: 01
  -- @version: 1.0.0
  INSERT INTO CLAIM_DWD.DWD_CP02_TOTAL
  (
  INSURE_COMPANY_CHANNEL,
  DATE_MON,
  TREATMENT_TYPE,
  CLAIM_NUM,
  INVOICE_NUMBER,
  C_INVOICE_NUMBER,
  TOTAL,
  DATA_DT
  )
  SELECT
      渠道,
    date_mon,
    就诊类型,
    count(accept_num) 案件数,
    sum(INVOICE_NUMBER) 发票数,
    sum(超发票数量) 超发票数量,
    cast(sum(单价) as decimal (10,2)) fee,
    replace(curdate(),'-','')
  FROM
    (

    SELECT
      substr(date_mon,1,7) date_mon,
      INSURE_COMPANY_CHANNEL 渠道,
      ACCEPT_NUM,
      TREATMENT_TYPE 就诊类型,
      UNIT_PRICE 单价,
      INVOICE_NUMBER,
      C_INVOICE_NUMBER 超发票数量
    FROM CLAIM_DWD.DWD_CP02_DETAIL
#     union  all
#     select
#       substr(date_mon,1,7) date_mon,
#       '太保产险上海分公司' 渠道,
#       ACCEPT_NUM,
#       TREATMENT_TYPE 就诊类型,
#       UNIT_PRICE 单价,
#       INVOICE_NUMBER,
#       C_INVOICE_NUMBER 超发票数量
# from CLAIM_DWD.DWD_CP07_DETAIL
  )
GROUP BY
  date_mon,
  渠道,
  就诊类型;

"""
def truncate_table(table_name='CLAIM_DWD.DWD_CP02_TOTAL'):
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
