# import sys
# sys.path.append(r"E:\pycharm\database")
from database import DatabaseConnection
from datetime import datetime



sql_query = f"""
 -- @description: 大家养老汇总
  -- @param:
  -- @return:
  -- @alter:
  -- @time: 2024-06-20 15:01:06
  -- @author: 01
  -- @version: 1.0.0
  insert into CLAIM_DWD.DWD_DJ01_TOTAL
    (
     INSURE_COMPANY_CHANNEL,
     DATE_MON,
     TREATMENT_TYPE,
     CLAIM_NUM,
     INVOICE_NUMBER,
     UNIT_PRICE,
     TOTAL,
     DATA_DT)
     SELECT
    '大家养老',
  date_mon,
  treatment_type,
  sum(caseload) AS caseload,
  sum(invoice_number) AS invoice_number,
  cast(avg(unit_price) as decimal(10,2)) AS unit_price,
  cast(sum(total) as decimal(10,2)) AS total,
  replace(date_mon,'-','')
FROM
  (
  SELECT
    date_mon,
    ACCEPT_NUM,
    treatment_type,
    invoice_number,
    caseload
  ,
    CASE
      WHEN treatment_type = '超10张门诊案件' THEN 1.48
      WHEN treatment_type = '门诊' THEN 14.8
      WHEN treatment_type = '住院' THEN 24
    END unit_price
   ,
    CASE
      WHEN treatment_type = '超10张门诊案件' THEN invoice_number * 1.48
      WHEN treatment_type = '门诊' THEN caseload * 14.8
      WHEN treatment_type = '住院' THEN invoice_number * 24
    END total
  FROM
    (
    SELECT
      date_mon,
      ACCEPT_NUM,
      CASE
        WHEN 就诊类型 = '门诊'
        AND COUNT(就诊类型)-10 >= 0 THEN '超10张门诊案件'
        WHEN 就诊类型 = '门诊' THEN '门诊'
        WHEN 就诊类型 = '住院' THEN '住院'
      END treatment_type
    ,
      COUNT(就诊类型) invoice_number
    ,
      count(DISTINCT ACCEPT_NUM) caseload
    FROM
      (
            SELECT
  substr(aa.back_time, 1, 7) AS date_mon,
  aa.ACCEPT_NUM,
  CASE
    WHEN EXISTS (
      SELECT 1
      FROM CLAIM_DWD.DWD_POSTBACK_TOTAL
      WHERE INSURE_COMPANY_CHANNEL = 'DJ01'
        AND ACCEPT_NUM = aa.ACCEPT_NUM
        AND treatment_date IS NULL
    ) THEN '住院'
    ELSE '门诊'
  END AS 就诊类型
FROM
  CLAIM_DWD.DWD_POSTBACK_TOTAL aa
WHERE
  aa.INSURE_COMPANY_CHANNEL = 'DJ01'
    )
    GROUP BY
      date_mon,
      ACCEPT_NUM,
      就诊类型
  )
)
GROUP BY
  date_mon,
  treatment_type
ORDER BY
  date_mon DESC;
"""
def truncate_table(table_name='CLAIM_DWD.DWD_DJ01_TOTAL'):
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
