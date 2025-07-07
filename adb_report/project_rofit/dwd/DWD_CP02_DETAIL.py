# import sys
# sys.path.append(r"E:\pycharm\database")
from database import DatabaseConnection
from datetime import datetime



sql_query = f"""
  -- @description: 太保宁波项目明细表
  -- @param:
  -- @return:
  -- @alter:
  -- @time: 2024-06-20 15:01:06
  -- @author: 01
  -- @version: 1.0.0
INSERT INTO CLAIM_DWD.DWD_CP02_DETAIL
  (
  INSURE_COMPANY_CHANNEL,
    DATE_MON,
  ACCEPT_NUM,
  claim_type,
  TREATMENT_TYPE,
  INVOICE_NUMBER,
  C_INVOICE_NUMBER,
  UNIT_PRICE,
  DATA_DT
  )
SELECT
    dim. channel_value,
    date_mon,
    accept_num,
    claim_type,
    就诊类型,
    发票数,
    CASE
   WHEN  就诊类型  = '门诊'
    AND  发票数- 8  >= 0  THEN  发票数- 8
   WHEN  就诊类型  = '住院'
    AND  发票数- 8  >= 0  THEN  发票数- 8
   ELSE
   0
   END  超发票数量,
     cast( CASE
   WHEN  insure_company_channel='苏州分公司' and   就诊类型  = '门诊' and claim_type='线上' THEN  11
   WHEN insure_company_channel='苏州分公司' and 就诊类型  = '住院' and claim_type='线上' THEN  24
  WHEN  insure_company_channel='苏州分公司' and 就诊类型  = '门诊' and  claim_type='线下' THEN   14
   WHEN insure_company_channel='苏州分公司' and 就诊类型  = '住院' and claim_type='线下' THEN    27
   WHEN insure_company_channel='上海分公司'  THEN    13
   WHEN insure_company_channel='CP02' and 就诊类型  = '住院'  THEN    27
    WHEN insure_company_channel='CP02' and 就诊类型  = '门诊'  THEN    14 else 0
   END  as decimal (10,2) ) 单价,
   replace(date_mon,'-','')

  FROM
    (
select INSURE_COMPANY_CHANNEL,
       ACCEPT_NUM,
       count(distinct bill_no) 发票数,
       就诊类型,
       claim_type,
       date_mon
from (SELECT
       case when  INSURE_COMPANY_CHANNEL='CP10' then '苏州分公司' else insure_company_channel end as INSURE_COMPANY_CHANNEL,
        case
             when aa. claim_source in ('1', '4') then
              '线下'
             WHEN aa. claim_source in ('2', '3') THEN
              '线上'
             else
              ''
           END as claim_type,
  substr(aa.back_time, 1, 10) AS date_mon,
  aa.ACCEPT_NUM,
  CASE
    WHEN EXISTS (
      SELECT 1
      FROM CLAIM_DWD.DWD_POSTBACK_TOTAL
      WHERE INSURE_COMPANY_CHANNEL  IN ('CP02','大连分公司','苏州分公司','上海分公司')
        AND ACCEPT_NUM = aa.ACCEPT_NUM
        AND treatment_date IS NULL
    ) THEN '住院'
    ELSE '门诊'
  END AS 就诊类型,
    bill_no
FROM
  CLAIM_DWD.DWD_POSTBACK_TOTAL aa
WHERE
  aa.INSURE_COMPANY_CHANNEL IN ('CP02','大连分公司','苏州分公司','上海分公司'))  ff group by INSURE_COMPANY_CHANNEL,ACCEPT_NUM
  )
  left JOIN  claim_ods.dim_insure_company_channel  dim
   on  insure_company_channel  = dim. channel_key
;
"""
def truncate_table(table_name='CLAIM_DWD.DWD_CP02_DETAIL'):
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
