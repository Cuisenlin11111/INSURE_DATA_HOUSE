# import sys
#
# sys.path.append(r"E:\pycharm\database")
from database import DatabaseConnection
from datetime import datetime

sql_query = r"""
  -- @description: 暖哇对账汇总
  -- @param:
  -- @return:
  -- @alter:
  -- @time: 2024-06-20 15:01:06
  -- @author: 01
  -- @version: 1.0.0
 


  insert into CLAIM_DWD.DWD_ZA01_BILL_TOTAL
    (INSURE_COMPANY_CHANNEL,
     DATE_MON,
     policy_type,
     CLAIM_NUM,
     INVOICE_NUMBER,
     price_dj,
     total,
     data_dt)
    with t1 as
     (SELECT substr(comm_date,1,7) 年月,
             accept_num,
             policy_type,
             case

               when policy_type = '团单' then
                12
               when policy_type = '个单' then
                25
               when policy_type = '好医保' or policy_type = '一日赔' then
                28
             end as 单价,
             case
               when policy_type = '团单' then
                '暖哇团险'
               when policy_type = '个单' then
                '暖哇个险'
               else
                policy_type
             end as 保单类型,
             BILL_VOL
        from claim_dwd.DWD_ZA01_BILL_DETAIL),
    t2 as     (select 年月,
                   保单类型,
                   单价,
                   count(distinct accept_num) 案件量,
                   sum(BILL_VOL) as bill_num
              from t1
             group by 年月, 保单类型, 单价)
        select
          '暖哇科技' as INSURE_COMPANY_CHANNEL,
           年月,
           保单类型,
           案件量,
           bill_num,
           cast(单价 as decimal(10,2)),
           cast( 单价*案件量 as decimal(10,2)) as 费用合计,
           replace(curdate(),'-','')
    from t2;

"""


def truncate_table(table_name='CLAIM_DWD.DWD_ZA01_BILL_TOTAL'):
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
