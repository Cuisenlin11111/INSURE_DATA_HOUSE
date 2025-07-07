# import sys
# sys.path.append(r"E:\pycharm\database")
from database import DatabaseConnection
from datetime import datetime

sql_query = f"""
  -- @description: 平安产险项目汇总表
  -- @param:
  -- @return:
  -- @alter:
  -- @time: 2024-06-20 15:01:06
  -- @author: 01
  -- @version: 1.0.0
 insert into claim_dwd.DWD_PA02_TOTAL
    (department_code,
     year_month,
     chanel_type,
     half_flow_hc,
     half_flow_price,
     half_flow_fee_sum,
     all_flow_hc,
     all_flow_price,
     all_flow_fee_sum,
     fee_total,
     data_dt)
    with t1 as
     (select department_code,
             chanel_type,
             ym_date as `YEAR_MONTH`,
             sum(all_flow_hc) all_flow_hc,
             sum(half_flow_hc) half_flow_hc
        from (             select department_code,
                     chanel_type,
                     ym_date,
                     0 as all_flow_hc,
                     sum(half_flow_hc) half_flow_hc
              from (select a.department_code,
             CASE
                 WHEN a.product_type = 'Z' OR a.product_type = 'C' THEN '雇主_车险'
                 WHEN a.product_type = 'P' OR a.product_type = 'G' THEN '意健险'
                 ELSE ''
             END AS chanel_type,
             substr(a.back_time,1,7) ym_date,
             count(distinct a.app_no) as half_flow_hc
        from CLAIM_DWD.DWD_POSTBACK_TOTAL a
        where
          a.postback_way = 'H'
       and a.insure_company_channel = 'PA02'
       group by a.department_code,
                substr(a.back_time,1,7),
                CASE
                    WHEN a.product_type = 'Z' OR a.product_type = 'C' THEN '雇主_车险'
                    WHEN a.product_type = 'P' OR a.product_type = 'G' THEN '意健险'
                    ELSE ''
                END)
group by department_code,chanel_type, ym_date

union all
             select department_code,
                    '意健险' chanel_type,
                     ym_date,
                     sum(all_flow_hc) all_flow_hc,
                     0 as half_flow_hc
                from (select a.department_code,
                             '意健险' chanel_type,
                             substr(a.back_time, 1,7) ym_date,
                             count(distinct a.app_no) as all_flow_hc
                        from CLAIM_DWD.DWD_POSTBACK_TOTAL a
                       where  a.insure_company_channel = 'PA02'
                         and a.postback_way = 'W'
                       group by a.department_code,
                                substr(a.back_time,1,7))
               group by department_code, ym_date)
       group by department_code,chanel_type, ym_date)
    select department_code,
           `YEAR_MONTH`,
           chanel_type,
           half_flow_hc,
           cast(half_flow_price as decimal(10,2)),
           cast(half_flow_fee_sum as decimal(10,2)),
           all_flow_hc,
           cast(all_flow_price as decimal(10,2)),
           cast(all_flow_fee_sum as decimal(10,2)),
           cast((half_flow_fee_sum  + all_flow_fee_sum ) as decimal(10,2)) fee_total,
           replace(curdate(),'-','')
      from (select department_code,
                   `YEAR_MONTH`,
                   chanel_type,
                   half_flow_hc,
                   half_flow_price,
                   half_flow_hc * half_flow_price AS half_flow_fee_sum,
                   all_flow_hc,
                   全流程单价 all_flow_price,
                   all_flow_hc * 全流程单价 all_flow_fee_sum
              from (             select      department_code,
                           `YEAR_MONTH`,
                           chanel_type,
                           coalesce(half_flow_hc, 0) half_flow_hc,
                           case
                             when coalesce(all_flow_hc, 0) between 0 and 1000 then
                              8
                             when coalesce(all_flow_hc, 0) between 1001 and 3000 then
                              7.5
                             when coalesce(all_flow_hc, 0) between 3001 and 5000 then
                              7
                             when coalesce(all_flow_hc, 0) > 5000 then
                              6.5
                             else
                              0
                           end as half_flow_price,
                           0 as 全流程单价,
                           coalesce(all_flow_hc, 0) all_flow_hc
                      from t1   where   chanel_type='雇主_车险'
             union all
             select department_code,
                           `YEAR_MONTH`,
                           chanel_type,
                           coalesce(half_flow_hc, 0) half_flow_hc,
                           case
                             when coalesce(all_flow_hc, 0)   between 0 and 1000 and  department_code='上海分公司'   then
                              8.5
                             when coalesce(all_flow_hc, 0) between 1001 and 3000   and  department_code='上海分公司'  then
                              8
                             when coalesce(all_flow_hc, 0) between 3001 and 5000 and  department_code='上海分公司' then
                              7.5
                             when coalesce(all_flow_hc, 0) > 5000 and  department_code='上海分公司'  then
                              7
                               when coalesce(all_flow_hc, 0) between 0 and 1000 and  department_code<>'上海分公司'   then
                              9
                             when coalesce(all_flow_hc, 0) between 1001 and 3000  and  department_code<>'上海分公司'  then
                              8.5
                             when coalesce(all_flow_hc, 0) between 3001 and 5000 and  department_code<>'上海分公司'  then
                              8
                             when coalesce(all_flow_hc, 0) > 5000 and  department_code<>'上海分公司'  then
                              7.5
                             else
                              0
                           end as half_flow_price,
                           case
                             when coalesce(all_flow_hc, 0) between 0 and 1000 then
                              18.5
                             when coalesce(all_flow_hc, 0) between 1001 and 3000 then
                              18
                             when coalesce(all_flow_hc, 0) between 3001 and 5000 then
                              17.5
                             when coalesce(all_flow_hc, 0) > 5000 then
                              17
                             else
                              0
                           end as 全流程单价,
                           coalesce(all_flow_hc, 0) all_flow_hc
                      from t1   where   chanel_type='意健险'));

"""


def truncate_table(table_name='CLAIM_DWD.DWD_PA02_TOTAL'):
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
