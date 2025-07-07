# import sys
# sys.path.append(r"E:\pycharm\database")
from database import DatabaseConnection
from datetime import datetime, date, timedelta

# 计算昨天的日期
yesterday = date.today() - timedelta(days=1)
# 提取昨天所在的年月，格式为 YYYY-MM
yesterday_month = yesterday.strftime("%Y-%m")

sql_query = f"""
  -- @description: 项目录入成本月统计
  -- @param:
  -- @return:
  -- @alter:
  -- @time: 2024-06-20 15:01:06
  -- @author: 01
  -- @version: 1.0.0

  INSERT INTO CLAIM_DWS.DWS_INPUT_COST_M
  with   t1 as (# 智在录入
      select  '智在'  entry_merchant,
              insure_company_channel,
              `YEAR_MONTH`,
              coalesce(MZ_ONLINE_AJ,0) + coalesce(ZY_ONLINE_AJ,0) 录入案件量,
              PRICE_COUNT 录入成本,
             case
                     when coalesce(MZ_ONLINE_AJ,0) + coalesce(ZY_ONLINE_AJ,0) = 0 then
                      0
                     else
                      PRICE_COUNT / (coalesce(MZ_ONLINE_AJ,0) + coalesce(ZY_ONLINE_AJ,0))
                   end as 录入均价
        from CLAIM_DWD.DWD_ZZ_INPUT_FEE_COUNT
        WHERE `YEAR_MONTH` = '{yesterday_month}'
  ),
             t2 as (##广纳录入
      select  '广纳'  entry_merchant,
              insure_company_channel,
              `YEAR_MONTH`,
              coalesce(MZ_ONLINE_AJ,0) + coalesce(ZY_ONLINE_AJ,0)  录入案件量,
              PRICE_COUNT 录入成本,
              case
                     when coalesce(MZ_ONLINE_AJ,0) + coalesce(ZY_ONLINE_AJ,0) = 0 then
                      0
                     else
                      PRICE_COUNT / (coalesce(MZ_ONLINE_AJ,0) + coalesce(ZY_ONLINE_AJ,0))
                   end as 录入均价
        from CLAIM_DWD.DWD_GN_INPUT_FEE_COUNT
        WHERE `YEAR_MONTH` = '{yesterday_month}'
  ),
             t3 as (##成都视觉录入
      select  '成都视觉'  entry_merchant,
              insure_company_channel,
              `YEAR_MONTH`,
              coalesce(MZ_ONLINE_AJ,0) + coalesce(ZY_ONLINE_AJ,0)  录入案件量,
              PRICE_COUNT 录入成本,
              case
                     when coalesce(MZ_ONLINE_AJ,0) + coalesce(ZY_ONLINE_AJ,0) = 0 then
                      0
                     else
                      PRICE_COUNT / (coalesce(MZ_ONLINE_AJ,0) + coalesce(ZY_ONLINE_AJ,0))
                   end as 录入均价
        from CLAIM_DWD.DWD_CDSJ_INPUT_FEE_COUNT
        WHERE `YEAR_MONTH` = '{yesterday_month}'
  ),
             t4 as (   ##施博录入
                                                      select   '施博'  entry_merchant,
              insure_company_channel,
              `YEAR_MONTH`,
              sum(coalesce(hospital_claim_count,0) + coalesce(mz_online_claim_count,0) +
              coalesce(mz_offline_claim_count,0)) 录入案件量,
              sum(total_fee) 录入成本,
                            case
                     when sum(coalesce(hospital_claim_count,0) + coalesce(mz_online_claim_count,0) +
              coalesce(mz_offline_claim_count,0)) = 0 then
                      0
                     else
                      sum(total_fee) / sum(coalesce(hospital_claim_count,0) + coalesce(mz_online_claim_count,0) +
              coalesce(mz_offline_claim_count,0))
                   end as 录入均价
        from CLAIM_DWD.DWD_SH_INPUT_FEE_COUNT
        WHERE `YEAR_MONTH` = '{yesterday_month}'
          group by  insure_company_channel
  ),
      t4aa as (select '因朔桔' entry_merchant,
      channel insure_company_channel,
       substr(aa.data_dt,1,7)  `YEAR_MONTH`,
       bb.录入案件量 as 录入案件量,
       sum(ocr_cost) + sum(hsbx_n_cost) + sum(hsbx_y_cost) + sum(inp_cost) + sum(rev_cost) + sum(wsy_cost) as 录入成本  ,
       (sum(ocr_cost) + sum(hsbx_n_cost) + sum(hsbx_y_cost) + sum(inp_cost) + sum(rev_cost) + sum(wsy_cost))/bb.录入案件量 as 录入均价
from CLAIM_DWD.DWD_YSJ_INPUT_FEE_COUNT  aa
left join claim_ods.dim_insure_company_channel dim on aa.channel=dim.channel_value
left join   (
       select
           INSURE_COMPANY_CHANNEL,
           substr(back_time,1,7) dt ,
           count(distinct  ACCEPT_NUM) as 录入案件量
           from    CLAIM_DWD.DWD_POSTBACK_INPUT_INFO 
           WHERE substr(back_time,1,7) = '{yesterday_month}'  AND  input_company='因朔桔'
           group by  INSURE_COMPANY_CHANNEL,substr(back_time,1,7)
           )  bb on bb.INSURE_COMPANY_CHANNEL=dim.channel_key and substr(aa.data_dt,1,7)=substr(bb.dt,1,10)
WHERE substr(aa.data_dt,1,7) = '{yesterday_month}'
group by channel, substr(aa.data_dt,1,7)
  ),
             t5 as ( select
    '合计' entry_merchant,
    insure_company_channel,
    `YEAR_MONTH`,
    sum(录入案件量) 录入案件量,
    sum(录入成本) 录入成本,
     case when sum(录入案件量)=0 then 0 else sum(录入成本)/sum(录入案件量) end as 录入均价
from (
select *
from t1
union all
select *
from t2
union all
select *
from t3
union all
select *
from t4
union all
select * from t4aa
) ff   group by    insure_company_channel,
                           `YEAR_MONTH`  
  )
select  entry_merchant,
        case when insure_company_channel='泰康养老甘肃分公司' then '泰康养老甘肃-半流程' else insure_company_channel end  insure_company_channel,
        `YEAR_MONTH`,
         coalesce(录入案件量,0) 录入案件量,
        cast(coalesce(录入成本,0) as decimal(10,2)) 录入成本,
        cast(coalesce(录入均价,0) as decimal(10,2)) 录入均价,
       replace(current_date,'-','')
from t1
union  all
select  entry_merchant,
        case when insure_company_channel='泰康养老甘肃分公司' then '泰康养老甘肃-半流程' else insure_company_channel end  insure_company_channel,
        `YEAR_MONTH`,
         coalesce(录入案件量,0) 录入案件量,
        cast(coalesce(录入成本,0) as decimal(10,2)) 录入成本,
        cast(coalesce(录入均价,0)  as decimal(10,2)) 录入均价,
       replace(current_date,'-','')
from t2
  union  all
select  entry_merchant,
        case when insure_company_channel='泰康养老甘肃分公司' then '泰康养老甘肃-半流程' else insure_company_channel end  insure_company_channel,
        `YEAR_MONTH`,
         coalesce(录入案件量,0) 录入案件量,
        cast(coalesce(录入成本,0) as decimal(10,2)) 录入成本,
        cast(coalesce(录入均价,0)  as decimal(10,2)) 录入均价,
       replace(current_date,'-','')
from t3
  union  all
select  entry_merchant,
        case when insure_company_channel='泰康养老甘肃分公司' then '泰康养老甘肃-半流程' else insure_company_channel end  insure_company_channel,
        `YEAR_MONTH`,
         coalesce(录入案件量,0) 录入案件量,
        cast(coalesce(录入成本,0) as decimal(10,2)) 录入成本,
        cast(coalesce(录入均价,0)  as decimal(10,2)) 录入均价,
       replace(current_date,'-','')
from t4
  union  all
select  entry_merchant,
        case when insure_company_channel='泰康养老甘肃分公司' then '泰康养老甘肃-半流程' else insure_company_channel end  insure_company_channel,
        `YEAR_MONTH`,
         coalesce(录入案件量,0) 录入案件量,
        cast(coalesce(录入成本,0) as decimal(10,2)) 录入成本,
        cast(coalesce(录入均价,0)  as decimal(10,2)) 录入均价,
       replace(current_date,'-','')
from t5
    union  all
select  entry_merchant,
        case when insure_company_channel='泰康养老甘肃分公司' then '泰康养老甘肃-半流程' else insure_company_channel end  insure_company_channel,
        `YEAR_MONTH`,
         coalesce(录入案件量,0) 录入案件量,
        cast(coalesce(录入成本,0) as decimal(10,2)) 录入成本,
        cast(coalesce(录入均价,0)  as decimal(10,2)) 录入均价,
       replace(current_date,'-','')
from t4aa
  ;
"""

def truncate_table(table_name='CLAIM_DWS.DWS_INPUT_COST_M'):
    with DatabaseConnection() as conn:
        # 删除昨天对应月的数据
        truncate_sql = f"DELETE FROM {table_name} WHERE DT_MONTH = '{yesterday_month}'"
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