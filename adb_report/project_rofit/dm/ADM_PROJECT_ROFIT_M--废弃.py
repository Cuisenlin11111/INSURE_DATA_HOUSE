# import sys
# sys.path.append(r"D:\因朔桔智能科技有限公司\pycharm\pycharm")
from database import DatabaseConnection
from datetime import datetime, date, timedelta

# 计算昨天的日期
yesterday = date.today() - timedelta(days=1)
# 提取昨天所在的年月，格式为 YYYY-MM
yesterday_month = yesterday.strftime("%Y-%m")



sql_query = f"""
  -- @description: 项目利润月统计-更新版
  -- @param:
  -- @return:
  -- @alter:
  -- @time: 2024-06-20 15:01:06
  -- @author: 01
  -- @version: 1.0.0
INSERT INTO CLAIM_DM.ADM_PROJECT_ROFIT_M
  (    insurance_company_channel ,
    DT_MONTH ,
   DRHCAL,
    CLAIM_NUM ,
    Case_Income ,
    Tax_Exempt_Income ,
    Marketing_Cost ,
    Gross_Profit ,
    Gross_Profit_Ratio ,
    Gross_Profit_2 ,
    Gross_Profit_Ratio_2 ,
    Sim_Profit ,
    Sim_Profit_Ratio ,
    Net_Profit ,
    Net_Profit_Ratio ,
    Entry_Cost_Ratio ,
    Entry_Cost_Case ,
    Technical_Cost_Case ,
    Operation_Cost_Case ,
    Total_Cost_Case ,
    Personnel_Capacity_Psn,
    Personnel_Capacity ,
    Case_Avg_Price ,
    Case_Avg_Price_1 ,
    SBLR_RATE ,
    SBLR_AVG_Price ,
    SBLR_NUM ,
    SHLR_COST ,
    CDSJLR_RATE ,
    CDSJLR_AVG_Price ,
    CDSJLR_NUM ,
    CDSJLR_COST ,
    GNLR_RATE ,
    GNLR_AVG_Price ,
    GNLR_NUM ,
    GNLR_COST ,
    ZZSJLR_RATE ,
    ZZSJLR_AVG_Price ,
    ZZSJLR_NUM ,
    ZZSJLR_COST ,
    dt )
    with t1 as (SELECT insurance_company_channel AS 大渠道,
       DT_MONTH AS 年月,
       DRHCAL as 回传案件量,
       CLAIM_NUM 案件量,
       claim_income  AS 案件收入,
       claim_income/1.06  AS 除税收入,
       case when insurance_company_channel='中智'  then 0
         when    insurance_company_channel in ('泰康养老广东分公司',
'泰康养老山东分公司',
'泰康养老上海分公司',
'泰康养老河南分公司',
'泰康养老北京分公司','泰康养老江苏分公司','泰康养老辽宁分公司','泰康养老福建分公司')   then  claim_income*0.08
    else claim_income*0.05 end  AS 营销成本
    FROM CLAIM_DWS.DWS_CLAIM_INCOME_M where insurance_company_channel<>'中国人民财产保险'),
    t2 as  (select entry_merchant,
       insurance_company_channel AS 大渠道,
       DT_MONTH AS 年月,
       IN_CLAIM_NUM 录入案件量,
       expense 录入成本,
       avg_expense 录入均价
from   CLAIM_DWS.DWS_INPUT_COST_M ),
    t3 as  (select DT_MONTH AS 年月,
       CHANNEL_VALUE  AS 大渠道,
       CLAIM_OPERATE_VOL + CONFIG_VOL + CLAIM_STANDARD_VOL + CLAIM_AUDIT_VOL + CUSTOMER_VOL   as 项目作业人数,
       CLAIM_OPERATE_VOL*CLAIM_OPERATE_MONEY + CONFIG_VOL*CONFIG_MONEY + CLAIM_STANDARD_VOL*CLAIM_STANDARD_MONEY + CLAIM_AUDIT_VOL*CLAIM_AUDIT_MONEY + CUSTOMER_VOL*CUSTOMER_MONEY    as 项目作业人力成本,
       TECH_VOL*TECH_MONEY 技术人力成本
from CLAIM_DIM.DIM_MANPOWER_CONFIG)
     select
       t1.大渠道,
       t1.年月,
       t1.回传案件量 ,
       t1.案件量,
       CAST(t1.案件收入 AS decimal(10,2)),
       cast(t1.除税收入 as decimal(10,2)),
       cast(t1.营销成本 as decimal(10,2)) ,
       cast(t1.除税收入-coalesce(t2.录入成本,0)-t1.营销成本 as decimal(10,2)) 毛利润,
       cast((t1.除税收入-coalesce(t2.录入成本,0)-t1.营销成本)/t1.除税收入 as decimal(10,4))  毛利润率,
       cast(t1.除税收入-coalesce(t2.录入成本,0)-t1.营销成本-t3.项目作业人力成本 as decimal(10,2)) 毛利润2,
       cast((t1.除税收入-coalesce(t2.录入成本,0)-t1.营销成本-t3.项目作业人力成本)/t1.除税收入  as decimal(10,4)) 毛利润率2,
       cast(t1.除税收入 - coalesce(t2.录入成本,0) - t1.营销成本 - t3.项目作业人力成本 - t3.技术人力成本 as decimal(10,2))  单纯净利润,
       cast((t1.除税收入 - coalesce(t2.录入成本,0) - t1.营销成本 - t3.项目作业人力成本 - t3.技术人力成本)/t1.除税收入  as decimal(10,4)) 单纯净利润率,
       cast(t1.除税收入 - coalesce(t2.录入成本,0) - t1.营销成本 - t3.项目作业人力成本 - t3.技术人力成本 -t1.案件量*t4.HLP_MANAGE_CLAIM - t1.案件量*t4.YSJ_MANAGE_CLAIM  as decimal(10,2)) 净利润,
       cast((t1.除税收入 - coalesce(t2.录入成本,0) - t1.营销成本 - t3.项目作业人力成本 - t3.技术人力成本 -t1.案件量*t4.HLP_MANAGE_CLAIM - t1.案件量*t4.YSJ_MANAGE_CLAIM )/t1.除税收入 as decimal(10,4))  净利润率,
       cast(coalesce(t2.录入成本,0)/t1.案件收入 as decimal(10,4))  外包录入占收入比,
       cast(coalesce(t2.录入成本,0)/t1.案件量 as decimal(10,2)) 外包录入成本案件,
       cast(t3.技术人力成本/t1.案件量 as decimal(10,2)) 项目技术成本案件,
       cast(t3.项目作业人力成本/t1.案件量  as decimal(10,2))   项目作业成本案件,
       cast((t3.项目作业人力成本 + t3.技术人力成本 +coalesce(t2.录入成本,0)  ) /t1.案件量 + t4.HLP_MANAGE_CLAIM + t4.YSJ_MANAGE_CLAIM +coalesce(t2.录入成本,0)/t1.案件量+t1.营销成本/t1.案件量  as decimal(10,2))  总成本案件,
       cast(t3.项目作业人数  as decimal(10,2)) 项目作业人数,
        cast(case when t3.项目作业人数>0 then  t1.案件量/t3.项目作业人数 else 0 end as decimal(10,2))  项目作业人均产能,
       cast(t1.案件收入/t1.案件量 as decimal(10,2)) 案件均价,
       cast(case when  coalesce(t1.回传案件量,0)=0 then 0 else    t1.案件收入/t1.回传案件量 end  as decimal(10,2)) 案件均价不含撤案,
       cast(coalesce(t5.avg_expense,0)/(t1.案件收入/t1.案件量) as decimal(10,4))   施博录入成本占比,
       cast(coalesce(t5.avg_expense,0) as decimal(10,2)) 施博录入均价,
       coalesce(t5.IN_CLAIM_NUM,0) 施博录入案件量,
       cast(coalesce(t5.expense,0) as decimal(10,2)) 施博录入成本,
       cast(coalesce(t6.avg_expense,0)/(t1.案件收入/t1.案件量) as decimal(10,4))  成都视觉录入成本占比,
       cast(coalesce(t6.avg_expense,0) as decimal(10,2)) 成都视觉录入均价,
       coalesce(t6.IN_CLAIM_NUM,0) 成都视觉录入案件量,
       cast(coalesce(t6.expense ,0) as decimal(10,2)) 成都视觉录入成本,
       cast(coalesce(t7.avg_expense ,0)/(t1.案件收入/t1.案件量) as decimal(10,4))  广纳录入成本占比,
       cast(coalesce(t7.avg_expense ,0) as decimal(10,2)) 广纳录入均价,
       coalesce(t7.IN_CLAIM_NUM ,0) 广纳录入案件量,
       cast(coalesce(t7.expense ,0) as decimal(10,2)) 广纳录入成本,
       cast(coalesce(t8.avg_expense ,0)/(t1.案件收入/t1.案件量) as decimal(10,4))  智在录入成本占比,
       cast(coalesce(t8.avg_expense ,0) as decimal(10,2)) 智在录入均价,
       coalesce(t8.IN_CLAIM_NUM ,0) 智在录入案件量,
       cast(coalesce(t8.expense ,0) as decimal(10,2)) 智在录入成本,
       REPLACE(CURDATE(),'-','')
     from  t1
    left join  t2
  on  t1.大渠道 = t2.大渠道 and t1.年月 = t2.年月 and t2.entry_merchant = '合计'
  left join t3
on t1.大渠道 = t3.大渠道 and t1.年月 = t3.年月
 left join CLAIM_DWS.DWS_CLAIM_MANAGE_SHARE_M t4
     on  t1.年月 = t4.DT_MONTH
     left join  CLAIM_DWS.DWS_INPUT_COST_M t5
     on t1.大渠道 = t5.insurance_company_channel and t1.年月 = t5.DT_MONTH  and t5.entry_merchant = '施博'
         left join  CLAIM_DWS.DWS_INPUT_COST_M t6
     on t1.大渠道 = t6.insurance_company_channel and t1.年月 = t6.DT_MONTH  and t6.entry_merchant = '成都视觉'
         left join  CLAIM_DWS.DWS_INPUT_COST_M t7
     on t1.大渠道 = t7.insurance_company_channel and t1.年月 = t7.DT_MONTH  and t7.entry_merchant = '广纳'
              left join  CLAIM_DWS.DWS_INPUT_COST_M t8
     on t1.大渠道 = t8.insurance_company_channel and t1.年月 = t8.DT_MONTH  and t8.entry_merchant = '因朔桔'
    where t1.年月 ='{yesterday_month}'

"""
def truncate_table(table_name='CLAIM_DM.ADM_PROJECT_ROFIT_M'):
    with DatabaseConnection() as conn:
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
