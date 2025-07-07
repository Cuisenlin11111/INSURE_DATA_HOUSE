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
   INSERT INTO CLAIM_DM.ADM_PROJECT_ROFIT_M_T
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
    Case_Avg_Price_1,
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
    dt,
    `HLP_MANAGE_M` ,
    `HLP_MANAGE_CLAIM` ,
    `YSJ_MANAGE_M` ,
    `YSJ_MANAGE_CLAIM`)
   with t1 as (                     select
                                  '太保财全渠道' as insurance_company_channel,
                                 aa.dt_month,
                                 sum(aa.DRHCAL) '回传案件量',
                                    sum(aa.CLAIM_NUM) '案件量',
                                    sum(Case_Income) '案件收入',
                                    sum(Tax_Exempt_Income) '除税收入',
                                    sum(Marketing_Cost) '营销成本',
                                    sum(Gross_Profit) '毛利润',
                                    sum(Gross_Profit)/sum(Tax_Exempt_Income)    '毛利润率',
                                    sum(Gross_Profit_2) '毛利润2',
                                    sum(Gross_Profit_2)/sum(Tax_Exempt_Income)  '毛利润率2',
                                    sum(Sim_Profit) '单纯净利润',
                                    sum(Sim_Profit)/sum(Tax_Exempt_Income)   '单纯净利润率',
                                    sum(Net_Profit )  '净利润',
                                     sum(Net_Profit )/sum(Tax_Exempt_Income) '净利润率',
                                    (sum(SHLR_COST ) + sum(CDSJLR_COST ) +sum(GNLR_COST ) ) /sum(Case_Income)  '外包录入占收入比',
                                     (sum(SHLR_COST ) + sum(CDSJLR_COST ) +sum(GNLR_COST ) ) /sum(aa.CLAIM_NUM) '外包录入成本案件',
                                    sum( TECH_VOL*TECH_MONEY )/sum(aa.CLAIM_NUM)  '项目技术成本案件',
                                    sum(CLAIM_OPERATE_VOL*CLAIM_OPERATE_MONEY + CONFIG_VOL*CONFIG_MONEY + CLAIM_STANDARD_VOL*CLAIM_STANDARD_MONEY + CLAIM_AUDIT_VOL*CLAIM_AUDIT_MONEY + CUSTOMER_VOL*CUSTOMER_MONEY)/sum(aa.CLAIM_NUM)   '项目作业成本案件',
                                     sum(CLAIM_OPERATE_VOL*CLAIM_OPERATE_MONEY + CONFIG_VOL*CONFIG_MONEY + CLAIM_STANDARD_VOL*CLAIM_STANDARD_MONEY +
                                      CLAIM_AUDIT_VOL*CLAIM_AUDIT_MONEY + CUSTOMER_VOL*CUSTOMER_MONEY + TECH_VOL*TECH_MONEY
                                      )/sum(aa.CLAIM_NUM) + cc.HLP_MANAGE_CLAIM + cc.YSJ_MANAGE_CLAIM +(sum(SHLR_COST ) + sum(CDSJLR_COST ) +sum(GNLR_COST ) ) /sum(aa.CLAIM_NUM) + sum(Marketing_Cost)/sum(aa.CLAIM_NUM) '总成本案件',
                                    sum(Personnel_Capacity_Psn) '项目作业人数',
                                    sum(aa.CLAIM_NUM)/sum(Personnel_Capacity_Psn)  '项目作业人均产能',
                                    sum(Case_Income)/sum(aa.CLAIM_NUM)  '案件均价',
                                    sum(Case_Income)/sum(aa.DRHCAL)  '案件均价不含撤案',
                                     sum(SHLR_COST ) / sum(Case_Income)   '施博录入成本占比',
                                    case when sum(SBLR_NUM )=0 then 0 else sum(SHLR_COST ) /  sum(SBLR_NUM )  end  '施博录入均价',
                                    sum(SBLR_NUM )    '施博录入案件量',
                                    sum(SHLR_COST )    '施博录入成本',
                                    sum(CDSJLR_COST )/ sum(Case_Income)   '成都视觉录入成本占比',
                                    case when sum(CDSJLR_NUM)=0 then 0 else  sum(CDSJLR_COST )/sum(CDSJLR_NUM ) end    '成都视觉录入均价',
                                    sum(CDSJLR_NUM )    '成都视觉录入案件量',
                                    sum(CDSJLR_COST )    '成都视觉录入成本',
                                    sum(GNLR_COST )/sum(Case_Income)    '广纳录入成本占比',
                                    case when sum(GNLR_NUM)=0 then 0 else sum(GNLR_COST )/sum(GNLR_NUM ) end     '广纳录入均价',
                                    sum(GNLR_NUM )    '广纳录入案件量',
                                    sum(GNLR_COST )    '广纳录入成本',
                                    avg(ZZSJLR_RATE )    '智在录入成本占比',
                                    avg(ZZSJLR_AVG_Price )  '智在录入均价',
                                    sum(ZZSJLR_NUM )  '智在录入案件量',
                                    sum(ZZSJLR_COST )   '智在录入成本',
                                    aa.dt
                              from CLAIM_DM.ADM_PROJECT_ROFIT_M aa
                              left join  CLAIM_DIM.DIM_MANPOWER_CONFIG bb
                              on aa.insurance_company_channel = bb.CHANNEL_VALUE
                              and aa.dt_month = bb.dt_month
                              left join CLAIM_DWS.DWS_CLAIM_MANAGE_SHARE_M cc
                            on  aa.dt_month = cc.dt_month
                             where aa.insurance_company_channel in ('太保产险宁波分公司','太保产险苏州分公司','太保产险上海分公司')
                             group by  aa.dt_month
                        union all
                        select
                                  '泰康全渠道' as insurance_company_channel,
                                   aa.dt_month,
                                   sum(aa.DRHCAL) '回传案件量',
                                    sum(aa.CLAIM_NUM) '案件量',
                                    sum(Case_Income) '案件收入',
                                    sum(Tax_Exempt_Income) '除税收入',
                                    sum(Marketing_Cost) '营销成本',
                                    sum(Gross_Profit) '毛利润',
                                    sum(Gross_Profit)/sum(Tax_Exempt_Income)    '毛利润率',
                                    sum(Gross_Profit_2) '毛利润2',
                                    sum(Gross_Profit_2)/sum(Tax_Exempt_Income)  '毛利润率2',
                                    sum(Sim_Profit) '单纯净利润',
                                    sum(Sim_Profit)/sum(Tax_Exempt_Income)   '单纯净利润率',
                                    sum(Net_Profit )  '净利润',
                                     sum(Net_Profit )/sum(Tax_Exempt_Income) '净利润率',
                                    (sum(SHLR_COST ) + sum(CDSJLR_COST ) +sum(GNLR_COST ) ) /sum(Case_Income)  '外包录入占收入比',
                                     (sum(SHLR_COST ) + sum(CDSJLR_COST ) +sum(GNLR_COST ) ) /sum(aa.CLAIM_NUM) '外包录入成本案件',
                                    sum( TECH_VOL*TECH_MONEY )/sum(aa.CLAIM_NUM)  '项目技术成本案件',
                                    sum(CLAIM_OPERATE_VOL*CLAIM_OPERATE_MONEY + CONFIG_VOL*CONFIG_MONEY + CLAIM_STANDARD_VOL*CLAIM_STANDARD_MONEY + CLAIM_AUDIT_VOL*CLAIM_AUDIT_MONEY + CUSTOMER_VOL*CUSTOMER_MONEY)/sum(aa.CLAIM_NUM)   '项目作业成本案件',
                                     sum(CLAIM_OPERATE_VOL*CLAIM_OPERATE_MONEY + CONFIG_VOL*CONFIG_MONEY + CLAIM_STANDARD_VOL*CLAIM_STANDARD_MONEY +
                                      CLAIM_AUDIT_VOL*CLAIM_AUDIT_MONEY + CUSTOMER_VOL*CUSTOMER_MONEY + TECH_VOL*TECH_MONEY
                                      )/sum(aa.CLAIM_NUM) + cc.HLP_MANAGE_CLAIM + cc.YSJ_MANAGE_CLAIM +(sum(SHLR_COST ) + sum(CDSJLR_COST ) +sum(GNLR_COST ) ) /sum(aa.CLAIM_NUM) + sum(Marketing_Cost)/sum(aa.CLAIM_NUM) '总成本案件',
                                    sum(Personnel_Capacity_Psn) '项目作业人数',
                                    sum(aa.CLAIM_NUM)/sum(Personnel_Capacity_Psn)  '项目作业人均产能',
                                    sum(Case_Income)/sum(aa.CLAIM_NUM)  '案件均价',
                                     sum(Case_Income)/sum(aa.DRHCAL)  '案件均价不含撤案',
                                     sum(SHLR_COST ) / sum(Case_Income)   '施博录入成本占比',
                                    case when sum(SBLR_NUM )=0 then 0 else sum(SHLR_COST ) /  sum(SBLR_NUM )  end  '施博录入均价',
                                    sum(SBLR_NUM )    '施博录入案件量',
                                    sum(SHLR_COST )    '施博录入成本',
                                    sum(CDSJLR_COST )/ sum(Case_Income)   '成都视觉录入成本占比',
                                    case when sum(CDSJLR_NUM)=0 then 0 else  sum(CDSJLR_COST )/sum(CDSJLR_NUM ) end    '成都视觉录入均价',
                                    sum(CDSJLR_NUM )    '成都视觉录入案件量',
                                    sum(CDSJLR_COST )    '成都视觉录入成本',
                                    sum(GNLR_COST )/sum(Case_Income)    '广纳录入成本占比',
                                    case when sum(GNLR_NUM)=0 then 0 else sum(GNLR_COST )/sum(GNLR_NUM ) end     '广纳录入均价',
                                    sum(GNLR_NUM )    '广纳录入案件量',
                                    sum(GNLR_COST )    '广纳录入成本',
                                    avg(ZZSJLR_RATE )    '智在录入成本占比',
                                     sum(ZZSJLR_COST ) /sum(ZZSJLR_NUM )  '智在录入均价',
                                    sum(ZZSJLR_NUM )  '智在录入案件量',
                                    sum(ZZSJLR_COST )   '智在录入成本',
                                    aa.dt
                              from CLAIM_DM.ADM_PROJECT_ROFIT_M aa
                              left join  CLAIM_DIM.DIM_MANPOWER_CONFIG bb
                              on aa.insurance_company_channel = bb.CHANNEL_VALUE
                              and aa.dt_month = bb.dt_month
                              left join CLAIM_DWS.DWS_CLAIM_MANAGE_SHARE_M cc
                            on aa.dt_month = cc.dt_month
                             where aa.insurance_company_channel like "%泰康%"
                             group by  aa.dt_month
                        union all
                        select aa.*
                        from CLAIM_DM.ADM_PROJECT_ROFIT_M aa)
                       select insurance_company_channel,
                                    t1.dt_month,
                                    回传案件量,
                                    案件量 ,
                                    cast(案件收入 as decimal(10,2)) ,
                                    cast(除税收入 as decimal(10,2)),
                                    cast(营销成本 as decimal(10,2)),
                                    cast(毛利润 as decimal(10,2)),
                                    cast(毛利润率 as decimal(10,4)),
                                    cast(毛利润2  as decimal(10,2)) ,
                                    cast(毛利润率2 as decimal(10,4)),
                                    cast(单纯净利润  as decimal(10,2)) ,
                                    cast(单纯净利润率 as decimal(10,4)),
                                    cast(净利润  as decimal(10,2)) ,
                                    cast(净利润率 as decimal(10,4)),
                                    cast(外包录入占收入比  as decimal(10,4)) ,
                                    cast(外包录入成本案件 as decimal(10,2)) ,
                                    cast(项目技术成本案件 as decimal(10,2)) ,
                                    cast(项目作业成本案件 as decimal(10,2)) ,
                                    cast(总成本案件 as decimal(10,2)) ,
                                    cast(项目作业人数 as decimal(10,2)) ,
                                    cast(项目作业人均产能 as decimal(10,2)) ,
                                    cast(案件均价 as decimal(10,2)) ,
                                    cast(案件均价不含撤案 as decimal(10,2)) ,
                                    cast(施博录入成本占比 as decimal(10,4)),
                                    cast(施博录入均价 as decimal(10,2)) ,
                                    cast(施博录入案件量 as bigint),
                                    cast(施博录入成本 as decimal(10,2)) ,
                                    cast(成都视觉录入成本占比 as decimal(10,4)),
                                    cast(成都视觉录入均价 as decimal(10,2)) ,
                                    cast(成都视觉录入案件量 as bigint),
                                    cast(成都视觉录入成本 as decimal(10,2)) ,
                                    cast(广纳录入成本占比 as decimal(10,4)),
                                    cast(广纳录入均价 as decimal(10,2)) ,
                                    cast(广纳录入案件量 as bigint),
                                    cast(广纳录入成本 as decimal(10,2)) ,
                                    cast(智在录入成本占比 as decimal(10,4)),
                                    cast(智在录入均价 as decimal(10,2)) ,
                                    cast(智在录入案件量 as bigint),
                                    cast(智在录入成本 as decimal(10,2)) ,
                                    t1.dt,
                                    cast(t2.HLP_MANAGE_CLAIM*t1.案件量 as decimal(10,2)) ,
                                    cast(t2.HLP_MANAGE_CLAIM as decimal(10,2)) ,
                                    cast(t2.YSJ_MANAGE_CLAIM*t1.案件量 as decimal(10,2)) ,
                                    cast(t2.YSJ_MANAGE_CLAIM as decimal(10,2))
                       from t1
                         left join CLAIM_DWS.DWS_CLAIM_MANAGE_SHARE_M t2
                            on t1.dt_month = t2.dt_month
                            where t1.dt_month = '{yesterday_month}'

"""


def truncate_table(table_name='CLAIM_DM.ADM_PROJECT_ROFIT_M_T'):
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
