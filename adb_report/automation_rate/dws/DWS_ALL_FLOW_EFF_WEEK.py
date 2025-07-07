import sys
# sys.path.append(r"E:\pycharm\database")
from database import DatabaseConnection
from datetime import datetime



sql_query = f"""
 insert into CLAIM_DWS.DWS_ALL_FLOW_EFF_WEEK
    (insure_company_channel,
     week_region,
     sh_auto_rate,
     fh_auto_rate,
     per_manual_rate,
     hospital_match_rate,
     diag_match_rate,
     detail_match_rate,
     charge_bill_rate,
     config_rate,
	 ZDGZ_ZD_RATE,
     data_dt)
with t1 as
     (select insure_company_channel,

             week_region,
             case
               when coalesce(sum(qlchcal), 0) = 0 then
                0
               else
                coalesce(sum(fh_auto_claim_count), 0) /
                (coalesce(sum(qlchcal), 0)+coalesce(sum(BLCHCAL), 0))
             end as 复核自动化率,
             case
               when coalesce(sum(qlchcal), 0) = 0 then
                0
               else
                coalesce(sum(zdshal), 0) / (coalesce(sum(qlchcal), 0)+coalesce(sum(BLCHCAL), 0))
             end as 审核自动化率
        from CLAIM_DWS.DWS_CLAIM_COUNT_WEEK
       group by insure_company_channel, week_region
      union all
      ##泰康全渠道
      select insure_company_channel,
             week_region,
             case
               when coalesce(sum(qlchcal), 0) = 0 then
                0
               else
                coalesce(sum(fh_auto_claim_count), 0) /
                (coalesce(sum(qlchcal), 0)+coalesce(sum(BLCHCAL), 0))
             end as 复核自动化率,
             case
               when coalesce(sum(qlchcal), 0) = 0 then
                0
               else
                coalesce(sum(zdshal), 0) / coalesce(sum(qlchcal), 0)
             end as 审核自动化率
        from (select case
                       when insure_company_channel like '%泰康养老%' then
                        '泰康养老全渠道'
                       else
                        insure_company_channel
                     end as insure_company_channel,
                     week_region,
                     fh_auto_claim_count,
                     zdshal,
                     qlchcal,
                     BLCHCAL
                from CLAIM_DWS.DWS_CLAIM_COUNT_WEEK
               where insure_company_channel like '%泰康养老%')
       group by insure_company_channel, week_region),
    t2 as
     (select insure_company_channel,
             wk.week_qj as week_region,
             case
               when coalesce(sum(charge_total), 0) = 0 then
                0
               else
                1 - coalesce(sum(manual_charge_bill_vol), 0) /
                coalesce(sum(charge_total), 0)
             end as 扣费发票自动化率,
             case
               when coalesce(sum(detail_total), 0) = 0 then
                0
               else
                1 - coalesce(sum(detail_manual_match_vol), 0) /
                coalesce(sum(detail_total), 0)
             end as 明细匹配自动化率,
             case
               when coalesce(sum(config_total), 0) = 0 then
                0
               else
                1 - coalesce(sum(config_manual_vol), 0) /
                coalesce(sum(config_total), 0)
             end as 配置自动化率,
             case
               when coalesce(sum(hospital_match_total), 0) = 0 then
                0
               else
                1 - coalesce(sum(hospital_manual_vol), 0) /
                coalesce(sum(hospital_match_total), 0)
             end as 医院自动化率,
             case
               when coalesce(sum(in_claim), 0) = 0 then
                0
               else
                1 - coalesce(sum(per_manual_claim_vol), 0) /
                coalesce(sum(in_claim), 0)
             end as 预审自动化率,
             case
               when coalesce(sum(diag_match_total), 0) = 0 then
                0
               else
                1 - coalesce(sum(diag_match_manual_vol), 0) /
                coalesce(sum(diag_match_total), 0)
             end as 诊断自动化率
        from CLAIM_DWS.dws_all_flow_eff_monitor
       inner join claim_ods.DIM_DATE_WEEK wk
          on create_time = wk.date_dtd
       group by insure_company_channel, wk.week_qj),
	   t3 as (    SELECT
                dt_dim.week_qj,
				bb.channel_value,
				 ROUND(SUM(CASE WHEN is_created_task='N' THEN 1 ELSE 0 END) / COUNT(*), 6) AS ZDGZ_ZD_RATE
    FROM claim_ods.bill_diagnose_rule_match_task aa
	   inner join claim_ods.DIM_DATE_WEEK dt_dim
        on substr(aa.gmt_created,1,10) = substr(dt_dim.date_dtd,1,10)
		LEFT JOIN claim_ods.dim_insure_company_channel bb
    ON aa.insure_company_channel = bb.channel_key
		GROUP BY  bb.channel_value,dt_dim.week_qj)
    select t2.insure_company_channel,
           t2.week_region,
           CAST(coalesce(t1.审核自动化率,0)  AS DECIMAL(10, 4)),
           CAST(coalesce(t1.复核自动化率,0)  AS DECIMAL(10, 4)),
           CAST(t2.预审自动化率 AS DECIMAL(10, 4)),
           CAST(t2.医院自动化率 AS DECIMAL(10, 4)),
           CAST(t2.诊断自动化率 AS DECIMAL(10, 4)),
           CAST(t2.明细匹配自动化率 AS DECIMAL(10, 4)),
           CAST(t2.扣费发票自动化率 AS DECIMAL(10, 4)),
           CAST(t2.配置自动化率 AS DECIMAL(10, 4)),
		   CAST(coalesce(t3.ZDGZ_ZD_RATE,0) AS DECIMAL(10, 4)),
           curdate()
      from t2
       left  join t1
        on t1.insure_company_channel = t2.insure_company_channel
       and t1.week_region = t2.week_region
	   left join t3
	   on t1.insure_company_channel = t3.channel_value
       and t1.week_region = t3.week_qj;
"""
def truncate_table(table_name='CLAIM_DWS.DWS_ALL_FLOW_EFF_WEEK'):
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
