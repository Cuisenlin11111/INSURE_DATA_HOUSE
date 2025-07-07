import sys
# sys.path.append(r"E:\pycharm\database")
from database import DatabaseConnection
from datetime import datetime



sql_query = f"""
  insert into CLAIM_DWS.DWS_ALL_FLOW_EFF_MONITOR
    (insure_company_channel,
     create_time,
     autosh_claim,
     allsh_claim,
     shzdhl_rw,
     sh_auto_rate,
     fh_auto_rate,
     in_claim,
     per_manual_claim_vol,
     PER_MANUAL_RATE,
     hospital_manual_vol,
     hospital_match_total,
     HOSPITAL_MATCH_RATE,
     diag_match_manual_vol,
     diag_match_total,
     DIAG_MATCH_RATE,
     detail_manual_match_vol,
     detail_total,
     DETAIL_RATE,
     manual_charge_bill_vol,
     manual_charge_vol,
     charge_total,
     CHARGE_RATE,
     config_manual_vol,
     config_total,
     CONFIG_RATE,
     ZDGZ_TOTAL,
	 ZDGZ_ZD,
	 ZDGZ_ZD_RATE,
     data_dt)
with  tmp_00 as (	 
    select a1.insure_company_channel,
           a1.create_time,
           自动审核案件数 autosh_claim,
           当天审核案件总数 allsh_claim,
           a2.shzdhl_rw,
           coalesce(a2.sh_auto_rate, 0) sh_auto_rate,
           coalesce(a2.fh_auto_rate, 0) fh_auto_rate,
           a1.in_claim,
           a1.per_manual_claim_vol,
           a1.hospital_manual_vol,
           a1.hospital_match_total,
           a1.diag_match_manual_vol,
           a1.diag_match_total,
           a1.detail_manual_match_vol,
           a1.detail_total,
           a1.manual_charge_bill_vol,
           a1.manual_charge_vol,
           a1.charge_total,
           a1.config_manual_vol,
           a1.config_total
      from claim_dwd.dwd_all_flow_eff_monitor a1
      left join (select insure_company_channel,
                        gmt_created,
                        shzdhl_rw,
                        shzdhl sh_auto_rate,
                        fhzdhl fh_auto_rate,
                        coalesce(zdshal, 0) 自动审核案件数,
                        coalesce(drhcal, 0) 当天审核案件总数
                   from claim_dws.dws_claim_count_day) a2
        on a1.insure_company_channel = a2.insure_company_channel
       and a1.create_time = a2.gmt_created
    union all
    ##泰康全渠道
    select insure_company_channel,
           create_time,
           自动审核案件数          autosh_claim,
           当天审核案件总数        allsh_claim,
           shzdhl_rw,
           sh_auto_rate,
           fh_auto_rate,
           in_claim,
           per_manual_claim_vol,
           hospital_manual_vol,
           hospital_match_total,
           diag_match_manual_vol,
           diag_match_total,
           detail_manual_match_vol,
           detail_total,
           manual_charge_bill_vol,
           manual_charge_vol,
           charge_total,
           config_manual_vol,
           config_total
      from CLAIM_DWD.DWD_ALL_FLOW_TK_MONITOR
      left join (select gmt_created,
                        sum(zdshal) 自动审核案件数,
                        sum(drhcal) 当天审核案件总数
                   from claim_dws.dws_claim_count_day
                  where insure_company_channel like '%泰康养老%'
                  group by gmt_created)
        on create_time = gmt_created
    UNION ALL 
    ##太保财全渠道
    select insure_company_channel,
           create_time,
           自动审核案件数          autosh_claim,
           当天审核案件总数        allsh_claim,
           shzdhl_rw,
           sh_auto_rate,
           fh_auto_rate,
           in_claim,
           per_manual_claim_vol,
           hospital_manual_vol,
           hospital_match_total,
           diag_match_manual_vol,
           diag_match_total,
           detail_manual_match_vol,
           detail_total,
           manual_charge_bill_vol,
           manual_charge_vol,
           charge_total,
           config_manual_vol,
           config_total
      from CLAIM_DWD.DWD_ALL_FLOW_TBC_MONITOR
      left join (select gmt_created,
                        sum(zdshal) 自动审核案件数,
                        sum(drhcal) 当天审核案件总数
                   from claim_dws.dws_claim_count_day
                  where insure_company_channel IN ('太保产险苏州分公司','太保产险宁波分公司','太保产险大连分公司','太保健康')
                  group by gmt_created)
        on create_time = gmt_created)
		SELECT
            aa.insure_company_channel,
            aa.create_time,
            COALESCE(aa.autosh_claim, 0),
            COALESCE(aa.allsh_claim, 0),
            CAST(COALESCE(aa.shzdhl_rw, 0) AS DECIMAL(10, 4)),
            CAST(aa.sh_auto_rate AS DECIMAL(10, 4)),
            CAST(aa.fh_auto_rate AS DECIMAL(10, 4)),
            aa.in_claim,
            aa.per_manual_claim_vol,
            CAST(CASE WHEN aa.IN_CLAIM = 0 THEN 0 ELSE 1 - aa.per_manual_claim_vol / aa.IN_CLAIM END  AS DECIMAL(10, 4)),
            aa.hospital_manual_vol,
            aa.hospital_match_total,
            CAST(CASE WHEN aa.hospital_match_total = 0 THEN 0 ELSE 1 - aa.hospital_manual_vol / aa.hospital_match_total END  AS DECIMAL(10, 4)),
            aa.diag_match_manual_vol,
            aa.diag_match_total,
            CAST(CASE WHEN aa.diag_match_total = 0 THEN 0 ELSE 1- aa.diag_match_manual_vol / aa.diag_match_total END  AS DECIMAL(10, 4)),
            aa.detail_manual_match_vol,
            aa.detail_total,
            CAST(CASE WHEN aa.detail_total = 0 THEN 0 ELSE 1 - aa.detail_manual_match_vol / aa.detail_total END AS DECIMAL(10, 4)),
            aa.manual_charge_bill_vol,
            aa.manual_charge_vol,
            aa.charge_total,
            CAST(CASE WHEN aa.CHARGE_TOTAL = 0 THEN 0 ELSE 1 -aa.manual_charge_bill_vol /aa.CHARGE_TOTAL END  AS DECIMAL(10, 4)),
            aa.config_manual_vol,
            aa.config_total,
            CAST(CASE WHEN aa.config_total = 0 THEN 0 ELSE 1 - aa.config_manual_vol / aa.config_total end AS DECIMAL(10, 4)),
            COALESCE(bb.total_count, 0),
            COALESCE(bb.ZDGZ_ZD, 0),
            CAST(COALESCE(bb.ZDGZ_ZD_RATE, 0) AS DECIMAL(10, 4)),
            REPLACE(aa.create_time, '-', '')
		   from  tmp_00 aa
		   left join  (  SELECT
        substr(aa.gmt_created,1,10) AS gmt_created_str,
        bb.channel_value,
        COUNT(*) AS total_count,
        SUM(CASE WHEN is_created_task='N' THEN 1 ELSE 0 END) AS ZDGZ_ZD,
        ROUND(CASE WHEN COUNT(*)=0 THEN 0 ELSE SUM(CASE WHEN is_created_task='N' THEN 1 ELSE 0 END) / COUNT(*) END, 6) AS ZDGZ_ZD_RATE
    FROM claim_ods.bill_diagnose_rule_match_task aa
    LEFT JOIN claim_ods.dim_insure_company_channel bb
    ON aa.insure_company_channel = bb.channel_key
    where aa.insure_company_channel<>'CP08'
    GROUP BY substr(aa.gmt_created,1,10), bb.channel_value

union all

  select SUBSTR(PR.back_time,1,10) as gmt_created_str,
         '太保健康' as channel_value,
         count(b.id) as total_count,
         count(b.id)-SUM(CASE WHEN is_created_task='Y' THEN 1 ELSE 0 END)  AS ZDGZ_ZD,
         ROUND( (count(b.id)-SUM(CASE WHEN is_created_task='Y' THEN 1 ELSE 0 END))/ count(b.id) , 6) AS ZDGZ_ZD_RATE
  from claim_ods.postback_record  pr
  left join claim_ods.claim c on pr.app_no=c.claim_no and c.delete_flag='0' and c.insure_company_channel='CP08'
  left join  claim_ods.bill b  on pr.app_no=b.claim_no and b.delete_flag='0' and b.insure_company_channel='CP08'
   left join   claim_ods.bill_diagnose_rule_match_task aa on b.claim_id = aa.claim_id  and  aa.bill_id=b.id and aa.insure_company_channel='CP08'
  where pr.insure_company_channel='CP08' AND  pr.is_deleted='N'
  GROUP BY SUBSTR(PR.back_time,1,10)  ) bb
		   on aa.create_time =bb.gmt_created_str  and aa.insure_company_channel =bb.channel_value;
"""
def truncate_table(table_name='CLAIM_DWS.DWS_ALL_FLOW_EFF_MONITOR'):
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
