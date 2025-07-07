import sys
#sys.path.append(r"E:\pycharm\database")
from database import DatabaseConnection
from datetime import datetime



sql_query = f"""
insert into CLAIM_DWD.DWD_ALL_FLOW_EFF_MONITOR
    (insure_company_channel,
     create_time,
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
     CHARGE_TOTAL,
     config_manual_vol,
     config_total,
     data_dt)
with a1 as
     (
       SELECT
               sum(in_claim) in_claim,
               insure_company_channel,
               create_time
       FROM
       (
               select count(*) in_claim,
             CASE
                WHEN c.insure_company_channel = 'CP10' THEN a.DEPARTMENT_CODE
                ELSE c.insure_company_channel
             END insure_company_channel,
             substr(c.create_time, 1,10) create_time
            from claim_ods.accept_list_record a
            join claim_ods.claim c
              on a.accept_num = c.acceptance_no
             and c.delete_flag = '0'
           where a.DEL_FLAG = '0'
             and c.delete_flag = '0'
             and substr(c.create_time, 1, 10) >= (CURRENT_DATE - INTERVAL '100' DAY)
           group by c.insure_company_channel,
                    substr(c.create_time, 1,10),
                    a.DEPARTMENT_CODE
       )
       GROUP BY insure_company_channel,create_time
     ),
    a2 as
     (
         SELECT
               sum(per_manual_claim_vol) per_manual_claim_vol,
               insure_company_channel,
               create_time
        FROM
        (
         	select count(DISTINCT c.id) per_manual_claim_vol,
                 CASE
                    WHEN c.insure_company_channel = 'CP10' THEN a.DEPARTMENT_CODE
                    ELSE c.insure_company_channel
             	 END insure_company_channel,
                 substr(c.create_time, 1,10) create_time
            from claim_ods.accept_list_record a
            join claim_ods.claim c
              on a.accept_num = c.acceptance_no
             and c.delete_flag = '0'
            join claim_ods.clm_pretrial_examine e
              on c.claim_no = e.claim_app_no
           where c.delete_flag = '0'
             
             and substr(c.create_time, 1, 10) >= (CURRENT_DATE - INTERVAL '100' DAY)
           group by c.insure_company_channel,
                    substr(c.create_time, 1,10),
                    a.DEPARTMENT_CODE
        )
        GROUP BY insure_company_channel,create_time
     ),
    a3 as
     (
         SELECT
               sum(hospital_manual_vol) hospital_manual_vol,
               insure_company_channel,
               create_time
        FROM
        (
         	select count(t.id) hospital_manual_vol,
                 CASE
                    WHEN c.insure_company_channel = 'CP10' THEN a.DEPARTMENT_CODE
                    ELSE c.insure_company_channel
             	 END insure_company_channel,
                 substr(c.create_time, 1,10) create_time
            from claim_ods.manual_match_task t
            join claim_ods.claim c
              on t.claim_id = c.id
            LEFT JOIN claim_ods.accept_list_record a
            ON c.ACCEPTANCE_NO = a.accept_num AND a.DEL_FLAG = '0'
           where c.delete_flag = '0'
             and c.clm_process_status not in ('0', '1')
             and match_type = 1
             and substr(c.create_time, 1, 10) >= (CURRENT_DATE - INTERVAL '100' DAY)
           group by c.insure_company_channel,
                    substr(c.create_time, 1,10),
                    a.DEPARTMENT_CODE
        )
        GROUP BY insure_company_channel,create_time
     ),
    a4 as
     (
         SELECT
               sum(diag_match_manual_vol) diag_match_manual_vol,
               insure_company_channel,
               create_time
        FROM
        (
         	select count(distinct t.id) diag_match_manual_vol,
                 CASE
                    WHEN c.insure_company_channel = 'CP10' THEN a.DEPARTMENT_CODE
                    ELSE c.insure_company_channel
             	 END insure_company_channel,
                 substr(c.create_time, 1,10) create_time
            from claim_ods.manual_match_task t
            join claim_ods.claim c
              on t.claim_id = c.id
            LEFT JOIN claim_ods.accept_list_record a
            ON c.ACCEPTANCE_NO = a.accept_num AND a.DEL_FLAG = '0'
           where c.clm_process_status not in ('0', '1')
             and match_type = 2
             and c.delete_flag = '0'
             
             and substr(c.create_time, 1, 10) >= (CURRENT_DATE - INTERVAL '100' DAY)
           group by c.insure_company_channel,
                    substr(c.create_time, 1,10),
                    a.DEPARTMENT_CODE
       )
       GROUP BY insure_company_channel,create_time
     ),
    a5 as
     (
         SELECT
               sum(detail_manual_match_vol) detail_manual_match_vol,
               insure_company_channel,
               create_time
        FROM
        (
         	select count(distinct t.id) detail_manual_match_vol,
                 CASE
                    WHEN c.insure_company_channel = 'CP10' THEN a.DEPARTMENT_CODE
                    ELSE c.insure_company_channel
             	 END insure_company_channel,
                 substr(c.create_time, 1,10) create_time
            from claim_ods.manual_match_task t
            join claim_ods.claim c
              on t.claim_id = c.id
            LEFT JOIN claim_ods.accept_list_record a
              ON c.ACCEPTANCE_NO = a.accept_num AND a.DEL_FLAG = '0'
           where c.clm_process_status not in ('0', '1')
             and match_type = 3
             and c.delete_flag = '0'
             
             and substr(c.create_time, 1, 10) >= (CURRENT_DATE - INTERVAL '100' DAY)
           group by c.insure_company_channel,
                    substr(c.create_time, 1,10),
                    a.DEPARTMENT_CODE
       )
       GROUP BY insure_company_channel,create_time
    ),
    a6 as
     (
         SELECT
               sum(manual_charge_bill_vol) manual_charge_bill_vol,
               insure_company_channel,
               create_time
        FROM
        (
         	select count(distinct b.id) manual_charge_bill_vol,
                 CASE
                    WHEN c.insure_company_channel = 'CP10' THEN a.DEPARTMENT_CODE
                    ELSE c.insure_company_channel
             	 END insure_company_channel,
                 substr(c.create_time, 1,10) create_time
            from claim_ods.claim c
            LEFT JOIN claim_ods.accept_list_record a
              ON c.ACCEPTANCE_NO = a.accept_num AND a.DEL_FLAG = '0'
            join claim_ods.deduct_task d
              on c.id = d.claim_id
            left join claim_ods.bill b
              on b.claim_id = c.id
             and b.delete_flag = '0'
           where c.delete_flag = '0'
             and b.reason_notes is not null
             
             and substr(c.create_time, 1, 10) >= (CURRENT_DATE - INTERVAL '100' DAY)
           group by c.insure_company_channel,
                    substr(c.create_time, 1,10),
                    a.DEPARTMENT_CODE
        )
        GROUP BY insure_company_channel,create_time
     ),
    a7 as
     (
         SELECT
               sum(manual_charge_vol) manual_charge_vol,
               insure_company_channel,
               create_time
        FROM
        (
         	select count(DISTINCT c.id) manual_charge_vol,
                 CASE
                    WHEN c.insure_company_channel = 'CP10' THEN a.DEPARTMENT_CODE
                    ELSE c.insure_company_channel
             	 END insure_company_channel,
                 substr(c.create_time, 1,10) create_time
            from claim_ods.claim c
            LEFT JOIN claim_ods.accept_list_record a
                  ON c.ACCEPTANCE_NO = a.accept_num AND a.DEL_FLAG = '0'
            join claim_ods.deduct_task d
              on c.id = d.claim_id
            left join claim_ods.bill b
              on b.claim_id = c.id
             and b.delete_flag = '0'
           where c.delete_flag = '0'
             and b.reason_notes is not null
             
             and substr(c.create_time, 1, 10) >= (CURRENT_DATE - INTERVAL '100' DAY)
           group by c.insure_company_channel,
                    substr(c.create_time, 1,10),
                    a.DEPARTMENT_CODE
        )
        GROUP BY insure_company_channel,create_time
     ),
    a8 as
     (
         SELECT
               sum(扣费总量) 扣费总量,
               insure_company_channel,
               create_time
        FROM
        (
         	select count(DISTINCT b.id) 扣费总量,
                 CASE
                    WHEN c.insure_company_channel = 'CP10' THEN a.DEPARTMENT_CODE
                    ELSE c.insure_company_channel
             	 END insure_company_channel,
                 substr(c.create_time, 1,10) create_time
            from claim_ods.claim c
            LEFT JOIN claim_ods.accept_list_record a
                  ON c.ACCEPTANCE_NO = a.accept_num AND a.DEL_FLAG = '0'
            join claim_ods.bill b
              on b.claim_id = c.id
             and b.delete_flag = '0'
           where c.delete_flag = '0'
             and c.clm_process_status not in ('0', '1')
             
             and substr(c.create_time, 1, 10) >= (CURRENT_DATE - INTERVAL '100' DAY)
           group by c.insure_company_channel,
                    substr(c.create_time, 1,10),
                    a.DEPARTMENT_CODE
        )
        GROUP BY insure_company_channel,create_time
    ),
    a9 as
     (
         SELECT
               sum(明细总数量) 明细总数量,
               insure_company_channel,
               create_time
        FROM
        (
         	select count(distinct t.id) 明细总数量,
                 CASE
                    WHEN c.insure_company_channel = 'CP10' THEN a.DEPARTMENT_CODE
                    ELSE c.insure_company_channel
             	 END insure_company_channel,
                 substr(c.create_time, 1,10) create_time
            from claim_ods.bill_detail t
            join claim_ods.claim c
              on t.claim_id = c.id
            LEFT JOIN claim_ods.accept_list_record a
              ON c.ACCEPTANCE_NO = a.accept_num AND a.DEL_FLAG = '0'
           where c.delete_flag = '0'
             
             and substr(c.create_time, 1, 10) >= (CURRENT_DATE - INTERVAL '100' DAY)
           group by c.insure_company_channel,
                    substr(c.create_time, 1,10),
                        a.DEPARTMENT_CODE
        )
        GROUP BY insure_company_channel,create_time
     ),
    a10 as
     (
         SELECT
               sum(诊断匹配全量) 诊断匹配全量,
               insure_company_channel,
               create_time
        FROM
        (
         	SELECT COUNT(distinct t.id) 诊断匹配全量,
                 CASE
                    WHEN c.insure_company_channel = 'CP10' THEN a.DEPARTMENT_CODE
                    ELSE c.insure_company_channel
             	 END insure_company_channel,
                 substr(t.create_time,1,10) create_time
            FROM claim_ods.bill_diagnose t
            join claim_ods.claim c
              on t.claim_id = c.id
           LEFT JOIN claim_ods.accept_list_record a
              ON c.ACCEPTANCE_NO = a.accept_num AND a.DEL_FLAG = '0'
           where c.delete_flag = '0'
             
             and substr(t.create_time, 1, 10) >= (CURRENT_DATE - INTERVAL '100' DAY)
           group by c.insure_company_channel,
                    substr(t.create_time,1,10),
                            a.DEPARTMENT_CODE
        )
        GROUP BY insure_company_channel,create_time
     ),
    a11 as
     (
         SELECT
               sum(医院匹配总量) 医院匹配总量,
               insure_company_channel,
               create_time
        FROM
        (
         	select count(b.id) 医院匹配总量,
                 CASE
                    WHEN c.insure_company_channel = 'CP10' THEN a.DEPARTMENT_CODE
                    ELSE c.insure_company_channel
             	 END insure_company_channel,
                 substr(c.create_time, 1,10) create_time
            from claim_ods.claim c
            join claim_ods.bill b
              on c.id = b.claim_id
           LEFT JOIN claim_ods.accept_list_record a
              ON c.ACCEPTANCE_NO = a.accept_num AND a.DEL_FLAG = '0'
           where c.clm_process_status not in ('0', '1')
             
             and substr(c.create_time, 1, 10) >= (CURRENT_DATE - INTERVAL '100' DAY)
           group by c.insure_company_channel,
                    substr(c.create_time, 1,10),
                                a.DEPARTMENT_CODE
        )
        GROUP BY insure_company_channel,create_time
     ),
    a12 as
     (
         SELECT
               sum(配置总数) 配置总数,
               sum(config_manual_vol) config_manual_vol,
               insure_company_channel,
               gmt_created
        FROM
        (
         	select count(distinct t.accept_num) 配置总数,
                 sum(case
                       when config_flag = 'config' then
                        '1'
                       else
                        '0'
                     end) config_manual_vol,
                  CASE
                    WHEN t.insure_company_channel = 'CP10' THEN a.DEPARTMENT_CODE
                    ELSE t.insure_company_channel
             	 END insure_company_channel,
                 substr(t.gmt_created,1,10) gmt_created
            from claim_ods.special_config_task t
            LEFT JOIN claim_ods.accept_list_record a
              ON t.accept_num = a.accept_num AND a.DEL_FLAG = '0'
           where t.is_deleted = 'N'
             
             and substr(t.gmt_created, 1, 10) >= (CURRENT_DATE - INTERVAL '100' DAY)
           group by t.insure_company_channel,
                    substr(t.gmt_created,1,10),
                                a.DEPARTMENT_CODE
        )
        GROUP BY insure_company_channel,gmt_created
     )
        select dim.channel_value insure_company_channel,
           a1.create_time,
           coalesce(a1.in_claim, 0) in_claim,
           coalesce(a2.per_manual_claim_vol, 0) per_manual_claim_vol,
           coalesce(a3.hospital_manual_vol, 0) hospital_manual_vol,
           coalesce(a11.医院匹配总量, 0) hospital_match_total,
           coalesce(a4.diag_match_manual_vol, 0) diag_match_manual_vol,
           coalesce(a10.诊断匹配全量, 0) diag_match_total,
           coalesce(a5.detail_manual_match_vol, 0) detail_manual_match_vol,
           coalesce(a9.明细总数量, 0) detail_total,
           coalesce(a6.manual_charge_bill_vol, 0) manual_charge_bill_vol,
           coalesce(a7.manual_charge_vol, 0) manual_charge_vol,
           coalesce(a8.扣费总量, 0) charge_tatal,
           cast(coalesce(a12.config_manual_vol, 0) as bigint) config_manual_vol,
           coalesce(a12.配置总数, 0) config_total,
           replace(a1.create_time,'-','')
      from a1
     inner join claim_ods.dim_insure_company_channel dim
        on a1.insure_company_channel = dim.channel_key
      left join a2
        on a1.insure_company_channel = a2.insure_company_channel
       and a1.create_time = a2.create_time
      left join a3
        on a1.insure_company_channel = a3.insure_company_channel
       and a1.create_time = a3.create_time
      left join a4
        on a1.insure_company_channel = a4.insure_company_channel
       and a1.create_time = a4.create_time
      left join a5
        on a1.insure_company_channel = a5.insure_company_channel
       and a1.create_time = a5.create_time
      left join a6
        on a1.insure_company_channel = a6.insure_company_channel
       and a1.create_time = a6.create_time
      left join a7
        on a1.insure_company_channel = a7.insure_company_channel
       and a1.create_time = a7.create_time
      left join a8
        on a1.insure_company_channel = a8.insure_company_channel
       and a1.create_time = a8.create_time
      left join a9
        on a1.insure_company_channel = a9.insure_company_channel
       and a1.create_time = a9.create_time
      left join a10
        on a1.insure_company_channel = a10.insure_company_channel
       and a1.create_time = a10.create_time
      left join a11
        on a1.insure_company_channel = a11.insure_company_channel
       and a1.create_time = a11.create_time
      left join a12
        on a1.insure_company_channel = a12.insure_company_channel
       and a1.create_time = a12.gmt_created;
"""
def truncate_table(table_name='CLAIM_DWD.DWD_ALL_FLOW_EFF_MONITOR'):
    with DatabaseConnection() as conn:
        truncate_sql = f"DELETE FROM {table_name}  WHERE CREATE_TIME >= (CURRENT_DATE - INTERVAL '100' DAY);"
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
