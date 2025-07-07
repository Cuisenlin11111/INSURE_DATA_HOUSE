import sys
# sys.path.append(r"E:\pycharm\database")
from database import DatabaseConnection
from datetime import datetime



sql_query = f"""
insert into CLAIM_DWD.DWD_ALL_FLOW_TK_MONITOR
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
     charge_total,
     config_manual_vol,
     config_total,
     shzdhl_rw,
     sh_auto_rate,
     fh_auto_rate,
     data_dt)
with a1 as
     (select '泰康养老全渠道' insure_company_channel,
             count(*) in_claim,
             substr(c.create_time,1,10) create_time
        from claim_ods.accept_list_record a
        join claim_ods.claim c
          on a.accept_num = c.acceptance_no
         and c.delete_flag = '0'
       where a.DEL_FLAG = '0'
         and c.delete_flag = '0'
         and c.insure_company_channel like 'TK%'
         and substr(c.create_time, 1, 10) >= (CURRENT_DATE - INTERVAL '100' DAY)  -- 添加时间筛选条件
       group by substr(c.create_time,1,10)),
    a2 as
     (select count(DISTINCT c.id) per_manual_claim_vol,
             substr(c.create_time,1,10) create_time
        from claim_ods.accept_list_record a
        join claim_ods.claim c
          on a.accept_num = c.acceptance_no
         and c.delete_flag = '0'
        join claim_ods.clm_pretrial_examine e
          on c.claim_no = e.claim_app_no
       where c.delete_flag = '0'
         and c.insure_company_channel like 'TK%'
         and substr(c.create_time, 1, 10) >= (CURRENT_DATE - INTERVAL '100' DAY)  -- 添加时间筛选条件
       group by substr(c.create_time,1,10)),
    a3 as
     (select count(t.id) hospital_manual_vol,
             substr(c.create_time,1,10) create_time
        from claim_ods.manual_match_task t
        join claim_ods.claim c
          on t.claim_id = c.id
       where c.delete_flag = '0'
         and c.clm_process_status not in ('0', '1')
         and match_type = 1
         and c.insure_company_channel like 'TK%'
         and substr(c.create_time, 1, 10) >= (CURRENT_DATE - INTERVAL '100' DAY)  -- 添加时间筛选条件
       group by substr(c.create_time,1,10)),
    a4 as
     (select count(distinct t.id) diag_match_manual_vol,
             substr(c.create_time,1,10) create_time
        from claim_ods.manual_match_task t
        join claim_ods.claim c
          on t.claim_id = c.id
       where c.clm_process_status not in ('0', '1')
         and match_type = 2
         and c.delete_flag = '0'
         and c.insure_company_channel like 'TK%'
         and substr(c.create_time, 1, 10) >= (CURRENT_DATE - INTERVAL '100' DAY)  -- 添加时间筛选条件
       group by substr(c.create_time,1,10)),
    a5 as
     (select count(distinct t.id) detail_manual_match_vol,
             substr(c.create_time,1,10) create_time
        from claim_ods.manual_match_task t
        join claim_ods.claim c
          on t.claim_id = c.id
       where c.clm_process_status not in ('0', '1')
         and match_type = 3
         and c.delete_flag = '0'
         and c.insure_company_channel like 'TK%'
         and substr(c.create_time, 1, 10) >= (CURRENT_DATE - INTERVAL '100' DAY)  -- 添加时间筛选条件
       group by substr(c.create_time,1,10)),
    a6 as
     (select count(distinct b.id) manual_charge_bill_vol,
             substr(c.create_time,1,10) create_time
        from claim_ods.claim c
        join claim_ods.deduct_task d
          on c.id = d.claim_id
        left join claim_ods.bill b
          on b.claim_id = c.id
         and b.delete_flag = '0'
       where c.delete_flag = '0'
         and b.reason_notes is not null
         and c.insure_company_channel like 'TK%'
         and substr(c.create_time, 1, 10) >= (CURRENT_DATE - INTERVAL '100' DAY)  -- 添加时间筛选条件
       group by substr(c.create_time,1,10)),
    a7 as
     (select count(DISTINCT c.id) manual_charge_vol,
             substr(c.create_time,1,10) create_time
        from claim_ods.claim c
        join claim_ods.deduct_task d
          on c.id = d.claim_id
        left join claim_ods.bill b
          on b.claim_id = c.id
         and b.delete_flag = '0'
       where c.delete_flag = '0'
         and b.reason_notes is not null
         and c.insure_company_channel like 'TK%'
         and substr(c.create_time, 1, 10) >= (CURRENT_DATE - INTERVAL '100' DAY)  -- 添加时间筛选条件
       group by substr(c.create_time,1,10)),
    a8 as
     (select count(DISTINCT b.id) 扣费总量,
             substr(c.create_time,1,10) create_time
        from claim_ods.claim c
        join claim_ods.bill b
          on b.claim_id = c.id
         and b.delete_flag = '0'
       where c.delete_flag = '0'
         and c.clm_process_status not in ('0', '1')
         and c.insure_company_channel like 'TK%'
         and substr(c.create_time, 1, 10) >= (CURRENT_DATE - INTERVAL '100' DAY)  -- 添加时间筛选条件
       group by substr(c.create_time,1,10)),
    a9 as
     (select count(distinct t.id) 明细总数量,
             substr(c.create_time,1,10) create_time
        from claim_ods.bill_detail t
        join claim_ods.claim c
          on t.claim_id = c.id
       where c.delete_flag = '0'
         and c.insure_company_channel like 'TK%'
         and substr(c.create_time, 1, 10) >= (CURRENT_DATE - INTERVAL '100' DAY)  -- 添加时间筛选条件
       group by substr(c.create_time,1,10)),
    a10 as
     (SELECT COUNT(distinct t.id) 诊断匹配全量,
             substr(t.create_time, 1,10) create_time
        FROM claim_ods.bill_diagnose t
        join claim_ods.claim c
          on t.claim_id = c.id
       where c.delete_flag = '0'
         and c.insure_company_channel like 'TK%'
         and substr(c.create_time, 1, 10) >= (CURRENT_DATE - INTERVAL '100' DAY)  -- 添加时间筛选条件
       group by substr(t.create_time, 1,10)),
    a11 as
     (select count(b.id) 医院匹配总量,
             substr(c.create_time,1,10) create_time
        from claim_ods.claim c
        join claim_ods.bill b
          on c.id = b.claim_id
       where c.clm_process_status not in ('0', '1')
         and c.insure_company_channel like 'TK%'
         and substr(c.create_time, 1, 10) >= (CURRENT_DATE - INTERVAL '100' DAY)  -- 添加时间筛选条件
       group by substr(c.create_time,1,10)),
    a12 as
     (select gmt_created,
             sum(配置总数) 配置总数,
             sum(config_manual_vol) config_manual_vol
        from (select count(id) 配置总数,
                     sum(case
                           when config_flag = 'config' then
                            '1'
                           else
                            '0'
                         end) config_manual_vol,
                     substr(t.gmt_created,1,10) gmt_created
                from claim_ods.special_config_task t
               where t.insure_company_channel like 'TK%'
                 and substr(t.gmt_created, 1, 10) >= (CURRENT_DATE - INTERVAL '100' DAY)  -- 添加时间筛选条件
               group by substr(t.gmt_created,1,10))
       group by gmt_created),
    a13 as
     (
      -- 泰康全渠道 审核自动化率(任务) 单独如下计算
      select a2.datestr,
              case
                when coalesce(a2.totalNum, 0) = 0 then
                 0
                else
                 coalesce(a1.num, 0) / coalesce(a2.totalNum, 0)
              end as 审核自动化率
        from (select substr(cat.T_CRT_TM,1,10) datestr,
                      count(DISTINCT cat.C_CLAIM_CASE_NO) num
                 FROM claim_ods.claim c
                 LEFT JOIN claim_ods.case_audit_task cat
                   on cat.c_claim_case_no = c.claim_no
                  and cat.C_DEL_FLAG = '0'
                  and cat.C_HANDLE_CDE = '1'
                WHERE cat.T_CRT_TM is not null
                  and c.insure_company_channel like 'TK%'
                  and substr(cat.T_CRT_TM, 1, 10) >= (CURRENT_DATE - INTERVAL '100' DAY)  -- 添加时间筛选条件
                group by substr(cat.T_CRT_TM,1,10)) a1
       right join (select substr(cat.T_CRT_TM,1,10) datestr,
                          count(distinct cat.C_CLAIM_CASE_NO) totalNum
                     FROM claim_ods.claim c
                     LEFT JOIN claim_ods.case_audit_task cat
                       on cat.c_claim_case_no = c.claim_no
                    WHERE cat.T_CRT_TM is not null
                      and c.insure_company_channel like 'TK%'
                      and substr(cat.T_CRT_TM, 1, 10) >= (CURRENT_DATE - INTERVAL '100' DAY)  -- 添加时间筛选条件
                    group by substr(cat.T_CRT_TM,1,10)) a2
          on a1.datestr = a2.datestr),
    a133 as
     (
      -- 泰康全渠道 审核自动化率(回传) 单独如下计算
      select d1.comm_date,
              case
                when coalesce(d2.qlchcal, 0) = 0 then
                 0
                else
                 coalesce(d1.zdshal, 0) / coalesce(d2.qlchcal, 0)
              end as 审核自动化率
        from (select count(DISTINCT cat.C_CLAIM_CASE_NO) zdshal,
                      substr(pr.gmt_created,1,10) comm_date
                 FROM claim_ods.postback_record pr
                 LEFT JOIN claim_ods.case_audit_task cat
                   on cat.c_claim_case_no = pr.app_no
                  and cat.C_DEL_FLAG = '0'
                  and cat.C_HANDLE_CDE = '1'
                WHERE pr.back_status in ('2', '21')
                  and pr.is_deleted = 'N'
                  and (pr.postback_way = 'W' or pr.postback_way is null)
                  and pr.insure_company_channel like 'TK%'
                  and substr(pr.gmt_created, 1, 10) >= (CURRENT_DATE - INTERVAL '100' DAY)  -- 添加时间筛选条件
                group by substr(pr.gmt_created,1,10)) d1
        left join (select substr(gmt_created,1,10) comm_date,
                          count(distinct pr.app_no) as qlchcal
                     from claim_ods.postback_record pr
                    where pr.back_status in ('2', '21')
                      and pr.is_deleted = 'N'
                      and (pr.postback_way = 'W' or pr.postback_way is null)
                      and insure_company_channel like 'TK%'
                      and substr(gmt_created, 1, 10) >= (CURRENT_DATE - INTERVAL '100' DAY)  -- 添加时间筛选条件
                    group by substr(gmt_created,1,10)) d2
          on d1.comm_date = d2.comm_date),
    a14 as
     (
      -- 泰康全渠道 复核自动化率 单独如下计算
      select substr(cat.T_CRT_TM,1,10) comm_date,
              count(DISTINCT a.accept_num) fh_auto_claim_count
        from claim_ods.accept_list_record a
        join claim_ods.claim c
          on a.accept_num = c.acceptance_no
         and c.delete_flag = '0'
         and a.insure_company_channel like 'TK%'
        join claim_ods.case_audit_task cat
          on cat.C_CLAIM_CASE_NO = c.claim_no
         and cat.C_DEL_FLAG = '0'
       where cat.C_REVIEWER_STAFF = '系统自动'
         and substr(cat.T_CRT_TM, 1, 10) >= (CURRENT_DATE - INTERVAL '100' DAY)  -- 添加时间筛选条件
       group by substr(cat.T_CRT_TM, 1,10)),
    a15 as
     (select substr(cat.T_CRT_TM, 1,10) comm_date,
             count(distinct cat.C_CLAIM_CASE_NO) qlchcal
        FROM claim_ods.claim c
        LEFT JOIN claim_ods.case_audit_task cat
          on cat.c_claim_case_no = c.claim_no
       WHERE cat.T_CRT_TM is not null
         and c.insure_company_channel like 'TK%'
         and exists (select 1
                from claim_ods.clm_process p
               where p.C_CLAIM_APPLY_NO = c.claim_no
                 and p.C_PROCESS_STATUS in ('7'))
         and substr(cat.T_CRT_TM, 1, 10) >= (CURRENT_DATE - INTERVAL '100' DAY)  -- 添加时间筛选条件
       group by substr(cat.T_CRT_TM, 1,10))
    select a1.insure_company_channel,
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
           cast(coalesce(a13.审核自动化率, 0)  as decimal(10, 4))  shzdhl_rw,
           cast(coalesce(a133.审核自动化率, 0) as decimal(10, 4)) sh_auto_rate,
           cast(case
             when coalesce(a15.qlchcal, 0) = 0 then
              0
             else
              coalesce(a14.fh_auto_claim_count, 0) /
              coalesce(a15.qlchcal, 0)
           end as decimal (10, 4)) as fh_auto_rate,
           replace(a1.create_time,'-','')
      from a1
      left join a2
        on a1.create_time = a2.create_time
      left join a3
        on a1.create_time = a3.create_time
      left join a4
        on a1.create_time = a4.create_time
      left join a5
        on a1.create_time = a5.create_time
      left join a6
        on a1.create_time = a6.create_time
      left join a7
        on a1.create_time = a7.create_time
      left join a8
        on a1.create_time = a8.create_time
      left join a9
        on a1.create_time = a9.create_time
      left join a10
        on a1.create_time = a10.create_time
      left join a11
        on a1.create_time = a11.create_time
      left join a12
        on a1.create_time = a12.gmt_created
      left join a13
        on a1.create_time = a13.datestr
      left join a133
        on a1.create_time = a133.comm_date
      left join a14
        on a1.create_time = a14.comm_date
      left join a15
        on a1.create_time = a15.comm_date;
"""
def truncate_table(table_name='CLAIM_DWD.DWD_ALL_FLOW_TK_MONITOR'):
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
