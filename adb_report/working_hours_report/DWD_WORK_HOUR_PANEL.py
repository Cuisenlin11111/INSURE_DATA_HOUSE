# import sys
# sys.path.append(r"E:\pycharm\database")
from database import DatabaseConnection
from datetime import datetime



sql_query = f"""
  -- @description: 人工工时看板
  -- @param:
  -- @return:
  -- @alter:
  -- @time: 2024-08-29 15:01:06
  -- @author: 01
  -- @version: 1.0.0
#
  insert into CLAIM_DWD.DWD_WORK_HOUR_PANEL
    (insure_company_channel,
     comm_date,
     pre_examine_sjgs,
     hospital_sjgs,
     diagnose_sjgs,
     detail_sjgs,
     charge_sjgs,
     config_sjgs,
     examine_sjgs,
     complex_sjgs,
     pre_examine_wqgs,
     hospital_wqgs,
     diagnose_wqgs,
     detail_wqgs,
     charge_wqgs,
     config_wqgs,
     examine_wqgs,
     complex_wqgs,
     pre_examine_gs,
     hospital_gs,
     diagnose_gs,
     detail_gs,
     charge_gs,
     config_gs,
     examine_gs,
     complex_gs,
     manage_gs,
     data_dt)
WITH t1 AS
     (SELECT insure_company_channel,
             comm_date,
             SUM(cn) / dim.pre_examine_basic AS 预审实际工时
        FROM (SELECT CASE
                       WHEN dim.channel_value LIKE '%泰康养老%' THEN
                        '泰康养老全渠道'
                       ELSE
                        dim.channel_value
                     END AS insure_company_channel,
                     SUBSTR(e.update_time,1,10) AS comm_date,
                     COUNT(DISTINCT e.id) AS cn
                FROM claim_ods.accept_list_record alr
               INNER JOIN claim_ods.claim c
                  ON alr.accept_num = c.acceptance_no
               INNER JOIN claim_ods.clm_pretrial_examine e
                  ON c.claim_no = e.claim_app_no
               INNER JOIN `CLAIM_DIM`.`dim_channel_gs_bisic` dim
                  ON alr.insure_company_channel = dim.channel_key
               WHERE e.app_state = '3'
               GROUP BY dim.channel_value,
                        SUBSTR(e.update_time,1,10))
       INNER JOIN `CLAIM_DIM`.`dim_channel_gs_bisic` dim
          ON insure_company_channel = dim.channel_value
       WHERE insure_company_channel IN ('平安产险', '泰康养老全渠道', '中智')
       GROUP BY insure_company_channel, comm_date, dim.pre_examine_basic),
t2 AS
     (SELECT insure_company_channel,
             comm_date,
             SUM(cn) / dim.hospital_basic AS 医院匹配实际工时
        FROM (SELECT CASE
                       WHEN dim.channel_value LIKE '%泰康养老%' THEN
                        '泰康养老全渠道'
                       ELSE
                        dim.channel_value
                     END AS insure_company_channel,
                     SUBSTR(t.deal_time,1,10) AS comm_date,
                     COUNT(DISTINCT t.id) AS cn
                FROM claim_ods.accept_list_record alr
               INNER JOIN claim_ods.claim c
                  ON alr.accept_num = c.acceptance_no
                 AND c.delete_flag = '0'
                JOIN claim_ods.manual_match_task t
                  ON t.claim_id = c.id
               INNER JOIN `CLAIM_DIM`.`dim_channel_gs_bisic` dim
                  ON alr.insure_company_channel = dim.channel_key
               WHERE alr.accept_status NOT IN ('1', '5')
                 AND c.delete_flag = '0'
                 AND t.match_type IN (1)
                 AND t.match_ower!= 1
               GROUP BY dim.channel_value,
                        SUBSTR(t.deal_time,1,10))
       INNER JOIN `CLAIM_DIM`.`dim_channel_gs_bisic` dim
          ON insure_company_channel = dim.channel_value
       WHERE insure_company_channel IN ('平安产险', '泰康养老全渠道', '中智')
       GROUP BY insure_company_channel, comm_date, dim.hospital_basic),
t3 AS
     (SELECT insure_company_channel,
             comm_date,
             SUM(cn) / dim.diagnose_basic AS 诊断匹配实际工时
        FROM (SELECT CASE
                       WHEN dim.channel_value LIKE '%泰康养老%' THEN
                        '泰康养老全渠道'
                       ELSE
                        dim.channel_value
                     END AS insure_company_channel,
                     SUBSTR(t.deal_time,1,10) AS comm_date,
                     COUNT(DISTINCT t.id) AS cn
                FROM claim_ods.accept_list_record alr
               INNER JOIN claim_ods.claim c
                  ON alr.accept_num = c.acceptance_no
                 AND c.delete_flag = '0'
                JOIN claim_ods.manual_match_task t
                  ON t.claim_id = c.id
               INNER JOIN `CLAIM_DIM`.`dim_channel_gs_bisic` dim
                  ON alr.insure_company_channel = dim.channel_key
               WHERE alr.accept_status NOT IN ('1', '5')
                 AND c.delete_flag = '0'
                 AND t.match_type IN (2)
                 AND t.match_ower!= 1
               GROUP BY dim.channel_value,
                        SUBSTR(t.deal_time,1,10))
       INNER JOIN `CLAIM_DIM`.`dim_channel_gs_bisic` dim
          ON insure_company_channel = dim.channel_value
       WHERE insure_company_channel IN ('平安产险', '泰康养老全渠道', '中智')
       GROUP BY insure_company_channel, comm_date, dim.diagnose_basic),
t4 AS
     (SELECT insure_company_channel,
             comm_date,
             SUM(cn) / dim.detail_basic AS 明细匹配实际工时
        FROM (SELECT CASE
                       WHEN dim.channel_value LIKE '%泰康养老%' THEN
                        '泰康养老全渠道'
                       ELSE
                        dim.channel_value
                     END AS insure_company_channel,
                     SUBSTR(t.deal_time,1,10) AS comm_date,
                     COUNT(DISTINCT t.id) AS cn
                FROM claim_ods.accept_list_record alr
               INNER JOIN claim_ods.claim c
                  ON alr.accept_num = c.acceptance_no
                 AND c.delete_flag = '0'
                JOIN claim_ods.manual_match_task t
                  ON t.claim_id = c.id
               INNER JOIN `CLAIM_DIM`.`dim_channel_gs_bisic` dim
                  ON alr.insure_company_channel = dim.channel_key
               WHERE alr.accept_status NOT IN ('1', '5')
                 AND c.delete_flag = '0'
                 AND t.match_type IN (3)
                 AND t.match_ower!= 1
               GROUP BY dim.channel_value,
                        SUBSTR(t.deal_time,1,10))
       INNER JOIN `CLAIM_DIM`.`dim_channel_gs_bisic` dim
          ON insure_company_channel = dim.channel_value
       WHERE insure_company_channel IN ('平安产险', '泰康养老全渠道', '中智')
       GROUP BY insure_company_channel, comm_date, dim.detail_basic),
t5 AS
     (SELECT insure_company_channel,
             comm_date,
             SUM(cn) / dim.charge_basic AS 扣费实际工时
        FROM (SELECT CASE
                       WHEN dim.channel_value LIKE '%泰康养老%' THEN
                        '泰康养老全渠道'
                       ELSE
                        dim.channel_value
                     END AS insure_company_channel,
                     SUBSTR(d.update_time,1,10) AS comm_date,
                     COUNT(DISTINCT b.id) AS cn
                FROM claim_ods.accept_list_record alr
               INNER JOIN claim_ods.claim c
                  ON alr.accept_num = c.acceptance_no
                 AND c.delete_flag = '0'
               INNER JOIN claim_ods.deduct_task d
                  ON c.id = d.claim_id
               INNER JOIN claim_ods.bill b
                  ON c.id = b.claim_id
               INNER JOIN `CLAIM_DIM`.`dim_channel_gs_bisic` dim
                  ON alr.insure_company_channel = dim.channel_key
               WHERE b.person_flag = 'Y'
                 AND d.deduct_status = 2
               GROUP BY dim.channel_value,
                        SUBSTR(d.update_time,1,10))
       INNER JOIN `CLAIM_DIM`.`dim_channel_gs_bisic` dim
          ON insure_company_channel = dim.channel_value
       WHERE insure_company_channel IN ('平安产险', '泰康养老全渠道', '中智')
       GROUP BY insure_company_channel, comm_date, dim.charge_basic),
t6 AS
     (SELECT insure_company_channel,
             comm_date,
             SUM(cn) / dim.config_basic AS 配置实际工时
        FROM (SELECT CASE
                       WHEN dim.channel_value LIKE '%泰康养老%' THEN
                        '泰康养老全渠道'
                       ELSE
                        dim.channel_value
                     END AS insure_company_channel,
                     SUBSTR(t.gmt_modified,1,10) AS comm_date,
                     COUNT(DISTINCT t.id) AS cn
                FROM claim_ods.special_config_task t
               INNER JOIN `CLAIM_DIM`.`dim_channel_gs_bisic` dim
                  ON t.insure_company_channel = dim.channel_key
               WHERE t.config_flag = 'config'
               GROUP BY dim.channel_value,
                        SUBSTR(t.gmt_modified,1,10))
       INNER JOIN `CLAIM_DIM`.`dim_channel_gs_bisic` dim
          ON insure_company_channel = dim.channel_value
       WHERE insure_company_channel IN ('平安产险', '泰康养老全渠道', '中智')
       GROUP BY insure_company_channel, comm_date, dim.config_basic),
t7 AS
     (SELECT insure_company_channel,
             comm_date,
             SUM(cn) / dim.examine_basic AS 审核实际工时
        FROM (SELECT CASE
                       WHEN dim.channel_value LIKE '%泰康养老%' THEN
                        '泰康养老全渠道'
                       ELSE
                        dim.channel_value
                     END AS insure_company_channel,
                     SUBSTR(cat.T_AUDIT_END_TM,1,10) AS comm_date,
                     COUNT(DISTINCT c.id) AS cn
                FROM claim_ods.claim c
                LEFT JOIN claim_ods.case_audit_task cat
                  ON cat.C_CLAIM_CASE_NO = c.claim_no
                 AND cat.C_DEL_FLAG = '0'
                 AND cat.C_HANDLE_CDE!= '1'
               INNER JOIN `CLAIM_DIM`.`dim_channel_gs_bisic` dim
                  ON c.insure_company_channel = dim.channel_key
               WHERE cat.T_AUDIT_END_TM IS NOT NULL
               GROUP BY dim.channel_value,
                        SUBSTR(cat.T_AUDIT_END_TM,1,10))
       INNER JOIN `CLAIM_DIM`.`dim_channel_gs_bisic` dim
          ON insure_company_channel = dim.channel_value
       WHERE insure_company_channel IN ('平安产险', '泰康养老全渠道', '中智')
       GROUP BY insure_company_channel, comm_date, dim.examine_basic),
    t8 as
     (select insure_company_channel,
             comm_date,
             sum(cn) / dim.complex_basic 复核实际工时
        from (select case
                       when dim.channel_value like '%泰康养老%' then
                        '泰康养老全渠道'
                       else
                        dim.channel_value
                     end as insure_company_channel,
                     substr(cat.T_CLOSING_CASE_TM,1,10) comm_date,
                     count(DISTINCT cat.id) cn
                from claim_ods.case_audit_task cat
               inner join `CLAIM_DIM`.`dim_channel_gs_bisic` dim
                  on cat.insure_company_channel = dim.channel_key
               where cat.C_REVIEWER_STAFF!= '系统自动'
               group by dim.channel_value,
                        substr(cat.T_CLOSING_CASE_TM,1,10))
       inner join `CLAIM_DIM`.`dim_channel_gs_bisic` dim
          on insure_company_channel = dim.channel_value
       where insure_company_channel in ('平安产险', '泰康养老全渠道', '中智')
       group by insure_company_channel, comm_date, dim.complex_basic),
t9 as
     (select insure_company_channel,
             comm_date,
             sum(cn) / dim.pre_examine_basic 预审完全工时
        from (select case
                       when dim.channel_value like '%泰康养老%' then
                        '泰康养老全渠道'
                       else
                        dim.channel_value
                     end as insure_company_channel,
                     substr(e.create_time,1,10) comm_date,
                     count(distinct e.id) cn
                from claim_ods.accept_list_record alr
                join claim_ods.claim c
                  on alr.accept_num = c.acceptance_no
                 and c.delete_flag = '0'
                join claim_ods.clm_pretrial_examine e
                  on c.claim_no = e.claim_app_no
               inner join `CLAIM_DIM`.`dim_channel_gs_bisic` dim
                  on c.insure_company_channel = dim.channel_key
               where 1 = 1
               group by dim.channel_value,
                        substr(e.create_time,1,10))
       inner join `CLAIM_DIM`.`dim_channel_gs_bisic` dim
          on insure_company_channel = dim.channel_value
       where insure_company_channel in ('平安产险', '泰康养老全渠道', '中智')
       group by insure_company_channel, comm_date, dim.pre_examine_basic),
t10 as
     (select insure_company_channel,
             comm_date,
             sum(cn) / dim.hospital_basic 医院匹配完全工时
        from (select case
                       when dim.channel_value like '%泰康养老%' then
                        '泰康养老全渠道'
                       else
                        dim.channel_value
                     end as insure_company_channel,
                     substr(t.create_time,1,10) comm_date,
                     count(distinct t.id) cn
                from claim_ods.accept_list_record alr
               inner join claim_ods.claim c
                  on alr.accept_num = c.acceptance_no
                 and c.delete_flag = '0'
                join claim_ods.manual_match_task t
                  on t.claim_id = c.id
               inner join `CLAIM_DIM`.`dim_channel_gs_bisic` dim
                  on alr.insure_company_channel = dim.channel_key
               WHERE alr.accept_status not in ('1', '5')
                 and c.delete_flag = '0'
                 and t.match_type in (1)
                 and t.match_ower!= 1
               group by dim.channel_value,
                        substr(t.create_time,1,10))
       inner join `CLAIM_DIM`.`dim_channel_gs_bisic` dim
          on insure_company_channel = dim.channel_value
       where insure_company_channel in ('平安产险', '泰康养老全渠道', '中智')
       group by insure_company_channel, comm_date, dim.hospital_basic),
t11 as
     (select insure_company_channel,
             comm_date,
             sum(cn) / dim.diagnose_basic 诊断匹配完全工时
        from (select case
                       when dim.channel_value like '%泰康养老%' then
                        '泰康养老全渠道'
                       else
                        dim.channel_value
                     end as insure_company_channel,
                     substr(t.create_time,1,10) comm_date,
                     count(distinct t.id) cn
                from claim_ods.accept_list_record alr
               inner join claim_ods.claim c
                  on alr.accept_num = c.acceptance_no
                 and c.delete_flag = '0'
                join claim_ods.manual_match_task t
                  on t.claim_id = c.id
               inner join `CLAIM_DIM`.`dim_channel_gs_bisic` dim
                  on alr.insure_company_channel = dim.channel_key
               WHERE alr.accept_status not in ('1', '5')
                 and c.delete_flag = '0'
                 and t.match_type in (2)
                 and t.match_ower!= 1
               group by dim.channel_value,
                        substr(t.create_time,1,10))
       inner join `CLAIM_DIM`.`dim_channel_gs_bisic` dim
          on insure_company_channel = dim.channel_value
       where insure_company_channel in ('平安产险', '泰康养老全渠道', '中智')
       group by insure_company_channel, comm_date, dim.diagnose_basic),
t12 as
     (select insure_company_channel,
             comm_date,
             sum(cn) / dim.detail_basic 明细匹配完全工时
        from (select case
                       when dim.channel_value like '%泰康养老%' then
                        '泰康养老全渠道'
                       else
                        dim.channel_value
                     end as insure_company_channel,
                     substr(t.create_time,1,10) comm_date,
                     count(distinct t.id) cn
                from claim_ods.accept_list_record alr
               inner join claim_ods.claim c
                  on alr.accept_num = c.acceptance_no
                 and c.delete_flag = '0'
                join claim_ods.manual_match_task t
                  on t.claim_id = c.id
               inner join `CLAIM_DIM`.`dim_channel_gs_bisic` dim
                  on alr.insure_company_channel = dim.channel_key
               WHERE alr.accept_status not in ('1', '5')
                 and c.delete_flag = '0'
                 and t.match_type in (3)
                 and t.match_ower!= 1
               group by dim.channel_value,
                        substr(t.create_time,1,10))
       inner join `CLAIM_DIM`.`dim_channel_gs_bisic` dim
          on insure_company_channel = dim.channel_value
       where insure_company_channel in ('平安产险', '泰康养老全渠道', '中智')
       group by insure_company_channel, comm_date, dim.detail_basic),
t13 as
     (select insure_company_channel,
             comm_date,
             sum(cn) / dim.charge_basic 扣费完全工时
        from (select case
                       when dim.channel_value like '%泰康养老%' then
                        '泰康养老全渠道'
                       else
                        dim.channel_value
                     end as insure_company_channel,
                     substr(d.create_time,1,10) comm_date,
                     count(distinct b.id) cn
                from claim_ods.accept_list_record alr
               inner join claim_ods.claim c
                  on alr.accept_num = c.acceptance_no
                 and c.delete_flag = '0'
               inner join claim_ods.deduct_task d
                  on c.id = d.claim_id
               inner join claim_ods.bill b
                  on c.id = b.claim_id
               inner join `CLAIM_DIM`.`dim_channel_gs_bisic` dim
                  on alr.insure_company_channel = dim.channel_key
               where b.person_flag = 'Y'
               group by dim.channel_value,
                        substr(d.create_time,1,10))
       inner join `CLAIM_DIM`.`dim_channel_gs_bisic` dim
          on insure_company_channel = dim.channel_value
       where insure_company_channel in ('平安产险', '泰康养老全渠道', '中智')
       group by insure_company_channel, comm_date, dim.charge_basic),
t14 as
     (select insure_company_channel,
             comm_date,
             sum(cn) / dim.config_basic 配置完全工时
        from (select case
                       when dim.channel_value like '%泰康养老%' then
                        '泰康养老全渠道'
                       else
                        dim.channel_value
                     end as insure_company_channel,
                     substr(t.gmt_created,1,10) comm_date,
                     count(distinct t.id) cn
                from claim_ods.special_config_task t
               inner join `CLAIM_DIM`.`dim_channel_gs_bisic` dim
                  on t.insure_company_channel = dim.channel_key
               where t.config_flag = 'config'
               group by dim.channel_value,
                        substr(t.gmt_created,1,10))
       inner join `CLAIM_DIM`.`dim_channel_gs_bisic` dim
          on insure_company_channel = dim.channel_value
       where insure_company_channel in ('平安产险', '泰康养老全渠道', '中智')
       group by insure_company_channel, comm_date, dim.config_basic),
t15 as
     (select insure_company_channel,
             comm_date,
             sum(cn) / dim.examine_basic 审核完全工时
        from (select case
                       when dim.channel_value like '%泰康养老%' then
                        '泰康养老全渠道'
                       else
                        dim.channel_value
                     end as insure_company_channel,
                     substr(cat.T_CRT_TM,1,10) comm_date,
                     count(DISTINCT c.id) cn
                FROM claim_ods.claim c
                LEFT JOIN claim_ods.case_audit_task cat
                  on cat.C_CLAIM_CASE_NO = c.claim_no
                 and cat.C_HANDLE_CDE!= '1'
               inner join `CLAIM_DIM`.`dim_channel_gs_bisic` dim
                  on c.insure_company_channel = dim.channel_key
               WHERE 1 = 1
               group by dim.channel_value,
                        substr(cat.T_CRT_TM,1,10))
       inner join `CLAIM_DIM`.`dim_channel_gs_bisic` dim
          on insure_company_channel = dim.channel_value
       where insure_company_channel in ('平安产险', '泰康养老全渠道', '中智')
       group by insure_company_channel, comm_date, dim.examine_basic),
    t16 as
     (select insure_company_channel,
             comm_date,
             sum(cn) / dim.complex_basic 复核完全工时
        from (select case
                       when dim.channel_value like '%泰康养老%' then
                        '泰康养老全渠道'
                       else
                        dim.channel_value
                     end as insure_company_channel,
                     substr(cat.T_CLOSING_CASE_TM,1,10) comm_date,
                     count(DISTINCT c.id) cn
                from claim_ods.claim c
                JOIN claim_ods.case_audit_task cat
                  on cat.C_CLAIM_CASE_NO = c.claim_no
               inner join `CLAIM_DIM`.`dim_channel_gs_bisic` dim
                  on c.insure_company_channel = dim.channel_key
               WHERE cat.C_REVIEWER_STAFF!= '系统自动'
               group by dim.channel_value,
                        substr(cat.T_CLOSING_CASE_TM,1,10))
       inner join `CLAIM_DIM`.`dim_channel_gs_bisic` dim
          on insure_company_channel = dim.channel_value
       where insure_company_channel in ('平安产险', '泰康养老全渠道', '中智')
       group by insure_company_channel, comm_date, dim.complex_basic)
      SELECT insure_company_channel,
       comm_date,
       CAST(sum(预审实际工时) AS DECIMAL(10,2)) pre_examine_sjgs,
       CAST(sum(医院匹配实际工时) AS DECIMAL(10,2)) hospital_sjgs,
       CAST(sum(诊断匹配实际工时) AS DECIMAL(10,2)) diagnose_sjgs,
       CAST(sum(明细匹配实际工时) AS DECIMAL(10,2)) detail_sjgs,
       CAST(sum(扣费实际工时) AS DECIMAL(10,2)) charge_sjgs,
       CAST(sum(配置实际工时) AS DECIMAL(10,2)) config_sjgs,
       CAST(sum(审核实际工时) AS DECIMAL(10,2)) examine_sjgs,
       CAST(sum(复核实际工时) AS DECIMAL(10,2)) complex_sjgs,
       CAST(sum(预审完全工时) AS DECIMAL(10,2)) pre_examine_wqgs,
       CAST(sum(医院匹配完全工时) AS DECIMAL(10,2)) hospital_wqgs,
       CAST(sum(诊断匹配完全工时) AS DECIMAL(10,2)) diagnose_wqgs,
       CAST(sum(明细匹配完全工时) AS DECIMAL(10,2)) detail_wqgs,
       CAST(sum(扣费完全工时) AS DECIMAL(10,2)) charge_wqgs,
       CAST(sum(配置完全工时) AS DECIMAL(10,2)) config_wqgs,
       CAST(sum(审核完全工时) AS DECIMAL(10,2)) examine_wqgs,
       CAST(sum(复核完全工时) AS DECIMAL(10,2)) complex_wqgs,
       dim.pre_examine_gs,
       dim.hospital_gs,
       dim.diagnose_gs,
       dim.detail_gs,
       dim.charge_gs,
       dim.config_gs,
       dim.examine_gs,
       dim.complex_gs,
       dim.manage_gs,
       replace(comm_date,'-','')
      from (select insure_company_channel,
                   comm_date,
                   预审实际工时,
                   0                      as 医院匹配实际工时,
                   0                      as 诊断匹配实际工时,
                   0                      as 明细匹配实际工时,
                   0                      as 扣费实际工时,
                   0                      as 配置实际工时,
                   0                      as 审核实际工时,
                   0                      as 复核实际工时,
                   0                      as 预审完全工时,
                   0                      as 医院匹配完全工时,
                   0                      as 诊断匹配完全工时,
                   0                      as 明细匹配完全工时,
                   0                      as 扣费完全工时,
                   0                      as 配置完全工时,
                   0                      as 审核完全工时,
                   0                      as 复核完全工时
              from t1
            union all
            select insure_company_channel,
                   comm_date,
                   0                      as 预审实际工时,
                   医院匹配实际工时,
                   0                      as 诊断匹配实际工时,
                   0                      as 明细匹配实际工时,
                   0                      as 扣费实际工时,
                   0                      as 配置实际工时,
                   0                      as 审核实际工时,
                   0                      as 复核实际工时,
                   0                      as 预审完全工时,
                   0                      as 医院匹配完全工时,
                   0                      as 诊断匹配完全工时,
                   0                      as 明细匹配完全工时,
                   0                      as 扣费完全工时,
                   0                      as 配置完全工时,
                   0                      as 审核完全工时,
                   0                      as 复核完全工时
              from t2
            union all
            select insure_company_channel,
                   comm_date,
                   0                      as 预审实际工时,
                   0                      as 医院匹配实际工时,
                   诊断匹配实际工时,
                   0                      as 明细匹配实际工时,
                   0                      as 扣费实际工时,
                   0                      as 配置实际工时,
                   0                      as 审核实际工时,
                   0                      as 复核实际工时,
                   0                      as 预审完全工时,
                   0                      as 医院匹配完全工时,
                   0                      as 诊断匹配完全工时,
                   0                      as 明细匹配完全工时,
                   0                      as 扣费完全工时,
                   0                      as 配置完全工时,
                   0                      as 审核完全工时,
                   0                      as 复核完全工时
              from t3
            union all
            select insure_company_channel,
                   comm_date,
                   0                      as 预审实际工时,
                   0                      as 医院匹配实际工时,
                   0                      as 诊断匹配实际工时,
                   明细匹配实际工时,
                   0                      as 扣费实际工时,
                   0                      as 配置实际工时,
                   0                      as 审核实际工时,
                   0                      as 复核实际工时,
                   0                      as 预审完全工时,
                   0                      as 医院匹配完全工时,
                   0                      as 诊断匹配完全工时,
                   0                      as 明细匹配完全工时,
                   0                      as 扣费完全工时,
                   0                      as 配置完全工时,
                   0                      as 审核完全工时,
                   0                      as 复核完全工时
              from t4
            union all
            select insure_company_channel,
                   comm_date,
                   0                      as 预审实际工时,
                   0                      as 医院匹配实际工时,
                   0                      as 诊断匹配实际工时,
                   0                      as 明细匹配实际工时,
                   扣费实际工时,
                   0                      as 配置实际工时,
                   0                      as 审核实际工时,
                   0                      as 复核实际工时,
                   0                      as 预审完全工时,
                   0                      as 医院匹配完全工时,
                   0                      as 诊断匹配完全工时,
                   0                      as 明细匹配完全工时,
                   0                      as 扣费完全工时,
                   0                      as 配置完全工时,
                   0                      as 审核完全工时,
                   0                      as 复核完全工时
              from t5
            union all
            select insure_company_channel,
                   comm_date,
                   0                      as 预审实际工时,
                   0                      as 医院匹配实际工时,
                   0                      as 诊断匹配实际工时,
                   0                      as 明细匹配实际工时,
                   0                      as 扣费实际工时,
                   配置实际工时,
                   0                      as 审核实际工时,
                   0                      as 复核实际工时,
                   0                      as 预审完全工时,
                   0                      as 医院匹配完全工时,
                   0                      as 诊断匹配完全工时,
                   0                      as 明细匹配完全工时,
                   0                      as 扣费完全工时,
                   0                      as 配置完全工时,
                   0                      as 审核完全工时,
                   0                      as 复核完全工时
              from t6
            union all
            select insure_company_channel,
                   comm_date,
                   0                      as 预审实际工时,
                   0                      as 医院匹配实际工时,
                   0                      as 诊断匹配实际工时,
                   0                      as 明细匹配实际工时,
                   0                      as 扣费实际工时,
                   0                      as 配置实际工时,
                   审核实际工时,
                   0                      as 复核实际工时,
                   0                      as 预审完全工时,
                   0                      as 医院匹配完全工时,
                   0                      as 诊断匹配完全工时,
                   0                      as 明细匹配完全工时,
                   0                      as 扣费完全工时,
                   0                      as 配置完全工时,
                   0                      as 审核完全工时,
                   0                      as 复核完全工时
              from t7
            union all
            select insure_company_channel,
                   comm_date,
                   0                      as 预审实际工时,
                   0                      as 医院匹配实际工时,
                   0                      as 诊断匹配实际工时,
                   0                      as 明细匹配实际工时,
                   0                      as 扣费实际工时,
                   0                      as 配置实际工时,
                   0                      as 审核实际工时,
                   复核实际工时,
                   0                      as 预审完全工时,
                   0                      as 医院匹配完全工时,
                   0                      as 诊断匹配完全工时,
                   0                      as 明细匹配完全工时,
                   0                      as 扣费完全工时,
                   0                      as 配置完全工时,
                   0                      as 审核完全工时,
                   0                      as 复核完全工时
              from t8
            union all
            select insure_company_channel,
                   comm_date,
                   0                      as 预审实际工时,
                   0                      as 医院匹配实际工时,
                   0                      as 诊断匹配实际工时,
                   0                      as 明细匹配实际工时,
                   0                      as 扣费实际工时,
                   0                      as 配置实际工时,
                   0                      as 审核实际工时,
                   0                      as 复核实际工时,
                   预审完全工时,
                   0                      as 医院匹配完全工时,
                   0                      as 诊断匹配完全工时,
                   0                      as 明细匹配完全工时,
                   0                      as 扣费完全工时,
                   0                      as 配置完全工时,
                   0                      as 审核完全工时,
                   0                      as 复核完全工时
              from t9
            union all
            select insure_company_channel,
                   comm_date,
                   0                      as 预审实际工时,
                   0                      as 医院匹配实际工时,
                   0                      as 诊断匹配实际工时,
                   0                      as 明细匹配实际工时,
                   0                      as 扣费实际工时,
                   0                      as 配置实际工时,
                   0                      as 审核实际工时,
                   0                      as 复核实际工时,
                   0                      as 预审完全工时,
                   医院匹配完全工时,
                   0                      as 诊断匹配完全工时,
                   0                      as 明细匹配完全工时,
                   0                      as 扣费完全工时,
                   0                      as 配置完全工时,
                   0                      as 审核完全工时,
                   0                      as 复核完全工时
              from t10
            union all
            select insure_company_channel,
                   comm_date,
                   0                      as 预审实际工时,
                   0                      as 医院匹配实际工时,
                   0                      as 诊断匹配实际工时,
                   0                      as 明细匹配实际工时,
                   0                      as 扣费实际工时,
                   0                      as 配置实际工时,
                   0                      as 审核实际工时,
                   0                      as 复核实际工时,
                   0                      as 预审完全工时,
                   0                      as 医院匹配完全工时,
                   诊断匹配完全工时,
                   0                      as 明细匹配完全工时,
                   0                      as 扣费完全工时,
                   0                      as 配置完全工时,
                   0                      as 审核完全工时,
                   0                      as 复核完全工时
              from t11
            union all
            select insure_company_channel,
                   comm_date,
                   0                      as 预审实际工时,
                   0                      as 医院匹配实际工时,
                   0                      as 诊断匹配实际工时,
                   0                      as 明细匹配实际工时,
                   0                      as 扣费实际工时,
                   0                      as 配置实际工时,
                   0                      as 审核实际工时,
                   0                      as 复核实际工时,
                   0                      as 预审完全工时,
                   0                      as 医院匹配完全工时,
                   0                      as 诊断匹配完全工时,
                   明细匹配完全工时,
                   0                      as 扣费完全工时,
                   0                      as 配置完全工时,
                   0                      as 审核完全工时,
                   0                      as 复核完全工时
              from t12
            union all
            select insure_company_channel,
                   comm_date,
                   0                      as 预审实际工时,
                   0                      as 医院匹配实际工时,
                   0                      as 诊断匹配实际工时,
                   0                      as 明细匹配实际工时,
                   0                      as 扣费实际工时,
                   0                      as 配置实际工时,
                   0                      as 审核实际工时,
                   0                      as 复核实际工时,
                   0                      as 预审完全工时,
                   0                      as 医院匹配完全工时,
                   0                      as 诊断匹配完全工时,
                   0                      as 明细匹配完全工时,
                   扣费完全工时,
                   0                      as 配置完全工时,
                   0                      as 审核完全工时,
                   0                      as 复核完全工时
              from t13
            union all
            select insure_company_channel,
                   comm_date,
                   0                      as 预审实际工时,
                   0                      as 医院匹配实际工时,
                   0                      as 诊断匹配实际工时,
                   0                      as 明细匹配实际工时,
                   0                      as 扣费实际工时,
                   0                      as 配置实际工时,
                   0                      as 审核实际工时,
                   0                      as 复核实际工时,
                   0                      as 预审完全工时,
                   0                      as 医院匹配完全工时,
                   0                      as 诊断匹配完全工时,
                   0                      as 明细匹配完全工时,
                   0                      as 扣费完全工时,
                   配置完全工时,
                   0                      as 审核完全工时,
                   0                      as 复核完全工时
              from t14
            union all
            select insure_company_channel,
                   comm_date,
                   0                      as 预审实际工时,
                   0                      as 医院匹配实际工时,
                   0                      as 诊断匹配实际工时,
                   0                      as 明细匹配实际工时,
                   0                      as 扣费实际工时,
                   0                      as 配置实际工时,
                   0                      as 审核实际工时,
                   0                      as 复核实际工时,
                   0                      as 预审完全工时,
                   0                      as 医院匹配完全工时,
                   0                      as 诊断匹配完全工时,
                   0                      as 明细匹配完全工时,
                   0                      as 扣费完全工时,
                   0                      as 配置完全工时,
                   审核完全工时,
                   0                      as 复核完全工时
              from t15
            union all
            select insure_company_channel,
                   comm_date,
                   0                      as 预审实际工时,
                   0                      as 医院匹配实际工时,
                   0                      as 诊断匹配实际工时,
                   0                      as 明细匹配实际工时,
                   0                      as 扣费实际工时,
                   0                      as 配置实际工时,
                   0                      as 审核实际工时,
                   0                      as 复核实际工时,
                   0                      as 预审完全工时,
                   0                      as 医院匹配完全工时,
                   0                      as 诊断匹配完全工时,
                   0                      as 明细匹配完全工时,
                   0                      as 扣费完全工时,
                   0                      as 配置完全工时,
                   0                      as 审核完全工时,
                   复核完全工时
              from t16)
     inner join `CLAIM_DIM`.`dim_channel_gs_bisic` dim
        on insure_company_channel = dim.channel_value
     group by insure_company_channel,
              comm_date,
              dim.pre_examine_gs,
              dim.hospital_gs,
              dim.diagnose_gs,
              dim.detail_gs,
              dim.charge_gs,
              dim.config_gs,
              dim.examine_gs,
              dim.complex_gs,
              dim.manage_gs;


"""
def truncate_table(table_name='CLAIM_DWD.DWD_WORK_HOUR_PANEL'):
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
