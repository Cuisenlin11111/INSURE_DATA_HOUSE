import sys
#sys.path.append(r"E:\pycharm\database")
from database import DatabaseConnection
from datetime import datetime



sql_query = f"""
  -- @description: 审核自动化率保单
  -- @param:
  -- @return:
  -- @alter:
  -- @time: 2024-09-24 15:01:06
  -- @author: 01
  -- @version: 1.0.0
insert into CLAIM_DWD.DWD_EXAM_AUTO_POLICY
    (insure_company_channel,  -- 渠道
     comm_date,               -- 时间
     policy_no,               -- 保单号
     is_policy_auto,          -- 是否开启保单自动化
     auto_exam_vol,           -- 自动审核案件量
     all_flow_hcvol,          -- 全部审核案件量
     policy_crt_tm,           -- 保单创建时间
     early_claim_date,        -- 最早案件时间
     near_claim_date,         -- 最近案件时间
     data_dt)                 -- 调度日期

     with a1 as
     (
     	SELECT
			insure_company_channel,
			comm_date,
			保单号,
			sum(自动审核案件量) 自动审核案件量
		FROM
		(
	     	select
	     		 CASE
					WHEN c.insure_company_channel = 'CP10' THEN alro.DEPARTMENT_CODE
					ELSE c.insure_company_channel
				 END insure_company_channel,
	             substr(cat.T_AUDIT_END_TM, 1,10) comm_date,
	             cc.group_policy_no 保单号,
	             count(DISTINCT c.id) 自动审核案件量
	        FROM claim_ods.case_audit_task cat
	        join claim_ods.claim c
	          on cat.C_CLAIM_CASE_NO = c.claim_no
	         and c.delete_flag = '0'
	         and cat.C_DEL_FLAG = '0'
	         and cat.C_HANDLE_CDE = '1'
	        LEFT JOIN claim_ods.ACCEPT_LIST_RECORD alro
			ON c.ACCEPTANCE_NO = alro.accept_num AND alro.DEL_FLAG = '0'
	        join claim_ods.apply_claim ac
	          on c.claim_no = ac.apply_no
	         and ac.claim_status = '1'
	         and ac.delete_flag = '0'
	        join claim_ods.claim_policy cc
	          on ac.policy_part_no = cc.policy_no
	         and cc.is_deleted = 'N'
	       WHERE 1 = 1
	       GROUP BY c.insure_company_channel,
	                cc.group_policy_no,
	                substr(cat.T_AUDIT_END_TM,1,10),
	                alro.DEPARTMENT_CODE
       )
       GROUP BY insure_company_channel,comm_date,保单号
     ),
    a2 as
     (
     	select insure_company_channel,
             comm_date,
             保单号,
             是否开启保单自动化,
             sum(全流程回传案件量) 全流程回传案件量,
             保单创建时间,
             最早案件时间,
             最近案件时间
        from (select
        			CASE
						WHEN c.insure_company_channel = 'CP10' THEN alro.DEPARTMENT_CODE
						ELSE c.insure_company_channel
				 	END insure_company_channel,
                     substr(cat.T_AUDIT_END_TM,1,10) comm_date,
                     cc.group_policy_no 保单号,
                     (select (case
                               when p.is_open_auto_audit = 'Y' then
                                '已开启'
                               else
                                ''
                             end)
                        from claim_ods.ply p
                       where p.C_CUSTOM_PLY_NO = cc.channel_group_policy_no) 是否开启保单自动化,
                     count(DISTINCT c.id) 全流程回传案件量,
                     (select p.T_CRT_TM
                        from claim_ods.ply p
                       where p.C_PLY_NO = cc.group_policy_no) 保单创建时间,
                     '' 最早案件时间,
                     '' 最近案件时间
                FROM claim_ods.case_audit_task cat
                join claim_ods.claim c
                  on cat.C_CLAIM_CASE_NO = c.claim_no
                 and c.delete_flag = '0'
                 and cat.C_DEL_FLAG = '0'
                LEFT JOIN claim_ods.ACCEPT_LIST_RECORD alro
				ON c.ACCEPTANCE_NO = alro.accept_num AND alro.DEL_FLAG = '0'
                join claim_ods.apply_claim ac
                  on c.claim_no = ac.apply_no
                 and ac.claim_status = '1'
                 and ac.delete_flag = '0'
                join claim_ods.claim_policy cc
                  on ac.policy_part_no = cc.policy_no
                 and cc.is_deleted = 'N'
               WHERE 1 = 1
               GROUP BY c.insure_company_channel,
                        substr(cat.T_AUDIT_END_TM, 1,10),
                        cc.group_policy_no,
                        cc.channel_group_policy_no,
                        alro.DEPARTMENT_CODE
               )
       group by insure_company_channel,
                comm_date,
                保单号,
                是否开启保单自动化,
                保单创建时间,
                最早案件时间,
                最近案件时间
    ),
    a3 as
     (select c.insure_company_channel,
             substr(cat.T_AUDIT_END_TM, 1,10) comm_date,
             cc.group_policy_no 保单号,
             count(DISTINCT c.id) 自动审核案件量
        FROM claim_ods.case_audit_task cat
        join claim_ods.claim c
          on cat.C_CLAIM_CASE_NO = c.claim_no
         and c.delete_flag = '0'
         and cat.C_DEL_FLAG = '0'
         and cat.C_SUB_STATUS = '79'
        join claim_ods.apply_claim ac
          on c.claim_no = ac.apply_no
         and ac.claim_status = '1'
         and ac.delete_flag = '0'
        join claim_ods.claim_policy cc
          on ac.policy_part_no = cc.policy_no
         and cc.is_deleted = 'N'
       WHERE c.insure_company_channel like 'CP01'
         and cat.C_HANDLE_CDE = '1'
         and cc.channel_type = 'M'
       GROUP BY c.insure_company_channel,
                cc.group_policy_no,
                substr(cat.T_AUDIT_END_TM,1,10) ),
    a4 as
     (select 保单号,
             comm_date,
             是否开启保单自动化,
             sum(全流程回传案件量) 全流程回传案件量,
             保单创建时间,
             最早案件时间,
             最近案件时间
        from (select cc.group_policy_no 保单号,
                     substr(cat.T_AUDIT_END_TM,1,10) comm_date,
                     (select (case
                               when p.is_open_auto_audit = 'Y' then
                                '已开启'
                               else
                                ''
                             end)
                        from claim_ods.ply p
                       where p.C_CUSTOM_PLY_NO = cc.channel_group_policy_no) 是否开启保单自动化,
                     count(DISTINCT c.id) 全流程回传案件量,
                     (select p.T_CRT_TM
                        from claim_ods.ply p
                       where p.C_PLY_NO = cc.group_policy_no) 保单创建时间,
                     '' 最早案件时间,
                     '' 最近案件时间
                FROM claim_ods.claim c
                JOIN claim_ods.case_audit_task cat
                  on cat.C_CLAIM_CASE_NO = c.claim_no
                 and cat.C_DEL_FLAG = '0'
                 and cat.C_SUB_STATUS = '79'
                 and c.delete_flag = '0'
                join claim_ods.apply_claim ac
                  on c.claim_no = ac.apply_no
                 and ac.claim_status = '1'
                 and ac.delete_flag = '0'
                join claim_ods.claim_policy cc
                  on ac.policy_part_no = cc.policy_no
                 and cc.is_deleted = 'N'
               WHERE c.insure_company_channel like 'CP01'
                 and cc.channel_type = 'M'
               GROUP BY cc.group_policy_no,
                        substr(cat.T_AUDIT_END_TM,1,10),
                        cc.channel_group_policy_no)
       GROUP BY 保单号,
                comm_date,
                是否开启保单自动化,
                保单创建时间,
                最早案件时间,
                最近案件时间)
    select 渠道             as insure_company_channel,
           comm_date,
           保单号           as policy_no,
           is_policy_auto,
           自动审核案件量   auto_exam_vol,
           全流程回传案件量 all_flow_hcvol,
           保单创建时间     policy_crt_tm,
           最早案件时间     early_claim_date,
           最近案件时间     near_claim_date,
           replace(comm_date,'-','')
      from (select case
                     when a2.insure_company_channel = 'TK01' then
                      '泰康养老上海分公司'
                     when a2.insure_company_channel = 'TK02' then
                      '泰康养老山东分公司'
                     when a2.insure_company_channel = 'TK03' then
                      '泰康养老浙江分公司'
                     when a2.insure_company_channel = 'TK04' then
                      '泰康养老北京分公司'
                     when a2.insure_company_channel = 'TK05' then
                      '泰康养老重庆分公司'
                     when a2.insure_company_channel = 'TK06' then
                      '泰康养老河南分公司'
                     when a2.insure_company_channel = 'TK07' then
                      '泰康养老广东分公司'
                     when a2.insure_company_channel = 'TK08' then
                      '泰康养老厦门分公司'
                     else
                      dim.channel_value
                   end as 渠道,
                   a2.comm_date,
                   a2.保单号,
                   a2.是否开启保单自动化 is_policy_auto,
                   coalesce(a1.自动审核案件量, 0) 自动审核案件量,
                   coalesce(a2.全流程回传案件量, 0) 全流程回传案件量,
                   a2.保单创建时间,
                   a2.最早案件时间 ,
                   a2.最近案件时间
              from a1
             right join a2
                on a1.保单号 = a2.保单号
               and a1.comm_date = a2.comm_date
             inner join  claim_ods.dim_insure_company_channel dim
                on a2.insure_company_channel = dim.channel_key)
     where 渠道 not like '中智' and comm_date is not null
    union all
    select insure_company_channel,
           comm_date,
           保单号                 as policy_no,
           is_policy_auto,
           自动审核案件量         auto_exam_vol,
           全流程回传案件量       all_flow_hcvol,
           保单创建时间           policy_crt_tm,
           最早案件时间           early_claim_date,
           最近案件时间           near_claim_date,
           replace(comm_date,'-','')
      from (select '中智' insure_company_channel,
                   a4.comm_date,
                   a4.保单号,
                   coalesce(a3.自动审核案件量, 0) 自动审核案件量,
                   coalesce(a4.全流程回传案件量, 0) 全流程回传案件量,
                   a4.保单创建时间,
                   a4.最早案件时间 ,
                   a4.最近案件时间 ,
                   a4.是否开启保单自动化 is_policy_auto
              from a3
             right join a4
                on a3.保单号 = a4.保单号
               and a3.comm_date = a4.comm_date) where comm_date is not null
"""
def truncate_table(table_name='CLAIM_DWD.DWD_EXAM_AUTO_POLICY'):
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
