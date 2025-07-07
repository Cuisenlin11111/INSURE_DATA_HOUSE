# import sys
# sys.path.append(r"E:\pycharm\database")
from database import DatabaseConnection
from datetime import datetime, date, timedelta

# 获取当前日期时间
now = datetime.now()
# 计算 60 天前的日期时间
ago_60_days = now - timedelta(days=10)
# 格式化为指定的字符串格式
formatted_date = ago_60_days.strftime('%Y-%m-%d')



sql_query = f"""
  -- @description: 审核风控分析--保单信息
  -- @param:
  -- @return:
  -- @alter:
  -- @time: 2024-06-27 15:01:06
  -- @author: 01
  -- @version: 1.0.0
INSERT INTO CLAIM_DWD.DWD_EXAM_RISK_ANALYSIS_POLICY
    (insure_company_channel,
     group_policy_no,
     comm_date,
     risk_layer,
     c_risk_reason,
     EXPLAIN_CODE,
     `EXPLAIN_DESC`,  -- 使用反引号括起来
     claim_no,
     data_dt,
     product_type)
   with v1 as
     (select case when  coalesce(C_RISK_REASON,'')=''  then C_AUTO_CHECK_CONCLUSION  else  C_RISK_REASON  end   c_risk_reason,
             C_CUSTOM_APP_NO
        from claim_ods.clm_app_info
       where (coalesce(C_RISK_REASON,'')<>''  or C_AUTO_CHECK_CONCLUSION is not null )
         and C_DEL_FLAG = '0'
       and substr(T_CRT_TM,1,10) >= '{formatted_date}' 
       ),
    v2 as
     (select c_risk_reason,
             C_CUSTOM_APP_NO,
             EXPLAIN_CODE,
             `EXPLAIN_DESC`
        from claim_ods.clm_visit_info
       where coalesce(C_RISK_REASON,'')<>''
         and C_DEL_FLAG = '0'
       and substr(T_CRT_TM,1,10) >= '{formatted_date}'
       ),
    v3 as
     (select c_risk_reason,
             C_CUSTOM_APP_NO,
             EXPLAIN_CODE,
             `EXPLAIN_DESC`
        from claim_ods.clm_visit_inv_info
       where coalesce(C_RISK_REASON,'')<>''
         and C_DEL_FLAG = '0'
       and substr(T_CRT_TM,1,10) >= '{formatted_date}'
       ),
          v4 as
     (select c_risk_reason,
             C_CUSTOM_APP_NO,
             EXPLAIN_CODE,
             `EXPLAIN_DESC`
        from claim_ods.clm_treatment
       where coalesce(C_RISK_REASON,'')<>''
         and C_DEL_FLAG = '0'
       and substr(T_CRT_TM,1,10) >= '{formatted_date}'
       )
    select dim.channel_value as insure_company_channel,
           group_policy_no,
           comm_date,
           risk_layer,
           c_risk_reason,
           EXPLAIN_CODE,
           `EXPLAIN_DESC`,
#            trigger_claim_num,
           claim_no
           ,
           replace(comm_date,'-',''),
           case when product_type='P' THEN '意健险-个险'
                when product_type='G' THEN '意健险-团险'
                 when product_type='Z' THEN '雇主'
                 when product_type='C' THEN '车险' else '' end as product_type
      from (select insure_company_channel,
                   group_policy_no,
                   comm_date,
                   risk_layer,
                   c_risk_reason,
                   EXPLAIN_CODE,
                   `EXPLAIN_DESC`,
#                    SUM(trigger_claim_num) trigger_claim_num,
                   claim_no,
                   product_type
              from (SELECT CASE
                                WHEN c.insure_company_channel = 'CP10' THEN alro.DEPARTMENT_CODE
                                ELSE c.insure_company_channel
                            END insure_company_channel,
                           cc.group_policy_no,
                           substr(cat.T_CRT_TM,1,10) comm_date,
                           '申请层' risk_layer,
                           v1.c_risk_reason,
                           '' EXPLAIN_CODE,
                           '' `EXPLAIN_DESC`,
#                            count(DISTINCT c.claim_no) as trigger_claim_num,
                           c.claim_no,
                           alro.product_type
                      FROM claim_ods.claim c
                      LEFT JOIN claim_ods.ACCEPT_LIST_RECORD alro
                            ON c.ACCEPTANCE_NO = alro.accept_num AND alro.DEL_FLAG = '0'
                      join claim_ods.apply_claim ac
                        on c.claim_no = ac.apply_no
                       and ac.claim_status = '1'
                       and c.delete_flag = '0'
                      join claim_ods.claim_policy cc
                        on ac.policy_part_no = cc.policy_no
                      JOIN claim_ods.case_audit_task cat
                        on cat.C_CLAIM_CASE_NO = c.claim_no
                       and cat.C_DEL_FLAG = '0'
                       and cat.C_HANDLE_CDE <> '1'
                      join v1
                        on v1.C_CUSTOM_APP_NO = c.claim_no
                      where substr(cat.T_CRT_TM,1,10)>='{formatted_date}'
                     GROUP BY c.insure_company_channel,
                              cc.group_policy_no,
                              substr(cat.T_CRT_TM, 1,10),
                              v1.c_risk_reason,alro.DEPARTMENT_CODE,c.claim_no,alro.product_type)
                     GROUP BY insure_company_channel,
                              group_policy_no,
                              comm_date,
                              c_risk_reason,
                              `EXPLAIN_DESC`,
                              risk_layer,
                              claim_no,
                              product_type
            union all
            select insure_company_channel,
                   group_policy_no,
                   comm_date,
                   risk_layer,
                   c_risk_reason,
                   EXPLAIN_CODE,
                   `EXPLAIN_DESC`,
#                    SUM(trigger_claim_num) trigger_claim_num,
                   claim_no,
                   product_type
              from (SELECT CASE
                                WHEN c.insure_company_channel = 'CP10' THEN alro.DEPARTMENT_CODE
                                ELSE c.insure_company_channel
                            END insure_company_channel,
                           cc.group_policy_no,
                           substr(cat.T_CRT_TM,1,10) comm_date,
                           '诊断层' risk_layer,
                           v2.c_risk_reason,
                           v2.EXPLAIN_CODE,
                           v2.`EXPLAIN_DESC`,
#                            count(DISTINCT c.claim_no) as trigger_claim_num,
                           c.claim_no claim_no,
                           alro.product_type
                      FROM claim_ods.claim c
                       LEFT JOIN claim_ods.ACCEPT_LIST_RECORD alro
                            ON c.ACCEPTANCE_NO = alro.accept_num AND alro.DEL_FLAG = '0'
                      join claim_ods.apply_claim ac
                        on c.claim_no = ac.apply_no
                       and ac.claim_status = '1'
                       and c.delete_flag = '0'
                      join claim_ods.claim_policy cc
                        on ac.policy_part_no = cc.policy_no
                      JOIN claim_ods.case_audit_task cat
                        on cat.C_CLAIM_CASE_NO = c.claim_no
                       and cat.C_DEL_FLAG = '0'
                       and cat.C_HANDLE_CDE <> '1'
                      join v2
                        on v2.C_CUSTOM_APP_NO = c.claim_no
                      where substr(cat.T_CRT_TM,1,10)>='{formatted_date}'
                     GROUP BY c.insure_company_channel,
                              cc.group_policy_no,
                              substr(cat.T_CRT_TM, 1,10),
                              v2.c_risk_reason,alro.DEPARTMENT_CODE,c.claim_no,alro.product_type)
                     GROUP BY insure_company_channel,
                              group_policy_no,
                              comm_date,
                              c_risk_reason,
                              `EXPLAIN_DESC`,
                              risk_layer,
                              claim_no,
                              product_type
            union all
            select insure_company_channel,
                   group_policy_no,
                   comm_date,
                   risk_layer,
                   c_risk_reason,
                   EXPLAIN_CODE,
                   `EXPLAIN_DESC`,
#                    SUM(trigger_claim_num) trigger_claim_num,
                   claim_no,
                   product_type
              from (SELECT CASE
                                WHEN c.insure_company_channel = 'CP10' THEN alro.DEPARTMENT_CODE
                                ELSE c.insure_company_channel
                            END insure_company_channel,
                           cc.group_policy_no,
                           substr(cat.T_CRT_TM,1,10) comm_date,
                           '发票层' risk_layer,
                           v3.c_risk_reason,
                           v3.EXPLAIN_CODE,
                           v3.`EXPLAIN_DESC`,
#                            count(DISTINCT c.claim_no) as trigger_claim_num,
                           c.claim_no claim_no,
                           alro.product_type
                      FROM claim_ods.claim c
                      LEFT JOIN claim_ods.ACCEPT_LIST_RECORD alro
                            ON c.ACCEPTANCE_NO = alro.accept_num AND alro.DEL_FLAG = '0'
                      join claim_ods.apply_claim ac
                        on c.claim_no = ac.apply_no
                       and ac.claim_status = '1'
                       and c.delete_flag = '0'
                      join claim_ods.claim_policy cc
                        on ac.policy_part_no = cc.policy_no
                      JOIN claim_ods.case_audit_task cat
                        on cat.C_CLAIM_CASE_NO = c.claim_no
                       and cat.C_DEL_FLAG = '0'
                       and cat.C_HANDLE_CDE <> '1'
                      join v3
                        on v3.C_CUSTOM_APP_NO = c.claim_no
                      where substr(cat.T_CRT_TM,1,10)>='{formatted_date}'
                     GROUP BY c.insure_company_channel,
                              cc.group_policy_no,
                              substr(cat.T_CRT_TM, 1,10),
                              v3.c_risk_reason,
                              v3.`EXPLAIN_DESC`,alro.DEPARTMENT_CODE,c.claim_no,alro.product_type)
                    GROUP BY insure_company_channel,
                              group_policy_no,
                              comm_date,
                              c_risk_reason,
                              `EXPLAIN_DESC`,
                              risk_layer,
                              claim_no,
                              product_type
            union all
                        select insure_company_channel,
                   group_policy_no,
                   comm_date,
                   risk_layer,
                   c_risk_reason,
                   EXPLAIN_CODE,
                   `EXPLAIN_DESC`,
#                    SUM(trigger_claim_num) trigger_claim_num,
                   claim_no,
                   product_type
              from (SELECT CASE
                                WHEN c.insure_company_channel = 'CP10' THEN alro.DEPARTMENT_CODE
                                ELSE c.insure_company_channel
                            END insure_company_channel,
                           cc.group_policy_no,
                           substr(cat.T_CRT_TM,1,10) comm_date,
                           '明细层' risk_layer,
                           v4.c_risk_reason,
                           v4.EXPLAIN_CODE,
                           v4.`EXPLAIN_DESC`,
#                            count(DISTINCT c.claim_no) as trigger_claim_num,
                           c.claim_no claim_no,
                           alro.product_type
                      FROM claim_ods.claim c
                      LEFT JOIN claim_ods.ACCEPT_LIST_RECORD alro
                            ON c.ACCEPTANCE_NO = alro.accept_num AND alro.DEL_FLAG = '0'
                      join claim_ods.apply_claim ac
                        on c.claim_no = ac.apply_no
                       and ac.claim_status = '1'
                       and c.delete_flag = '0'
                      join claim_ods.claim_policy cc
                        on ac.policy_part_no = cc.policy_no
                      JOIN claim_ods.case_audit_task cat
                        on cat.C_CLAIM_CASE_NO = c.claim_no
                       and cat.C_DEL_FLAG = '0'
                       and cat.C_HANDLE_CDE <> '1'
                      join v4
                        on v4.C_CUSTOM_APP_NO = c.claim_no
                      where substr(cat.T_CRT_TM,1,10)>='{formatted_date}'
                     GROUP BY c.insure_company_channel,
                              cc.group_policy_no,
                              substr(cat.T_CRT_TM, 1,10),
                              v4.c_risk_reason,
                              v4.`EXPLAIN_DESC`,alro.DEPARTMENT_CODE,c.claim_no,alro.product_type)
                    GROUP BY insure_company_channel,
                              group_policy_no,
                              comm_date,
                              c_risk_reason,
                              `EXPLAIN_DESC`,
                              risk_layer,
                              claim_no,
                              product_type
                             ) ff

     inner join claim_ods.dim_insure_company_channel dim
        on ff.insure_company_channel = dim.channel_key
    where  comm_date>='{formatted_date}'
        ;

"""


def truncate_table(table_name='CLAIM_DWD.DWD_EXAM_RISK_ANALYSIS_POLICY'):
    with DatabaseConnection() as conn:
        truncate_sql = f"delete  from  {table_name}  where  COMM_DATE>='{formatted_date}'"
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
