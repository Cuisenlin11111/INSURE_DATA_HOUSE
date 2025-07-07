# import sys
#
# sys.path.append(r"E:\pycharm\database")
from database import DatabaseConnection
from datetime import datetime
from datetime import datetime, timedelta

# 获取当前日期
now = datetime.now()




sql_query = f"""
  -- @description: 泰康全渠道结案
  -- @param:
  -- @return:
  -- @alter:
  -- @time: 2025-01-08 15:01:06
  -- @author: 01
  -- @version: 1.0.0
insert into `CLAIM_DWD`.`DWD_TK_CLOSE_SUMMARY_M`
SELECT DISTINCT
    CASE
        WHEN alr.claim_source = '1' THEN '线下'
        WHEN alr.claim_source = '2' THEN '线上'
        WHEN alr.claim_source = '3' THEN '线上转线下'
        WHEN alr.claim_source = '4' THEN '线下转线上'
    END AS '来源',
    pr.ACCEPT_NUM AS 受理编号,
    cp.channel_group_policy_no AS 保单号,
    p.C_DEPT_NAME AS 投保单位,
    pr.insurance_claim_no AS 甲方案件号,
    cpc.insured_name AS 出险人姓名,
    cpc.insured_id_no AS 证件号码,
    COUNT(DISTINCT inv.C_INV_NO) AS 账单数量,
    GROUP_CONCAT(DISTINCT inv.c_response_desc) AS 赔付责任,
    CASE alr.INSURE_COMPANY_CHANNEL
        WHEN 'TK01' THEN '泰康上分'
        WHEN 'TK02' THEN '泰康电力'
        WHEN 'TK03' THEN '泰康浙分'
        WHEN 'TK04' THEN '泰康北分'
        WHEN 'TK05' THEN '泰康重分'
        WHEN 'TK06' THEN '泰康河南'
        WHEN 'TK07' THEN '泰康广分'
        WHEN 'TK08' THEN '泰康厦门'
        WHEN 'TK09' THEN '泰康江苏'
        WHEN 'TK10' THEN '泰康辽宁'
    END AS 渠道,
    pr.back_time AS 回传时间,
    IF(qc.id IS NULL, '否', '是') AS 是否发起过问题件,
    alr.T_CRT_TIME AS 系统受理时间,
    alr.ACCEPT_DATE AS 受理时间,
    clp.T_CRT_TM AS 预审最早流出节点时间,
    clp.T_UPD_TM AS 预审最后流出节点时间,
    MIN(clp11.T_CRT_TM) AS 第一次内部结案时间,
    cat.T_CLOSING_CASE_TM AS 内部结案时间,
    t.gmt_created AS 保司退回时间,
    cat.C_HANDLE_STAFF AS 审核人,
    cat.C_REVIEWER_STAFF AS 复核人,
    t.creator AS 退回人,
    cai.N_COMPENSATE_AMT AS 赔付金额,
    iat.gmt_created AS 上传时间,
    case when  pr.postback_way='H'  then '半流程'
         when  pr.postback_way='W'  then '全流程' else '未知' end as 案件形式,
             '2024-12'   
FROM claim_ods.postback_record pr
LEFT JOIN claim_ods.apply_claim ac ON pr.app_no = ac.apply_no AND ac.delete_flag = '0'
LEFT JOIN claim_ods.claim_policy cp ON cp.policy_no = ac.policy_part_no
LEFT JOIN claim_ods.claim_policy_customer cpc ON cpc.customer_no = cp.customer_no
LEFT JOIN claim_ods.ply p ON cp.group_policy_no = p.C_PLY_NO
LEFT JOIN claim_ods.`clm_visit_inv_info` inv ON pr.app_no = inv.C_CUSTOM_APP_NO AND inv.C_DEL_FLAG = '0'
LEFT JOIN claim_ods.`accept_list_record` alr ON pr.accept_num = alr.accept_num
LEFT JOIN claim_ods.question_claim qc ON pr.app_no = qc.claim_no
LEFT JOIN claim_ods.clm_app_info cai ON cai.C_CUSTOM_APP_NO = pr.app_no AND cai.C_DEL_FLAG = '0'
LEFT JOIN claim_ods.clm_process clp ON pr.app_no = clp.C_CLAIM_APPLY_NO AND clp.C_PROCESS_STATUS = '0' AND clp.C_PROCESS_SUB_STATUS = '02'
LEFT JOIN claim_ods.`case_audit_task` cat ON inv.C_CUSTOM_APP_NO = cat.C_CLAIM_CASE_NO AND cat.C_DEL_FLAG = '0'
LEFT JOIN claim_ods.operation_log t ON t.busi_no = pr.app_no AND t.oper_type = '16' AND t.sub_busi_type IN ('2')
LEFT JOIN claim_ods.clm_process clp11 ON pr.app_no = clp11.C_CLAIM_APPLY_NO AND clp11.C_PROCESS_SUB_STATUS = '82' AND clp11.C_PROCESS_STATUS = '8'
LEFT JOIN claim_ods.image_assign_task iat ON iat.accept_batch_no = pr.accept_batch_no AND iat.is_deleted = 'N'
WHERE pr.INSURE_COMPANY_CHANNEL LIKE 'TK%'
    AND pr.is_deleted = 'N'
    AND pr.back_status IN ('21', '2')
    AND SUBSTRING(pr.back_time, 1, 7) IN ('2024-12')
    AND pr.receiver = 'I'
    AND cp.channel_group_policy_no NOT IN ('2853017521816', '2853017514284', '2853017405982', '2853017440422', '2853017337887', '2853017337885', '2853017337886', '2853017326791', '2853017326792', '2853017326788')
GROUP BY pr.insurance_claim_no, pr.ACCEPT_NUM

UNION ALL

SELECT DISTINCT
    CASE
        WHEN alr.claim_source = '1' THEN '线下'
        WHEN alr.claim_source = '2' THEN '线上'
        WHEN alr.claim_source = '3' THEN '线上转线下'
        WHEN alr.claim_source = '4' THEN '线下转线上'
    END AS '来源',
    pr.ACCEPT_NUM AS 受理编号,
    cp.channel_group_policy_no AS 保单号,
    p.C_DEPT_NAME AS 投保单位,
    pr.insurance_case_no AS 甲方案件号,
    cpc.insured_name AS 出险人姓名,
    cpc.insured_id_no AS 证件号码,
    COUNT(DISTINCT inv.C_INV_NO) AS 账单数量,
    GROUP_CONCAT(DISTINCT inv.c_response_desc) AS 赔付责任,
    CASE alr.INSURE_COMPANY_CHANNEL
        WHEN 'TK01' THEN '泰康上分'
        WHEN 'TK02' THEN '泰康电力'
        WHEN 'TK03' THEN '泰康浙分'
        WHEN 'TK04' THEN '泰康北分'
        WHEN 'TK05' THEN '泰康重分'
        WHEN 'TK06' THEN '泰康河南'
        WHEN 'TK07' THEN '泰康广分'
        WHEN 'TK08' THEN '泰康厦门'
        WHEN 'TK09' THEN '泰康江苏'
        WHEN 'TK10' THEN '泰康辽宁'
    END AS 渠道,
    pr.back_time AS 回传时间,
    IF(qc.id IS NULL, '否', '是') AS 是否发起过问题件,
    alr.T_CRT_TIME AS 系统受理时间,
    alr.ACCEPT_DATE AS 受理时间,
    clp.T_CRT_TM AS 预审最早流出节点时间,
    clp.T_UPD_TM AS 预审最后流出节点时间,
    MIN(clp11.T_CRT_TM) AS 第一次内部结案时间,
    cat.T_CLOSING_CASE_TM AS 内部结案时间,
    t.gmt_created AS 保司退回时间,
    cat.C_HANDLE_STAFF AS 审核人,
    cat.C_REVIEWER_STAFF AS 复核人,
    t.creator AS 退回人,
    cai.N_COMPENSATE_AMT AS 赔付金额,
    iat.gmt_created AS 上传时间,
        '' as 案件形式,
    '2024-12'    
        
FROM claim_ods.insurance_company_case pr
LEFT JOIN claim_ods.claim_policy cp ON cp.policy_no = pr.policy_no
LEFT JOIN claim_ods.claim_policy_customer cpc ON cpc.customer_no = cp.customer_no
LEFT JOIN claim_ods.ply p ON cp.group_policy_no = p.C_PLY_NO
LEFT JOIN claim_ods.`clm_visit_inv_info` inv ON pr.app_no = inv.C_CUSTOM_APP_NO AND pr.policy_no = inv.C_PLY_NO AND inv.C_DEL_FLAG = '0'
LEFT JOIN claim_ods.`accept_list_record` alr ON pr.accept_num = alr.accept_num
LEFT JOIN claim_ods.question_claim qc ON pr.app_no = qc.claim_no
LEFT JOIN claim_ods.clm_app_info cai ON cai.C_CUSTOM_APP_NO = pr.app_no AND cai.C_DEL_FLAG = '0'
LEFT JOIN claim_ods.clm_process clp ON pr.app_no = clp.C_CLAIM_APPLY_NO AND clp.C_PROCESS_STATUS = '0' AND clp.C_PROCESS_SUB_STATUS = '02'
LEFT JOIN claim_ods.`case_audit_task` cat ON inv.C_CUSTOM_APP_NO = cat.C_CLAIM_CASE_NO AND cat.C_DEL_FLAG = '0'
LEFT JOIN claim_ods.operation_log t ON t.busi_no = pr.app_no AND t.oper_type = '16' AND t.sub_busi_type IN ('2')
LEFT JOIN claim_ods.clm_process clp11 ON pr.app_no = clp11.C_CLAIM_APPLY_NO AND clp11.C_PROCESS_SUB_STATUS = '82' AND clp11.C_PROCESS_STATUS = '8'
LEFT JOIN claim_ods.image_assign_task iat ON iat.accept_batch_no = alr.accept_batch_no AND iat.is_deleted = 'N'
WHERE pr.INSURE_COMPANY_CHANNEL LIKE 'TK%'
    AND pr.is_deleted = 'N'
    AND pr.case_status IN ('05', '08')
    AND SUBSTRING(pr.back_time, 1, 7) = '2024-12'
    AND cp.channel_group_policy_no IN ('2853017521816', '2853017514284', '2853017405982', '2853017440422', '2853017337887', '2853017337885', '2853017337886', '2853017326791', '2853017326792', '2853017326788')
GROUP BY pr.insurance_case_no, pr.ACCEPT_NUM

"""


def truncate_table(table_name='CLAIM_DWD.DWD_TK_CLOSE_SUMMARY_M'):
    with DatabaseConnection() as conn:
        truncate_sql = f"delete  from {table_name} where date_month='2024-12'"
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
