# import sys
# sys.path.append(r"E:\pycharm\database")
from database import DatabaseConnection
from datetime import datetime



sql_query = f"""
  -- @description: 案件退回原因信息
  -- @param:
  -- @return:
  -- @alter:
  -- @time: 2024-08-29 15:01:06
  -- @author: 01
  -- @version: 1.0.0
INSERT INTO CLAIM_DWD.DWD_CLAIM_RETURN_REASON (
    insure_company_channel,
    gmt_created,
    claim_no,
    accept_num,
    oper_type,
    return_reason,
    pzr,
    ysr,
    yyppr,
    zdppr,
    mxppr,
    kfr,
    shr,
    fhr,
    ply_no,
    C_DEPT_NAME,
    data_dt
)
WITH yy AS (
    SELECT DISTINCT claim_id, match_ower
    FROM claim_ods.manual_match_task
    WHERE match_type = 1
),
zd AS (
    SELECT DISTINCT claim_id, match_ower
    FROM claim_ods.manual_match_task
    WHERE match_type = 2
),
mx AS (
    SELECT DISTINCT claim_id, match_ower
    FROM claim_ods.manual_match_task
    WHERE match_type = 3
),
t1 AS (
    SELECT DISTINCT
        c.claim_no,
        cf.handle_staff AS 配置人,
        e.allot_operator_name AS 预审人,
        (SELECT DISTINCT full_name FROM claim_ods.sys_users WHERE id = yy.match_ower) AS 医院匹配人,
        (SELECT DISTINCT full_name FROM claim_ods.sys_users WHERE id = zd.match_ower) AS 诊断匹配人,
        (SELECT DISTINCT full_name FROM claim_ods.sys_users WHERE id = mx.match_ower) AS 明细匹配人,
        CASE
            WHEN d.deduct_owner IS NULL THEN '系统自动'
            ELSE (SELECT DISTINCT full_name FROM claim_ods.sys_users WHERE id = d.deduct_owner)
        END AS 扣费人,
        cat.C_HANDLE_STAFF AS 审核人,
        cat.C_REVIEWER_STAFF AS 复核人
    FROM claim_ods.claim c
    LEFT JOIN claim_ods.clm_pretrial_examine e ON c.claim_no = e.claim_app_no
    LEFT JOIN yy ON yy.claim_id = c.id
    LEFT JOIN zd ON zd.claim_id = c.id
    LEFT JOIN mx ON mx.claim_id = c.id
    LEFT JOIN claim_ods.deduct_task d ON c.id = d.claim_id  ## 扣费
    LEFT JOIN claim_ods.case_audit_task cat ON cat.c_claim_case_no = c.claim_no AND cat.C_DEL_FLAG = '0'
    LEFT JOIN claim_ods.special_config_task cf ON cf.accept_num = c.acceptance_no AND c.delete_flag = '0'
)
SELECT
    dim.channel_value,
    subquery.gmt_created,
    claim_no,
    accept_num,
    oper_type,
    return_reason,
    GROUP_CONCAT(DISTINCT 配置人) AS 配置人,
    GROUP_CONCAT(DISTINCT 预审人) AS 预审人,
    GROUP_CONCAT(DISTINCT 医院匹配人) AS 医院匹配人,
    GROUP_CONCAT(DISTINCT 诊断匹配人) AS 诊断匹配人,
    GROUP_CONCAT(DISTINCT 明细匹配人) AS 明细匹配人,
    GROUP_CONCAT(DISTINCT 扣费人) AS 扣费人,
    GROUP_CONCAT(DISTINCT 审核人) AS 审核人,
    GROUP_CONCAT(DISTINCT 复核人) AS 复核人,
    cp.channel_group_policy_no as 保单号,
    p.C_DEPT_NAME 投保单位,
    REPLACE(CURDATE(), '-', '') AS data_dt
FROM (
    SELECT
        CASE
            WHEN c.insure_company_channel = 'CP10' THEN alro.DEPARTMENT_CODE
            ELSE c.insure_company_channel
        END AS insure_company_channel,
        op.gmt_created,
        c.claim_no,
        c.acceptance_no AS accept_num,
        CASE
            WHEN op.sub_busi_type = '6' AND op.oper_type = '15' THEN '退回录入'
            WHEN op.sub_busi_type = '4' AND op.oper_type = '16' THEN '回传保司退回审核'
            WHEN op.sub_busi_type = '2' AND op.oper_type = '16' THEN '保司复核退回审核'
            WHEN op.oper_type = '16' THEN '退回审核'
            WHEN op.oper_type = '15' THEN '退回扣费'
            ELSE '其他操作'
        END AS oper_type,
        NVL(op.extra, bep.bill_info) AS return_reason,
        配置人,
        预审人,
        医院匹配人,
        诊断匹配人,
        明细匹配人,
        扣费人,
        审核人,
        复核人
    FROM claim_ods.claim c
    LEFT JOIN claim_ods.ACCEPT_LIST_RECORD alro ON c.ACCEPTANCE_NO = alro.accept_num AND alro.DEL_FLAG = '0'
    JOIN claim_ods.operation_log op ON op.busi_no = c.claim_no AND c.delete_flag = '0'
    LEFT JOIN claim_ods.back_entry_provider bep ON bep.claim_no = op.busi_no
    JOIN t1 ON t1.claim_no = c.claim_no
    WHERE c.delete_flag = '0' AND op.oper_type IN (15, 16)
) AS subquery
JOIN claim_ods.dim_insure_company_channel dim ON insure_company_channel = dim.channel_key
left join claim_ods.clm_app_info cai on cai.C_CUSTOM_APP_NO = subquery.claim_no
left JOIN claim_ods.claim_policy cp on cp.policy_no = cai.C_PLY_NO and cai.C_DEL_FLAG = 0 -- and cp.insure_company_channel = 'TK04'
left join claim_ods.ply p on cp.group_policy_no = p.C_PLY_NO
WHERE SUBSTR(subquery.gmt_created, 1, 4) > '2023'
GROUP BY
    dim.channel_value,
    subquery.gmt_created,
    claim_no,
    accept_num,
    oper_type,
    return_reason;

"""
def truncate_table(table_name='CLAIM_DWD.DWD_CLAIM_RETURN_REASON'):
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
