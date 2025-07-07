# import sys
# sys.path.append(r"E:\pycharm\database")
from database import DatabaseConnection
from datetime import datetime, date, timedelta


# 获取当前日期时间
now = datetime.now()
# 计算60天前的日期时间
ago_60_days = now - timedelta(days=60)
# 格式化为指定的字符串格式
ago_1_days = now - timedelta(days=1)
# 格式化为指定的字符串格式
formatted_date = ago_60_days.strftime('%Y-%m-%d')
yesterday_date = ago_1_days.strftime('%Y-%m-%d')



sql_query = f"""
  -- @description: 案件量统计报表--太保财苏州--雇主--一般医疗-基础数据
  -- @param:
  -- @return:
  -- @alter:
  -- @time: 2024-08-29 15:01:06
  -- @author: 01
  -- @version: 1.0.0


INSERT INTO CLAIM_DWD.DWD_CLAIM_COUNT_DAY (
    insure_company_channel,
    gmt_created,
    jjl,
    cancel_vol,
    xxqd,
    xsqd,
    lrl,
    drhcal,
    blchcal,
    qlchcal,
    perc_all_flow,
    hcsbal,
    zdshal,
    ZDSHAL_RW,
    shzdhl,
    shzdhl_rw,
    fh_auto_claim_count,
    fhzdhl,
    question_claim_vol,
    hospatal_claim_vol,
    data_dt,
    sh_claim_count
)
WITH base_data AS (
    SELECT distinct
        alr.accept_num,
        alr.claim_source,
        alr.ACCEPT_STATUS,
        CASE
            WHEN alr.service_line = 'GZX' THEN '太保苏州_雇主'
            WHEN alr.service_line IN ('BCYL', 'PC') THEN '太保苏州_医疗'
            ELSE ''
        END AS insure_company_channel,
        DATE(alr.T_CRT_TIME) AS T_CRT_TIME,
        DATE(c.create_time) AS lr_time,
        DATE(pr.back_time) AS back_time,
        DATE(q.gmt_created) AS question_time,
        pr.back_status,
        pr.postback_way,
        cat.C_HANDLE_CDE,
        cat.C_REVIEWER_CDE,
        cat.C_REVIEWER_STAFF,
        DATE(cat.T_CRT_TM) AS T_CRT_TM,
        p.C_CUSTOM_PLY_NO,
        CASE
            WHEN EXISTS (
                SELECT 1
                FROM claim_ods.bill sub_bill
                WHERE sub_bill.claim_id = c.id
                  AND sub_bill.delete_flag = '0'
                  AND sub_bill.insure_company_channel = 'CP10'
                  AND sub_bill.treatment_date IS NULL
            ) THEN '住院'
            ELSE '门诊'
        END AS treatment_type
    FROM claim_ods.accept_list_record alr
    LEFT JOIN claim_ods.claim c
        ON alr.accept_num = c.acceptance_no
        AND c.delete_flag = '0'
        AND c.insure_company_channel = 'CP10'
    LEFT JOIN claim_ods.postback_record pr
        ON pr.accept_num = alr.accept_num
        AND pr.is_deleted = 'N'
        AND pr.insure_company_channel = 'CP10'
    LEFT JOIN claim_ods.case_audit_task cat
        ON cat.c_claim_case_no = pr.app_no
        AND cat.insure_company_channel = 'CP10'
    LEFT JOIN claim_ods.question_claim q
        ON c.claim_no = q.claim_no
        AND q.belong_company = 'CP10'
        AND q.is_deleted = 'N'
    LEFT JOIN claim_ods.bill b
        ON b.claim_id = c.id
        AND b.delete_flag = '0'
        AND b.insure_company_channel = 'CP10'
    LEFT JOIN claim_ods.apply_claim ac
        ON c.claim_no = ac.apply_no
        AND ac.delete_flag = '0'
    LEFT JOIN claim_ods.claim_policy cp
        ON cp.policy_no = ac.policy_part_no
        AND cp.is_deleted = 'N'
        AND cp.insure_company_channel = 'CP10'
    LEFT JOIN claim_ods.ply p
        ON cp.group_policy_no = p.C_PLY_NO
    WHERE alr.DEL_FLAG = '0'
        AND alr.insure_company_channel = 'CP10'
        AND alr.ACCEPT_STATUS <> '1'
        AND alr.department_code = '苏州分公司'
        AND substr(alr.T_CRT_TIME,1,10) >= '{formatted_date}'
        GROUP BY
    alr.accept_num,
    alr.claim_source,
    alr.ACCEPT_STATUS,
    alr.service_line,
    alr.T_CRT_TIME,
    c.create_time,
    pr.back_time,
    pr.back_status,
    pr.postback_way,
    cat.C_HANDLE_CDE,
    cat.C_REVIEWER_CDE,
    cat.C_REVIEWER_STAFF,
    cat.T_CRT_TM,
    p.C_CUSTOM_PLY_NO,
    c.id,
    pr.app_no,
    c.claim_no
),
aggregated_data AS (
    SELECT
        dc_dim.CHANNEL_VALUE AS insure_company_channel,
        dc_dim.date_dtd AS gmt_created,
        COALESCE(SUM(CASE WHEN bd.T_CRT_TIME = dc_dim.date_dtd THEN 1 ELSE 0 END), 0) AS jjl,
        COALESCE(SUM(CASE WHEN bd.ACCEPT_STATUS = '5' AND bd.T_CRT_TIME = dc_dim.date_dtd THEN 1 ELSE 0 END), 0) AS cancel_vol,
        COALESCE(SUM(CASE WHEN bd.claim_source = '1' AND bd.T_CRT_TIME = dc_dim.date_dtd THEN 1 ELSE 0 END), 0) AS xxqd,
        COALESCE(SUM(CASE WHEN bd.claim_source = '2' AND bd.T_CRT_TIME = dc_dim.date_dtd THEN 1 ELSE 0 END), 0) AS xsqd,
        COALESCE(SUM(CASE WHEN bd.lr_time = dc_dim.date_dtd THEN 1 ELSE 0 END), 0) AS lrl,
        COALESCE(SUM(CASE WHEN bd.back_status IN ('2', '21') AND bd.back_time = dc_dim.date_dtd THEN 1 ELSE 0 END), 0) AS drhcal,
        COALESCE(SUM(CASE WHEN bd.back_status IN ('2', '21') AND bd.postback_way = 'H' AND bd.back_time = dc_dim.date_dtd THEN 1 ELSE 0 END), 0) AS blchcal,
        COALESCE(SUM(CASE WHEN (bd.back_status IN ('2', '21') AND (bd.postback_way = 'W' OR bd.postback_way IS NULL)) AND bd.back_time = dc_dim.date_dtd THEN 1 ELSE 0 END), 0) AS qlchcal,
        COALESCE(SUM(CASE WHEN bd.back_status = '3' AND bd.back_time = dc_dim.date_dtd THEN 1 ELSE 0 END), 0) AS hcsbal,
        COALESCE(SUM(CASE WHEN bd.C_HANDLE_CDE = '1' AND bd.back_status IN ('2', '21') AND bd.back_time = dc_dim.date_dtd THEN 1 ELSE 0 END), 0) AS zdshal,
        COALESCE(SUM(CASE WHEN bd.C_HANDLE_CDE = '1' AND bd.T_CRT_TM = dc_dim.date_dtd THEN 1 ELSE 0 END), 0) AS ZDSHAL_RW,
        COALESCE(SUM(CASE WHEN bd.C_REVIEWER_CDE = '1' AND bd.back_status IN ('2', '21') AND bd.back_time = dc_dim.date_dtd THEN 1 ELSE 0 END), 0) AS fh_auto_claim_count,
        COALESCE(SUM(CASE WHEN bd.question_time = dc_dim.date_dtd THEN 1 ELSE 0 END), 0) AS question_claim_vol,
        COALESCE(SUM(CASE WHEN bd.treatment_type = '住院' AND bd.T_CRT_TIME = dc_dim.date_dtd THEN 1 ELSE 0 END), 0) AS hospatal_claim_vol,
        COALESCE(COUNT(DISTINCT CASE WHEN bd.T_CRT_TM = dc_dim.date_dtd THEN bd.accept_num ELSE NULL END), 0) AS sh_claim_count,
        COALESCE(
            COUNT(DISTINCT CASE WHEN bd.C_HANDLE_CDE = '1' AND bd.T_CRT_TM = dc_dim.date_dtd THEN bd.accept_num ELSE NULL END)
            / NULLIF(COUNT(DISTINCT CASE WHEN bd.T_CRT_TM = dc_dim.date_dtd THEN bd.accept_num ELSE NULL END), 0),
            0
        ) AS shzdhl_rw
    FROM (
        SELECT *
        FROM claim_ods.DIM_CHANNEL_DATE
        WHERE date_dt >= '{formatted_date}'
          AND date_dt < '{yesterday_date}'
          AND CHANNEL_VALUE IN ('太保苏州_雇主', '太保苏州_医疗')
    ) dc_dim
    LEFT JOIN base_data bd
        ON dc_dim.date_dtd IN (bd.T_CRT_TIME, bd.lr_time, bd.back_time, bd.question_time, bd.T_CRT_TM)
        AND dc_dim.CHANNEL_VALUE = bd.insure_company_channel
    GROUP BY dc_dim.CHANNEL_VALUE, dc_dim.date_dtd
)
SELECT
    insure_company_channel,
    gmt_created,
    jjl,
    cancel_vol,
    xxqd,
    xsqd,
    lrl,
    drhcal,
    blchcal,
    qlchcal,
    CAST(CASE WHEN drhcal = 0 THEN 0 ELSE qlchcal / drhcal END AS DECIMAL(10, 4)) AS perc_all_flow,
    hcsbal,
    zdshal,
    ZDSHAL_RW,
    CAST(CASE WHEN qlchcal = 0 THEN 0 ELSE zdshal / qlchcal END AS DECIMAL(10, 4)) AS shzdhl,
    CAST(CASE WHEN shzdhl_rw > 1 THEN 1 ELSE shzdhl_rw END AS DECIMAL(10, 4)) AS shzdhl_rw,
    fh_auto_claim_count,
    CAST(CASE WHEN qlchcal = 0 THEN 0 ELSE fh_auto_claim_count / qlchcal END AS DECIMAL(10, 4)) AS fhzdhl,
    question_claim_vol,
    hospatal_claim_vol,
    REPLACE(SUBSTRING(gmt_created, 1, 10), '-', ''),
    sh_claim_count
FROM aggregated_data
WHERE insure_company_channel IS NOT NULL;

"""
def truncate_table(table_name='CLAIM_DWD.DWD_CLAIM_COUNT_DAY'):
    with DatabaseConnection() as conn:
        truncate_sql = f"delete  from  {table_name}  where  insure_company_channel  in ('太保苏州_雇主','太保苏州_医疗')  and gmt_created>='{formatted_date}'"
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
