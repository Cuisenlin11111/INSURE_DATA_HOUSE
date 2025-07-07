# import sys
# sys.path.append(r"E:\pycharm\database")
from database import DatabaseConnection
from datetime import datetime



sql_query = f"""
  -- @description: 平安产险机构数据
  -- @param:
  -- @return:
  -- @alter:
  -- @time: 2024-08-29 15:01:06
  -- @author: 01
  -- @version: 1.0.0
INSERT INTO CLAIM_DWD.DWD_PA02_DEPARTMENT_DAY
(
    insure_company_channel,
    department_code,
    in_claim_date,
    in_count,
    total_in_count,
    fw_inclaim_vol,
    ow_inclaim_vol,
    month_hb_vol,
    week_hb_vol,
    all_hc_this_day,
    half_flow_hc_vol,
    full_flow_hc_vol,
    cancel_vol,
    data_dt
)

WITH t AS
(
    SELECT
        insure_company_channel,
        department_code,
        COUNT(*) AS in_count,
        SUBSTR(T_CRT_TIME, 1, 10) AS in_claim_date
    FROM claim_ods.accept_list_record
    WHERE insure_company_channel = 'PA02' AND DEL_FLAG = '0'
    GROUP BY
        insure_company_channel,
        department_code,
        SUBSTR(T_CRT_TIME, 1, 10)
),
lst AS
(
    SELECT
        insure_company_channel,
        department_code,
        COUNT(*) AS 四周前当日进件量,
        SUBSTR(T_CRT_TIME, 1, 10) AS in_claim_date
    FROM claim_ods.accept_list_record
    WHERE insure_company_channel = 'PA02' AND DEL_FLAG = '0'
    GROUP BY
        insure_company_channel,
        department_code,
        SUBSTR(T_CRT_TIME, 1, 10)
),
lw AS
(
    SELECT
        insure_company_channel,
        department_code,
        COUNT(*) AS 一周前当日进件量,
        SUBSTR(T_CRT_TIME, 1, 10) AS in_claim_date
    FROM claim_ods.accept_list_record
    WHERE insure_company_channel = 'PA02' AND DEL_FLAG = '0'
    GROUP BY
        insure_company_channel,
        department_code,
        SUBSTR(T_CRT_TIME, 1, 10)
),
a2 AS
(
    SELECT
        alr.department_code,
        SUBSTR(pr.gmt_created, 1, 10) AS gmt_created,
        COUNT(DISTINCT pr.app_no) AS all_hc_this_day
    FROM claim_ods.postback_record pr
    LEFT JOIN claim_ods.claim c ON pr.app_no = c.claim_no AND pr.is_deleted = 'N' AND pr.insure_company_channel = 'PA02'
    LEFT JOIN claim_ods.accept_list_record alr ON alr.accept_num = c.acceptance_no AND c.delete_flag = '0'
    WHERE pr.insure_company_channel = 'PA02' AND pr.back_status IN ('2', '21') AND pr.is_deleted = 'N'
    GROUP BY
        SUBSTR(pr.gmt_created, 1, 10),
        alr.department_code
),
tc AS
(
    SELECT
        insure_company_channel,
        COUNT(*) AS total_in_count,
        SUBSTR(T_CRT_TIME, 1, 10) AS in_claim_date
    FROM claim_ods.accept_list_record
    WHERE insure_company_channel = 'PA02' AND DEL_FLAG = '0'
    GROUP BY
        insure_company_channel,
        SUBSTR(T_CRT_TIME, 1, 10)
),
cxl AS
(
    SELECT
        department_code,
        COUNT(*) AS cancel_vol,
        SUBSTR(T_CRT_TIME, 1, 10) AS t_crt_time
    FROM claim_ods.accept_list_record
    WHERE insure_company_channel = 'PA02' AND DEL_FLAG = '0' AND ACCEPT_STATUS = '5'
    GROUP BY
        department_code,
        SUBSTR(T_CRT_TIME, 1, 10)
),
bhcl AS
(
    SELECT
        alr.department_code,
        SUBSTR(pr.gmt_created, 1, 10) AS gmt_created,
        COUNT(DISTINCT pr.app_no) AS half_flow_hc_vol
    FROM claim_ods.postback_record pr
    LEFT JOIN claim_ods.claim c ON pr.app_no = c.claim_no AND pr.is_deleted = 'N' AND pr.insure_company_channel = 'PA02'
    LEFT JOIN claim_ods.accept_list_record alr ON alr.accept_num = c.acceptance_no AND c.delete_flag = '0'
    WHERE pr.insure_company_channel = 'PA02' AND pr.back_status IN ('2', '21') AND pr.is_deleted = 'N' AND pr.postback_way = 'H'
    GROUP BY
        SUBSTR(pr.gmt_created, 1, 10),
        alr.department_code
),
qhcl AS
(
    SELECT
        alr.department_code,
        SUBSTR(pr.gmt_created, 1, 10) AS gmt_created,
        COUNT(DISTINCT pr.app_no) AS full_flow_hc_vol
    FROM claim_ods.postback_record pr
    LEFT JOIN claim_ods.claim c ON pr.app_no = c.claim_no AND pr.is_deleted = 'N' AND pr.insure_company_channel = 'PA02'
    LEFT JOIN claim_ods.accept_list_record alr ON alr.accept_num = c.acceptance_no AND c.delete_flag = '0'
    WHERE pr.insure_company_channel = 'PA02' AND pr.back_status IN ('2', '21') AND pr.is_deleted = 'N' AND (pr.postback_way = 'W' OR pr.postback_way IS NULL)
    GROUP BY
        SUBSTR(pr.gmt_created, 1, 10),
        alr.department_code
)
SELECT
    t.insure_company_channel,
    t.department_code,
    t.in_claim_date,
    COALESCE(t.in_count, 0) AS in_count,
    COALESCE(tc.total_in_count, 0) AS total_in_count,
    COALESCE(lst.四周前当日进件量, 0) AS fw_inclaim_vol,
    COALESCE(lw.一周前当日进件量, 0) AS ow_inclaim_vol,
    COALESCE(t.in_count, 0) - COALESCE(lst.四周前当日进件量, 0) AS month_hb_vol,
    COALESCE(t.in_count, 0) - COALESCE(lw.一周前当日进件量, 0) AS week_hb_vol,
    COALESCE(a2.all_hc_this_day, 0) AS all_hc_this_day,
    COALESCE(bhcl.half_flow_hc_vol, 0) AS half_flow_hc_vol,
    COALESCE(qhcl.full_flow_hc_vol, 0) AS full_flow_hc_vol,
    COALESCE(cxl.cancel_vol, 0) AS cancel_vol,
    REPLACE(CURDATE(), '-', '') AS data_dt
FROM
    t
LEFT JOIN
    lst ON t.department_code = lst.department_code AND lst.in_claim_date = DATE_SUB(t.in_claim_date, INTERVAL 28 DAY)
LEFT JOIN
    lw ON t.department_code = lw.department_code AND lw.in_claim_date = DATE_SUB(t.in_claim_date, INTERVAL 7 DAY)
LEFT JOIN
    a2 ON t.department_code = a2.department_code AND t.in_claim_date = a2.gmt_created
LEFT JOIN
    tc ON t.in_claim_date = tc.in_claim_date
LEFT JOIN
    cxl ON t.department_code = cxl.department_code AND t.in_claim_date = cxl.t_crt_time
LEFT JOIN
    bhcl ON t.department_code = bhcl.department_code AND t.in_claim_date = bhcl.gmt_created
LEFT JOIN
    qhcl ON t.department_code = qhcl.department_code AND t.in_claim_date = qhcl.gmt_created;

"""
def truncate_table(table_name='CLAIM_DWD.DWD_PA02_DEPARTMENT_DAY'):
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
