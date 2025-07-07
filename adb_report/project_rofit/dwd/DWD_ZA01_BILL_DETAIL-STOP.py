# import sys
#
# sys.path.append(r"E:\pycharm\database")
from database import DatabaseConnection
from datetime import datetime

sql_query = r"""
  -- @description: 暖哇对账明细
  -- @param:
  -- @return:
  -- @alter:
  -- @time: 2024-06-20 15:01:06
  -- @author: 01
  -- @version: 1.0.0
INSERT INTO CLAIM_DWD.DWD_ZA01_BILL_DETAIL (
    insure_company_channel,
    comm_date,
    accept_num,
    policy_type,
    accept_date,
    back_date,
    bill_vol,
    data_dt
)
WITH tmp AS (
    SELECT
           ACCEPT_NUM
    FROM claim_ods.accept_list_record
    WHERE insure_company_channel = 'ZA01'
      AND REGEXP_REPLACE(SUBSTRING_INDEX(SUBSTRING_INDEX(Extra, 'caseLevel', -1), ',', 1), '[\\\\":}]', '') = '3'
)
, d1 AS (
    -- 好医保
    SELECT r.*
    FROM claim_ods.claim_policy p
    LEFT JOIN claim_ods.apply_claim a ON p.policy_no = a.policy_part_no
    LEFT JOIN claim_ods.claim c ON a.apply_no = c.claim_no
    LEFT JOIN claim_ods.accept_list_record r ON c.acceptance_no = r.ACCEPT_NUM
    WHERE p.insure_company_channel = 'ZA01'
      AND p.grade_name LIKE '好医保%'
),

t1 AS (
    SELECT ss.comm_date,
           ss.accept_num,
           ss.保单类型,
           ss.accept_date,
           ss.回传时间
    FROM (
        SELECT DISTINCT alr.accept_num,
                        SUBSTR(pr.gmt_modified, 1, 10) AS comm_date,
                        CASE
                            WHEN d1.accept_num = alr.accept_num THEN '好医保'
                            WHEN d2.accept_num = alr.accept_num THEN '一日赔'
                            WHEN e.policy_type = 'G' THEN '团单'
                            WHEN e.policy_type = 'P' THEN '个单'
                        END AS 保单类型,
                        alr.accept_date,
                        CASE
                            WHEN pr.sucess_time IS NULL THEN pr.gmt_modified
                            ELSE pr.sucess_time
                        END AS 回传时间,
                        b.match_hospital_name,
                        b.hospital_dept,
                        b.match_diagnose_name
        FROM claim_ods.accept_list_record alr
        LEFT JOIN d1 ON d1.accept_num = alr.accept_num
        LEFT JOIN tmp d2 ON d2.accept_num = alr.accept_num
        LEFT JOIN claim_ods.claim c ON alr.accept_num = c.acceptance_no
                                   AND c.delete_flag = '0'
                                   AND c.insure_company_channel = 'ZA01'
        INNER JOIN (
            SELECT ac.apply_no, ply.C_PLY_TYP AS policy_type
            FROM claim_ods.apply_claim ac
            LEFT JOIN claim_ods.ply ply ON ac.policy_no = ply.C_PLY_NO
            WHERE ac.INSURE_COMPANY_CHANNEL = 'ZA01'
        ) e ON c.claim_no = e.apply_no
        LEFT JOIN claim_ods.bill b ON b.claim_id = c.id
                                  AND b.delete_flag = '0'
                                  AND b.insure_company_channel = 'ZA01'
        LEFT JOIN claim_ods.postback_record pr ON alr.accept_num = pr.accept_num
                                               AND pr.back_status = '2'
                                               AND pr.insure_company_channel = 'ZA01'
        WHERE alr.DEL_FLAG = '0'
          AND alr.INSURE_COMPANY_CHANNEL = 'ZA01'
          AND c.clm_process_status IN (8, 9, 10)
          AND pr.gmt_modified IS NOT NULL
    ) ss
),
    t2 AS (
    SELECT accept_num, comm_date, SUM(账单数) AS 账单数
    FROM (
        SELECT alr.accept_num,
               SUBSTR(pr.gmt_modified, 1, 10) AS comm_date,
               COUNT(b.id) AS 账单数
        FROM claim_ods.accept_list_record alr
        LEFT JOIN claim_ods.claim c ON alr.accept_num = c.acceptance_no
                                   AND c.delete_flag = '0'
                                   AND c.insure_company_channel = 'ZA01'
        LEFT JOIN claim_ods.bill b ON b.claim_id = c.id
                                  AND b.delete_flag = '0'
                                  AND b.insure_company_channel = 'ZA01'
        LEFT JOIN claim_ods.postback_record pr ON alr.accept_num = pr.accept_num
                                               AND pr.back_status = '2'
                                               AND pr.insure_company_channel = 'ZA01'
        WHERE alr.DEL_FLAG = '0'
          AND pr.gmt_modified IS NOT NULL
          AND alr.INSURE_COMPANY_CHANNEL = 'ZA01'
          AND c.clm_process_status IN (8, 9, 10)
        GROUP BY alr.accept_num, SUBSTR(pr.gmt_modified, 1, 10), b.treatment_date
    )
    GROUP BY accept_num, comm_date)
SELECT '暖哇科技' AS insure_company_channel,
       cast(t1.comm_date as date),
       t1.accept_num AS accept_num,
       t1.保单类型 AS policy_type,
       cast(t1.accept_date as date) AS accept_date,
       cast(t1.回传时间 as date) AS back_date,
       t2.账单数 AS bill_vol,
       replace(t1.comm_date,'-','')
FROM t1
LEFT JOIN t2 ON t1.accept_num = t2.accept_num
              AND t1.comm_date = t2.comm_date;
"""


def truncate_table(table_name='CLAIM_DWD.DWD_ZA01_BILL_DETAIL'):
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
