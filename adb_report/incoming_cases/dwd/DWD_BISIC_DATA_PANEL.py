# import sys
# sys.path.append(r"E:\pycharm\database")
from database import DatabaseConnection
from datetime import datetime



sql_query = f"""
  -- @description: 基础数据看板
  -- @param:
  -- @return:
  -- @alter:
  -- @time: 2024-08-29 15:01:06
  -- @author: 01
  -- @version: 1.0.0
INSERT INTO CLAIM_DWD.DWD_BISIC_DATA_PANEL (
    insure_company_channel,     -- 渠道
    create_time,                -- 时间
    total_claim_count,          -- 受理案件量
    total_mz_claim_count,       -- 门诊案件量
    hospital_claim_count,       -- 住院案件数
    mz_online_claim_count,      -- 门诊线上案件数
    mz_offline_claim_count,     -- 门诊线下案件数
    all_flow_claim_count,       -- 全流程案件量
    half_flow_claim_count,      -- 半流程案件量
    total_half_all_flow,        -- 全流程和半流程案件量之和
    data_dt                     -- 调度日期
)
WITH a1 AS (
    SELECT
        COUNT(DISTINCT c.id) AS mz_online_claim_count,
        c.insure_company_channel,
        SUBSTR(c.create_time, 1, 10) AS create_time
    FROM claim_ods.accept_list_record a
    LEFT JOIN claim_ods.claim c ON a.accept_num = c.acceptance_no AND c.delete_flag = '0'
    WHERE a.claim_source in ( '2','4') AND NOT EXISTS (
        SELECT 1
        FROM claim_ods.bill b
        WHERE b.claim_id = c.id AND b.delete_flag = '0' AND b.treatment_date IS NULL
    )
    GROUP BY c.insure_company_channel, SUBSTR(c.create_time, 1, 10)
),
a2 AS (
    SELECT
        COUNT(DISTINCT c.id) AS mz_offline_claim_count,
        c.insure_company_channel,
        SUBSTR(c.create_time, 1, 10) AS create_time
    FROM claim_ods.accept_list_record a
    JOIN claim_ods.claim c ON a.accept_num = c.acceptance_no AND c.delete_flag = '0'
    WHERE a.claim_source in ( '1','3') AND NOT EXISTS (
        SELECT 1
        FROM claim_ods.bill b
        WHERE b.claim_id = c.id AND b.delete_flag = '0' AND b.treatment_date IS NULL
    )
    GROUP BY c.insure_company_channel, SUBSTR(c.create_time, 1, 10)
),
a3 AS (
    SELECT
        COUNT(DISTINCT c.id) AS hospital_claim_count,
        c.insure_company_channel,
        SUBSTR(c.create_time, 1, 10) AS create_time
    FROM claim_ods.accept_list_record a
    JOIN claim_ods.claim c ON a.accept_num = c.acceptance_no AND c.delete_flag = '0'
    WHERE EXISTS (
        SELECT 1
        FROM claim_ods.bill b
        WHERE b.claim_id = c.id AND b.delete_flag = '0' AND b.treatment_date IS NULL
    )
    GROUP BY c.insure_company_channel, SUBSTR(c.create_time, 1, 10)
),
a4 AS (
    SELECT
        pr.insure_company_channel,
        SUBSTR(pr.gmt_created, 1, 10) AS gmt_created,
        COUNT(DISTINCT pr.app_no) AS all_flow_claim_count
    FROM claim_ods.claim c
    INNER JOIN claim_ods.postback_record pr ON c.claim_no = pr.app_no AND pr.is_deleted = 'N'
    WHERE pr.is_deleted = 'N' AND (pr.postback_way = 'W' OR pr.postback_way IS NULL)
    GROUP BY pr.insure_company_channel, SUBSTR(pr.gmt_created, 1, 10)
),
a5 AS (
    SELECT
        pr.insure_company_channel,
        SUBSTR(pr.gmt_created, 1, 10) AS gmt_created,
        COUNT(DISTINCT pr.app_no) AS half_flow_claim_count
    FROM claim_ods.claim c
    INNER JOIN claim_ods.postback_record pr ON c.claim_no = pr.app_no AND pr.is_deleted = 'N'
    WHERE pr.is_deleted = 'N' AND pr.postback_way = 'H'
    GROUP BY pr.insure_company_channel, SUBSTR(pr.gmt_created, 1, 10)
)
SELECT
    t1.insure_company_channel,
    t1.create_time,
    t1.total_claim_count,
    t1.total_mz_claim_count,
    t1.hospital_claim_count,
    t1.mz_online_claim_count,
    t1.mz_offline_claim_count,
    t1.all_flow_claim_count,
    t1.half_flow_claim_count,
    t1.total_half_all_flow,
    REPLACE(t1.create_time, '-', '')
FROM (
    SELECT
        dim.channel_value AS insure_company_channel,
        a1.create_time,
        COALESCE(a1.mz_online_claim_count, 0) AS mz_online_claim_count,
        COALESCE(a2.mz_offline_claim_count, 0) AS mz_offline_claim_count,
        COALESCE(a3.hospital_claim_count, 0) AS hospital_claim_count,
        COALESCE(a1.mz_online_claim_count, 0) + COALESCE(a2.mz_offline_claim_count, 0) AS total_mz_claim_count,
        COALESCE(a1.mz_online_claim_count, 0) + COALESCE(a2.mz_offline_claim_count, 0) + COALESCE(a3.hospital_claim_count, 0) AS total_claim_count,
        COALESCE(a4.all_flow_claim_count, 0) AS all_flow_claim_count,
        COALESCE(a5.half_flow_claim_count, 0) AS half_flow_claim_count,
        COALESCE(a4.all_flow_claim_count, 0) + COALESCE(a5.half_flow_claim_count, 0) AS total_half_all_flow
    FROM a1
    LEFT JOIN claim_ods.dim_insure_company_channel dim ON a1.insure_company_channel = dim.channel_key
    LEFT JOIN a2 ON a1.insure_company_channel = a2.insure_company_channel AND a1.create_time = a2.create_time
    LEFT JOIN a3 ON a1.insure_company_channel = a3.insure_company_channel AND a1.create_time = a3.create_time
    LEFT JOIN a4 ON a1.insure_company_channel = a4.insure_company_channel AND a1.create_time = a4.gmt_created
    LEFT JOIN a5 ON a1.insure_company_channel = a5.insure_company_channel AND a1.create_time = a5.gmt_created
) t1;



"""
def truncate_table(table_name='CLAIM_DWD.DWD_BISIC_DATA_PANEL'):
    with DatabaseConnection() as conn:
        truncate_sql =  f"delete from  {table_name}   where INSURE_COMPANY_CHANNEL<>'太保产险大连分公司'  and  INSURE_COMPANY_CHANNEL<>'太保产险苏州分公司'  "
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
