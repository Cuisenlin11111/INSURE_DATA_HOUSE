# import sys
# sys.path.append(r"E:\pycharm\database")
import pymysql
from database import DatabaseConnection
from datetime import datetime, date, timedelta


# 获取当前日期时间
now = datetime.now()
# 计算 60 天前的日期时间
ago_60_days = now - timedelta(days=30)
# 格式化为指定的字符串格式
formatted_date = ago_60_days.strftime('%Y-%m-%d')


sql_query = f"""
  -- @description: 案件量统计报表基础数据
  -- @param:
  -- @return:
  -- @alter:
  -- @time: 2024-08-29 15:01:06
  -- @author: 01
  -- @version: 1.0.0

-- 创建临时表 tmp_00
WITH tmp_00 AS (
    SELECT
        alr.accept_num,
        alr.claim_source,
        alr.ACCEPT_STATUS,
        CASE
            WHEN alr.product_type = 'P' THEN '平安产险_个'
            WHEN alr.product_type = 'G' THEN '平安产险_团'
            WHEN alr.product_type = 'C' THEN '平安产险_车险'
            ELSE ''
        END AS insure_company_channel,
        SUBSTR(alr.T_CRT_TIME, 1, 10) AS T_CRT_TIME,
        SUBSTR(c.create_time, 1, 10) AS lr_time,
        SUBSTR(pr.back_time, 1, 10) AS back_time,
        SUBSTR(q.gmt_created, 1, 10) AS question_time,
        pr.back_status,
        pr.postback_way,
        cat.C_HANDLE_CDE,
        cat.C_REVIEWER_CDE,
        cat.C_REVIEWER_STAFF,
        SUBSTR(cat.T_CRT_TM, 1, 10) T_CRT_TM,
        p.C_CUSTOM_PLY_NO,
        b.treatment_date
    FROM claim_ods.accept_list_record alr
    LEFT JOIN claim_ods.claim c ON alr.accept_num = c.acceptance_no
        AND c.delete_flag = '0'
        AND c.insure_company_channel = 'PA02'
    LEFT JOIN claim_ods.postback_record pr ON pr.accept_num = alr.accept_num
        AND pr.is_deleted = 'N'
        AND pr.insure_company_channel = 'PA02'
    LEFT JOIN claim_ods.case_audit_task cat ON cat.c_claim_case_no = pr.app_no
        AND cat.insure_company_channel = 'PA02'
    LEFT JOIN claim_ods.question_claim q ON c.claim_no = q.claim_no
        AND q.belong_company = 'PA02'
        AND q.is_deleted = 'N'
    LEFT JOIN claim_ods.bill b ON b.claim_id = c.id
        AND b.delete_flag = '0'
        AND b.insure_company_channel = 'PA02'
    LEFT JOIN claim_ods.apply_claim ac ON c.claim_no = ac.apply_no
        AND ac.delete_flag = '0'
    LEFT JOIN claim_ods.claim_policy cp ON cp.policy_no = ac.policy_part_no
        AND cp.is_deleted = 'N'
        AND cp.insure_company_channel = 'PA02'
    LEFT JOIN claim_ods.ply p ON cp.group_policy_no = p.C_PLY_NO
    WHERE alr.DEL_FLAG = '0'
        AND alr.insure_company_channel = 'PA02'
        AND alr.ACCEPT_STATUS <> '1'
        AND SUBSTR(alr.T_CRT_TIME, 1, 10) >= '{formatted_date}'
),
-- 创建临时表 tmp_01
tmp_01 AS (
    SELECT
        accept_num,
        claim_source,
        ACCEPT_STATUS,
        CASE
            WHEN insure_company_channel = '平安产险_个' AND bb.CUSTOM_PLY_NO IS NULL THEN '平安产险_个'
            WHEN insure_company_channel = '平安产险_团' AND bb.CUSTOM_PLY_NO IS NULL THEN '平安产险_团'
            WHEN insure_company_channel = '平安产险_车险' OR bb.CUSTOM_PLY_NO IS NOT NULL THEN '平安产险_车险'
            ELSE '平安产险_雇主'
        END AS insure_company_channel,
        T_CRT_TIME,
        lr_time,
        back_time,
        question_time,
        back_status,
        postback_way,
        C_HANDLE_CDE,
        C_REVIEWER_CDE,
        C_REVIEWER_STAFF,
        T_CRT_TM,
        C_CUSTOM_PLY_NO,
        CASE
            WHEN EXISTS (
                SELECT 1
                FROM tmp_00 subquery_tmp
                WHERE subquery_tmp.ACCEPT_NUM = aa.ACCEPT_NUM
                  AND subquery_tmp.treatment_date IS NULL
            ) THEN '住院'
            ELSE '门诊'
        END AS treatment_type
    FROM tmp_00 aa
    LEFT JOIN CLAIM_DIM.DIM_PA02_CUSTOM_PLY_NO bb ON aa.C_CUSTOM_PLY_NO = bb.CUSTOM_PLY_NO
    GROUP BY accept_num
),
-- 创建临时表 b1
b1 AS (
    SELECT
        insure_company_channel,
        T_CRT_TIME comm_date,
        COUNT(*) jjl
    FROM tmp_01
    GROUP BY insure_company_channel, T_CRT_TIME
),
-- 创建临时表 ba
ba AS (
    SELECT
        T_CRT_TIME comm_date,
        insure_company_channel,
        COUNT(*) cancel_vol
    FROM tmp_01 WHERE ACCEPT_STATUS = '5'
    GROUP BY insure_company_channel, T_CRT_TIME
),
-- 创建临时表 b2
b2 AS (
    SELECT
        T_CRT_TIME comm_date,
        insure_company_channel,
        COUNT(*) xxqd
    FROM tmp_01 WHERE claim_source = '1'
    GROUP BY insure_company_channel, comm_date
),
-- 创建临时表 b3
b3 AS (
    SELECT
        T_CRT_TIME comm_date,
        insure_company_channel,
        COUNT(*) xsqd
    FROM tmp_01 WHERE claim_source = '2'
    GROUP BY insure_company_channel, comm_date
),
-- 创建临时表 b4
b4 AS (
    SELECT
        lr_time comm_date,
        insure_company_channel,
        COUNT(*) lrl
    FROM tmp_01
    GROUP BY insure_company_channel, comm_date
),
-- 创建临时表 b5
b5 AS (
    SELECT
        back_time comm_date,
        insure_company_channel,
        COUNT(DISTINCT accept_num) drhcal
    FROM tmp_01 WHERE back_status IN ('2', '21')
    GROUP BY insure_company_channel, comm_date
),
-- 创建临时表 b6
b6 AS (
    SELECT
        back_time comm_date,
        insure_company_channel,
        COUNT(DISTINCT accept_num) blchcal
    FROM tmp_01 WHERE back_status IN ('2', '21') AND postback_way = 'H'
    GROUP BY insure_company_channel, comm_date
),
-- 创建临时表 b7
b7 AS (
    SELECT
        back_time comm_date,
        insure_company_channel,
        COUNT(DISTINCT accept_num) qlchcal
    FROM tmp_01 WHERE back_status IN ('2', '21') AND (postback_way = 'W' OR postback_way IS NULL)
    GROUP BY insure_company_channel, comm_date
),
-- 创建临时表 b8
b8 AS (
    SELECT
        back_time comm_date,
        insure_company_channel,
        COUNT(DISTINCT accept_num) hcsbal
    FROM tmp_01 WHERE back_status = '3'
    GROUP BY insure_company_channel, comm_date
),
-- 创建临时表 b9
b9 AS (
    SELECT
        back_time comm_date,
        insure_company_channel,
        COUNT(DISTINCT accept_num) zdshal
    FROM tmp_01 WHERE C_HANDLE_CDE = '1' AND back_status IN ('2', '21')
    GROUP BY insure_company_channel, comm_date
),
-- 创建临时表 b10
b10 AS (
    SELECT
        insure_company_channel,
        back_time comm_date,
        COUNT(DISTINCT accept_num) fh_auto_claim_count
    FROM tmp_01 WHERE C_REVIEWER_CDE = '1' AND back_status IN ('2', '21')
    GROUP BY insure_company_channel, comm_date
),
-- 创建临时表 b11
b11 AS (
    SELECT
        insure_company_channel,
        question_time comm_date,
        COUNT(DISTINCT accept_num) 问题件案件数
    FROM tmp_01
    GROUP BY insure_company_channel, comm_date
),
-- 创建临时表 b12
b12 AS (
    SELECT
        insure_company_channel,
        T_CRT_TIME comm_date,
        COUNT(DISTINCT accept_num) 住院案件数
    FROM tmp_01 WHERE treatment_type = '住院'
    GROUP BY insure_company_channel, comm_date
),
-- 创建临时表 b13
b13 AS (
    SELECT
        insure_company_channel,
        T_CRT_TM comm_date,
        COUNT(DISTINCT CASE WHEN C_HANDLE_CDE = '1' THEN accept_num ELSE NULL END) num,
        COUNT(DISTINCT accept_num) sh_claim_count,
        COUNT(DISTINCT CASE WHEN C_HANDLE_CDE = '1' THEN accept_num ELSE NULL END) / COUNT(DISTINCT accept_num) AS shzdhl
    FROM tmp_01
    GROUP BY insure_company_channel, comm_date
),
-- 创建临时表 b14
b14 AS (
    SELECT DISTINCT
        b1.insure_company_channel,
        b1.comm_date gmt_created,
        COALESCE(b1.jjl, 0) jjl,
        COALESCE(ba.cancel_vol, 0) cancel_vol,
        COALESCE(b2.xxqd, 0) xxqd,
        COALESCE(b3.xsqd, 0) xsqd,
        COALESCE(b4.lrl, 0) lrl,
        COALESCE(b5.drhcal, 0) drhcal,
        COALESCE(b6.blchcal, 0) blchcal,
        COALESCE(b7.qlchcal, 0) qlchcal,
        COALESCE(b8.hcsbal, 0) hcsbal,
        COALESCE(b9.zdshal, 0) zdshal,
        COALESCE(b13.shzdhl, 0) AS shzdhl_rw,
        COALESCE(b13.num, 0) AS ZDSHAL_RW,
        COALESCE(b10.fh_auto_claim_count, 0) fh_auto_claim_count,
        COALESCE(b11.问题件案件数, 0) question_claim_vol,
        COALESCE(b12.住院案件数, 0) hospatal_claim_vol,
        b1.comm_date,
        b13.sh_claim_count
    FROM b1
    LEFT JOIN ba ON b1.comm_date = ba.comm_date AND b1.insure_company_channel = ba.insure_company_channel
    LEFT JOIN b2 ON b1.comm_date = b2.comm_date AND b1.insure_company_channel = b2.insure_company_channel
    LEFT JOIN b3 ON b1.comm_date = b3.comm_date AND b1.insure_company_channel = b3.insure_company_channel
    LEFT JOIN b4 ON b1.comm_date = b4.comm_date AND b1.insure_company_channel = b4.insure_company_channel
    LEFT JOIN b5 ON b1.comm_date = b5.comm_date AND b1.insure_company_channel = b5.insure_company_channel
    LEFT JOIN b6 ON b1.comm_date = b6.comm_date AND b1.insure_company_channel = b6.insure_company_channel
    LEFT JOIN b7 ON b1.comm_date = b7.comm_date AND b1.insure_company_channel = b7.insure_company_channel
    LEFT JOIN b8 ON b1.comm_date = b8.comm_date AND b1.insure_company_channel = b8.insure_company_channel
    LEFT JOIN b9 ON b1.comm_date = b9.comm_date AND b1.insure_company_channel = b9.insure_company_channel
    LEFT JOIN b10 ON b1.comm_date = b10.comm_date AND b1.insure_company_channel = b10.insure_company_channel
    LEFT JOIN b11 ON b1.comm_date = b11.comm_date AND b1.insure_company_channel = b11.insure_company_channel
    LEFT JOIN b12 ON b1.comm_date = b12.comm_date AND b1.insure_company_channel = b12.insure_company_channel
    LEFT JOIN b13 ON b1.comm_date = b13.comm_date AND b1.insure_company_channel = b13.insure_company_channel
    WHERE (COALESCE(b1.jjl, 0) > 0 OR COALESCE(b5.drhcal, 0) > 0) AND b1.comm_date >= '{formatted_date}'
)

SELECT
    DISTINCT
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
    CAST(CASE
             WHEN COALESCE(drhcal, 0) = 0 THEN 0
             ELSE COALESCE(qlchcal, 0) / COALESCE(drhcal, 0) END AS DECIMAL(10, 4)) perc_all_flow,
    hcsbal,
    zdshal,
    ZDSHAL_RW,
    CAST(CASE
             WHEN COALESCE(qlchcal, 0) = 0 THEN 0
             ELSE COALESCE(zdshal, 0) / COALESCE(qlchcal, 0) END AS DECIMAL(10, 4)) shzdhl,
    CAST(CASE WHEN shzdhl_rw > 1 THEN 1 ELSE shzdhl_rw END AS DECIMAL(10, 4)),
    fh_auto_claim_count,
    CAST(CASE
             WHEN COALESCE(qlchcal, 0) = 0 THEN 0
             ELSE COALESCE(fh_auto_claim_count, 0) / COALESCE(qlchcal, 0) END AS DECIMAL(10, 4)) fhzdhl,
    question_claim_vol,
    hospatal_claim_vol,
    REPLACE(SUBSTRING(gmt_created, 1, 10), '-', ''),
    sh_claim_count
FROM b14
WHERE insure_company_channel IS NOT NULL and  gmt_created>='{formatted_date}';
"""


def truncate_table(table_name='CLAIM_DWD.DWD_CLAIM_COUNT_DAY'):
    with DatabaseConnection() as conn:
        truncate_sql = f"delete  from  {table_name}  where  insure_company_channel  in ('平安产险_个','平安产险_团','平安产险_车险','平安产险_雇主')  and gmt_created>='{formatted_date}'"
        with conn.cursor() as cursor:
            cursor.execute(truncate_sql)
            conn.commit()


def insert_data(sql_query):
    insert_sql = """
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
     values (%s, %s, %s, %s, %s, %s, %s, %s, %s)
        """
    batch_size = 1000  # 定义分批插入的批次大小
    with DatabaseConnection() as conn:
        with conn.cursor() as cursor:
            cursor.execute(sql_query)
            data = []
            while True:
                row = cursor.fetchone()
                if row is None:
                    break
                # 确保 row 的元素数量与占位符数量一致
                if len(row) == 8:
                    data.append(row)
                    if len(data) >= batch_size:
                        cursor.executemany(insert_sql, data)
                        data = []
            if data:
                cursor.executemany(insert_sql, data)
            conn.commit()


if __name__ == "__main__":
    start_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    print("程序开始时间：", start_time)
    truncate_table()
    insert_data(sql_query)
    end_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    print("程序结束时间：", end_time)