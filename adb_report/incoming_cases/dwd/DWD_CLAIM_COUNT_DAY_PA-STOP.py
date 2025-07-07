# import sys
# sys.path.append(r"E:\pycharm\database")
import pymysql
from database import DatabaseConnection
from datetime import datetime, date, timedelta


# 获取当前日期时间
now = datetime.now()
# 计算 60 天前的日期时间
ago_60_days = now - timedelta(days=10)
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
   INSERT INTO CLAIM_DWD.DWD_CLAIM_COUNT_DAY
   WITH main_query AS (
    SELECT
        -- 进行产品类型的映射
        CASE
            WHEN alr.product_type = 'P' THEN '平安产险_个'
            WHEN alr.product_type = 'G' THEN '平安产险_团'
            WHEN alr.product_type = 'C' THEN '平安产险_车险'
            ELSE '平安产险_雇主'
        END AS insure_company_channel,
        -- 提取日期部分
        SUBSTR(alr.T_CRT_TIME, 1, 10) AS T_CRT_TIME,
        SUBSTR(c.create_time, 1, 10) AS lr_time,
        SUBSTR(pr.back_time, 1, 10) AS back_time,
        SUBSTR(q.gmt_created, 1, 10) AS question_time,
        -- 其他字段
        alr.accept_num,
        alr.claim_source,
        alr.ACCEPT_STATUS,
        pr.back_status,
        pr.postback_way,
        cat.C_HANDLE_CDE,
        cat.C_REVIEWER_CDE,
        cat.C_REVIEWER_STAFF,
        SUBSTR(cat.T_CRT_TM, 1, 10) T_CRT_TM,
        p.C_CUSTOM_PLY_NO,
        b.treatment_date,
        -- 判断治疗类型
        CASE
            WHEN b.treatment_date IS NULL THEN '住院'
            ELSE '门诊'
        END AS treatment_type
    FROM claim_ods.accept_list_record alr
    -- 连接 claim 表并添加筛选条件
    LEFT JOIN claim_ods.claim c ON alr.accept_num = c.acceptance_no
        AND c.delete_flag = '0'
        AND c.insure_company_channel = 'PA02'
    -- 连接 postback_record 表并添加筛选条件
    LEFT JOIN claim_ods.postback_record pr ON pr.accept_num = alr.accept_num
        AND pr.is_deleted = 'N'
        AND pr.insure_company_channel = 'PA02'
    -- 连接 case_audit_task 表并添加筛选条件
    LEFT JOIN claim_ods.case_audit_task cat ON cat.c_claim_case_no = pr.app_no
        AND cat.insure_company_channel = 'PA02'
    -- 连接 question_claim 表并添加筛选条件
    LEFT JOIN claim_ods.question_claim q ON c.claim_no = q.claim_no
        AND q.belong_company = 'PA02'
        AND q.is_deleted = 'N'
    -- 连接 bill 表并添加筛选条件
    LEFT JOIN claim_ods.bill b ON b.claim_id = c.id
        AND b.delete_flag = '0'
        AND b.insure_company_channel = 'PA02'
    -- 连接 apply_claim 表
    LEFT JOIN claim_ods.apply_claim ac ON c.claim_no = ac.apply_no
        AND ac.delete_flag = '0'
    -- 连接 claim_policy 表
    LEFT JOIN claim_ods.claim_policy cp ON cp.policy_no = ac.policy_part_no
        AND cp.is_deleted = 'N'
        AND cp.insure_company_channel = 'PA02'
    -- 连接 ply 表
    LEFT JOIN claim_ods.ply p ON cp.group_policy_no = p.C_PLY_NO
    WHERE alr.DEL_FLAG = '0'
        AND alr.insure_company_channel = 'PA02'
        AND alr.ACCEPT_STATUS <> '1'
        AND SUBSTR(alr.T_CRT_TIME, 1, 10) >= '{formatted_date}'
),
   query AS (
            SELECT
                 CASE
            WHEN alr.product_type = 'P' THEN '平安产险_个'
            WHEN alr.product_type = 'G' THEN '平安产险_团'
            WHEN alr.product_type = 'C' THEN '平安产险_车险'
            ELSE ''
        END AS insure_company_channel,
        substr(cat.T_CRT_TM,1,10) comm_date,
        COUNT(DISTINCT CASE WHEN C_HANDLE_CDE = '1' THEN c_claim_case_no ELSE NULL END) num,
        COUNT(DISTINCT c_claim_case_no) sh_claim_count,
        COUNT(DISTINCT CASE WHEN C_HANDLE_CDE = '1' THEN c_claim_case_no ELSE NULL END) / COUNT(DISTINCT c_claim_case_no) AS shzdhl
    FROM claim_ods.case_audit_task cat
    left join claim_ods.claim c on  c.claim_no = cat.C_CLAIM_CASE_NO and c.insure_company_channel='PA02'
    left join  claim_ods.accept_list_record alr on c.acceptance_no = alr.ACCEPT_NUM AND alr.INSURE_COMPANY_CHANNEL='PA02'
    where cat.insure_company_channel = 'PA02' and cat.C_DEL_FLAG = '0' and  substr(cat.T_CRT_TM,1,10)>= '{formatted_date}'
    GROUP BY CASE
            WHEN alr.product_type = 'P' THEN '平安产险_个'
            WHEN alr.product_type = 'G' THEN '平安产险_团'
            WHEN alr.product_type = 'C' THEN '平安产险_车险'
            ELSE ''
        END , substr(cat.T_CRT_TM,1,10)
    order by  substr(cat.T_CRT_TM,1,10)   desc

   ),
subquery AS (
SELECT
    insure_company_channel,
    -- 选择时间列
    T_CRT_TIME AS comm_date,
    -- 案件量统计
    COUNT(distinct accept_num ) AS jjl,
    COUNT(CASE WHEN ACCEPT_STATUS = '5' THEN accept_num END) AS cancel_vol,
    COUNT(CASE WHEN claim_source = '1' THEN accept_num END) AS xxqd,
    COUNT(CASE WHEN claim_source = '2' THEN accept_num END) AS xsqd,
    COUNT(CASE WHEN lr_time IS NOT NULL THEN accept_num END) AS lrl,
    COUNT(DISTINCT CASE WHEN back_status IN ('2', '21') THEN accept_num END) AS drhcal,
    COUNT(DISTINCT CASE WHEN back_status IN ('2', '21') AND postback_way = 'H' THEN accept_num END) AS blchcal,
    COUNT(DISTINCT CASE WHEN back_status IN ('2', '21') AND (postback_way = 'W' OR postback_way IS NULL) THEN accept_num END) AS qlchcal,
    COUNT(DISTINCT CASE WHEN back_status = '3' THEN accept_num END) AS hcsbal,
    COUNT(DISTINCT CASE WHEN C_HANDLE_CDE = '1' AND back_status IN ('2', '21') THEN accept_num END) AS zdshal,
    COUNT(DISTINCT CASE WHEN C_REVIEWER_CDE = '1' AND back_status IN ('2', '21') THEN accept_num END) AS fh_auto_claim_count,
    COUNT(DISTINCT CASE WHEN question_time IS NOT NULL THEN accept_num END) AS 问题件案件数,
    COUNT(DISTINCT CASE WHEN treatment_type = '住院' THEN accept_num END) AS 住院案件数,
    -- 计算审核自动化率
    COUNT(DISTINCT CASE WHEN C_HANDLE_CDE = '1' THEN accept_num END) num,
    COUNT(DISTINCT CASE WHEN C_HANDLE_CDE = '1' THEN accept_num END) / COUNT(accept_num) AS shzdhl,
    COUNT(accept_num) AS sh_claim_count
FROM main_query
GROUP BY insure_company_channel, T_CRT_TIME
-- 计算百分比和其他指标
HAVING (COUNT(CASE WHEN claim_source <> '1' AND claim_source <> '2' THEN accept_num END) > 0 OR COUNT(DISTINCT CASE WHEN back_status IN ('2', '21') THEN accept_num END) > 0)
    AND T_CRT_TIME >= '{formatted_date}')
SELECT
    DISTINCT
    subquery.insure_company_channel,
    subquery.comm_date,
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
    query.num ZDSHAL_RW,
    CAST(CASE
             WHEN COALESCE(qlchcal, 0) = 0 THEN 0
             ELSE COALESCE(zdshal, 0) / COALESCE(qlchcal, 0) END AS DECIMAL(10, 4)) shzdhl,
    CAST(CASE WHEN query.shzdhl > 1 THEN 1 ELSE query.shzdhl END AS DECIMAL(10, 4)),
    fh_auto_claim_count,
    CAST(CASE
             WHEN COALESCE(qlchcal, 0) = 0 THEN 0
             ELSE COALESCE(fh_auto_claim_count, 0) / COALESCE(qlchcal, 0) END AS DECIMAL(10, 4)) fhzdhl,
    问题件案件数 question_claim_vol,
     住院案件数 hospatal_claim_vol,
    REPLACE(SUBSTRING(subquery.comm_date, 1, 10), '-', ''),
    query.sh_claim_count
FROM subquery
   left join query   on  subquery.insure_company_channel = query.insure_company_channel and subquery.comm_date=query.comm_date
"""


def truncate_table(table_name='CLAIM_DWD.DWD_CLAIM_COUNT_DAY'):
    with DatabaseConnection() as conn:
        truncate_sql = f"delete  from  {table_name}  where  insure_company_channel  in ('平安产险_个','平安产险_团','平安产险_车险','平安产险_雇主')  and gmt_created>='{formatted_date}'"
        with conn.cursor() as cursor:
            cursor.execute(truncate_sql)
            conn.commit()


def insert_data(sql_query):
    with DatabaseConnection() as conn:
        with conn.cursor() as cursor:
            cursor.execute(sql_query)



if __name__ == "__main__":
    start_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    print("程序开始时间：", start_time)
    truncate_table()
    insert_data(sql_query)
    end_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    print("程序结束时间：", end_time)