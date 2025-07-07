# import sys
# sys.path.append(r"E:\pycharm\database")
from database import DatabaseConnection
from datetime import datetime



sql_query = f"""
  -- @description: 人工审核原因分析表分析
  -- @param:
  -- @return:
  -- @alter:
  -- @time: 2024-10-12 15:01:06
  -- @author: 01
  -- @version: 1.0.0
INSERT INTO CLAIM_DWD.DWD_VERIFY_REASON_ANALY (
    insure_company_channel,               -- 渠道
    gmt_created,                          -- 进件日期
    rule_code,                            -- 规则代码
    rule_desc,                            -- 规则描述
    count_busi,                           -- 规则量
    data_dt                               -- 调度日期
)
SELECT
    dim.channel_value AS insure_company_channel,
    a.gmt_created,
    a.rule_code,
    a.rule_desc,
    COUNT(DISTINCT a.business_key) AS count_busi,
    REPLACE(SUBSTR(a.gmt_created, 1, 10), '-', '') AS data_dt
FROM (
    SELECT DISTINCT
        t.business_key,
        t.rule_code,
        r.tips AS rule_desc,
        t.insure_company_channel,
        a1.gmt_created
    FROM claim_ods.trigger_rules_log t
    INNER JOIN claim_ods.rule r ON r.rule_code = t.rule_code
    LEFT JOIN (
        SELECT DISTINCT
            t.business_key AS app_no,
            p2.insure_company_channel,
            p2.gmt_created
        FROM claim_ods.trigger_rules_log t
        INNER JOIN claim_ods.rule r ON r.rule_code = t.rule_code AND r.rule_code NOT IN ('AA0000000001')
        LEFT JOIN (
            SELECT
                pr.app_no,
                pr.insure_company_channel,
                SUBSTR(pr.gmt_created, 1, 10) AS gmt_created
            FROM claim_ods.postback_record pr
            WHERE pr.back_status IN ('2', '21', '3') AND pr.is_deleted = 'N' AND pr.postback_way = 'W'
        ) p2 ON t.business_key = p2.app_no AND t.insure_company_channel = p2.insure_company_channel
        UNION
        SELECT DISTINCT
            pr.app_no,
            pr.insure_company_channel,
            SUBSTR(pr.gmt_created, 1, 10) AS gmt_created
        FROM claim_ods.postback_record pr
        WHERE pr.back_status IN ('2', '21') AND pr.is_deleted = 'N' AND pr.postback_way = 'W'
    ) a1 ON t.business_key = a1.app_no AND t.insure_company_channel = a1.insure_company_channel
) a
INNER JOIN claim_ods.dim_insure_company_channel dim ON a.insure_company_channel = dim.channel_key
WHERE a.rule_code NOT IN ('D00000000001', 'D00000000002')
  AND (a.rule_desc NOT LIKE '存在风控结论' AND a.rule_desc NOT LIKE '%复审%')
  AND a.gmt_created IS NOT NULL
GROUP BY dim.channel_value, a.gmt_created, a.rule_code, a.rule_desc
ORDER BY count_busi DESC;


"""
def truncate_table(table_name='CLAIM_DWD.DWD_VERIFY_REASON_ANALY'):
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
