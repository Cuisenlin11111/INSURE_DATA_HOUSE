import pymysql
# import sys
#
# sys.path.append(r"E:\pycharm\database")
from database import DatabaseConnection
from datetime import datetime
from datetime import datetime, timedelta


sql_query = f"""
  -- @description: 泰康辽宁分公司账单汇总
  -- @param:
  -- @return:
  -- @alter:
  -- @time: 2024-06-20 15:01:06
  -- @author: 01
  -- @version: 1.0.0

   -- 使用公共表达式（CTE）创建临时结果集tt
   INSERT INTO  CLAIM_DWD.DWD_TK10_TOTAL
WITH tt AS (
    -- 第一个子查询，处理特定条件下的案件数据并进行分组统计
    SELECT DISTINCT
        pr.ACCEPT_NUM AS 受理编号,
        -- 取时间字段的前7位作为时间维度（可能是按月份等统计）
        SUBSTR(pr.back_time, 1, 7) AS 时间,
        -- 根据回传方式及相关条件判断具体的回传方式描述
        CASE
            WHEN pr.postback_way = 'H' THEN '半流程'
            WHEN pr.postback_way = 'W' THEN '全流程'
            ELSE '未知'
        END AS 回传方式,
        -- 使用 coalesce 函数确保 C_CVRG_DESC 不为 null，若为 null 则设置为 ''
        coalesce(group_concat(DISTINCT inv.C_CVRG_DESC), '') AS 险种描述,
        COUNT(DISTINCT inv.C_INV_NO) AS 发票数,
        -- 如果发票数大于8，计算超出8的数量，否则为0
        CASE
            WHEN COUNT(DISTINCT inv.C_INV_NO) > 8 THEN COUNT(DISTINCT inv.C_INV_NO) - 8
            ELSE 0
        END AS chao8,
        '结案' AS is_back
    FROM claim_ods.postback_record pr
    LEFT JOIN claim_ods.`clm_visit_inv_info` inv ON pr.app_no = inv.C_CUSTOM_APP_NO AND inv.C_DEL_FLAG = '0'
    LEFT JOIN claim_ods.`accept_list_record` alr ON pr.accept_num = alr.accept_num
    WHERE pr.INSURE_COMPANY_CHANNEL = 'TK10'
      AND pr.back_status IN ('2', '21')
      AND pr.is_deleted = 'N'
    GROUP BY pr.ACCEPT_NUM
    UNION ALL
    SELECT
        alr.accept_num 受理编号,
        -- 根据条件判断时间取值
        CASE
            WHEN c.cancle_time IS NULL THEN SUBSTR(alr.T_UPD_TIME, 1, 7)
            ELSE SUBSTR(c.cancle_time, 1, 7)
        END AS 时间,
        -- 根据业务模式判断回传方式描述
        CASE
            WHEN alr.business_mode = 'I' THEN '半流程'
            WHEN alr.business_mode = 'A' THEN '全流程'
            ELSE ''
        END 回传方式,
        -- 使用 coalesce 函数确保 C_CVRG_DESC 不为 null，若为 null 则设置为 ''
        coalesce(group_concat(DISTINCT inv.C_CVRG_DESC), '') AS 险种描述,
        COUNT(DISTINCT inv.C_INV_NO) AS 发票数,
        -- 如果发票数大于8，计算超出8的数量，否则为0
        CASE
            WHEN COUNT(DISTINCT inv.C_INV_NO) > 8 THEN COUNT(DISTINCT inv.C_INV_NO) - 8
            ELSE 0
        END AS chao8,
        '撤案' AS is_back
    FROM claim_ods.accept_list_record alr
    LEFT JOIN claim_ods.claim c ON c.acceptance_no = alr.ACCEPT_NUM
    LEFT JOIN claim_ods.clm_app_info cai ON c.claim_no = cai.C_CUSTOM_APP_NO
                                          AND cai.C_DEL_FLAG = '0'
                                          AND cai.INSURANCE_COMPANY = 'TK10'
    LEFT JOIN claim_ods.`clm_visit_inv_info` inv ON cai.C_CUSTOM_APP_NO = inv.C_CUSTOM_APP_NO
                                                 AND cai.c_ply_no = inv.c_ply_no
                                                 AND inv.C_DEL_FLAG = '0'
                                                 AND inv.INSURANCE_COMPANY = 'TK10'
                                                 AND inv.C_IS_NEED_SHOW = '0'
    WHERE alr.INSURE_COMPANY_CHANNEL = 'TK10'
      AND alr.DEL_FLAG = '0'
      AND (alr.ACCEPT_STATUS = '5' OR c.clm_process_status = '11')
    GROUP BY alr.ACCEPT_NUM
),
-- 使用公共表达式（CTE）创建临时结果集ff，基于tt进行进一步聚合和计算价格等操作
ff AS (
    SELECT
        时间,
        '泰康养老辽宁分公司' AS INSURE_COMPANY_CHANNEL,
        险种描述,
        回传方式,
        is_back,
        受理编号,
        chao8,
        -- 根据不同的业务来源、回传方式和案件状态等条件确定价格
        CASE
            WHEN 回传方式 = '全流程' AND is_back = '结案' THEN 7.5
            WHEN 回传方式 = '全流程' AND is_back = '撤案' THEN 7.5*0.5
            WHEN 险种描述 LIKE '%门诊%' AND 回传方式 = '半流程' AND is_back = '结案' THEN 4.73
            WHEN 险种描述 NOT LIKE '%门诊%' AND 回传方式 = '半流程' AND is_back = '结案' THEN 5.4
            WHEN 险种描述 LIKE '%门诊%' AND 回传方式 = '半流程' AND is_back = '撤案' THEN 4.73*0.5
            WHEN 险种描述 NOT LIKE '%门诊%' AND 回传方式 = '半流程' AND is_back = '撤案' THEN 5.4*0.5
            ELSE 0
        END AS price
    FROM tt
)
-- 最终查询，从ff中选择相关字段，并进行类型转换和日期处理等操作后返回结果
SELECT
    INSURE_COMPANY_CHANNEL,
    时间,
    险种描述,
    回传方式,
    is_back,
    COUNT(1) AS 案件量,
    CAST(price AS DECIMAL(10, 2)) AS price,
    CAST(SUM(price)  AS DECIMAL(10, 2)),
    REPLACE(CURDATE(), '-', '')
FROM ff
GROUP BY INSURE_COMPANY_CHANNEL, 时间, 险种描述, 回传方式, is_back;


"""


def truncate_table(table_name='CLAIM_DWD.DWD_TK10_TOTAL'):
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