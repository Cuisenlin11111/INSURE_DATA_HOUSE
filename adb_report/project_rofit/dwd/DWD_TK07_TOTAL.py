import pymysql
# import sys
#
# sys.path.append(r"E:\pycharm\database")
from database import DatabaseConnection
from datetime import datetime
from datetime import datetime, timedelta

current_date = datetime.now()
# 计算6个月前的日期
six_months_ago_date = current_date - timedelta(days=15)
six_months = six_months_ago_date.strftime('%Y-%m')

sql_query = f"""
  -- @description: 泰康广东分公司账单汇总
  -- @param:
  -- @return:
  -- @alter:
  -- @time: 2024-06-20 15:01:06
  -- @author: 01
  -- @version: 1.0.0


   -- 使用公共表达式（CTE）创建临时结果集tt
insert into  CLAIM_DWD.DWD_TK07_TOTAL
WITH tt AS (
    -- 第一个子查询，处理特定条件下的案件数据并进行分组统计
    SELECT DISTINCT
        -- 根据claim_source的值转换为对应的业务来源描述（线下或线上）
        CASE
            WHEN TRIM(alr.claim_source) IN ('1', '3') THEN '线下'
            WHEN TRIM(alr.claim_source) IN ('2', '4') THEN '线上'
        END AS claim_source,
        pr.insurance_claim_no AS 受理编号,
        -- 取时间字段的前7位作为时间维度（可能是按月份等统计）
        SUBSTR(pr.back_time, 1, 7) AS 时间,
        -- 根据回传方式及相关条件判断具体的回传方式描述
        CASE
            WHEN pr.postback_way = 'H' AND INSTR(pr.insurance_claim_no, ',') > 0 THEN '半流程多案件'
            WHEN pr.postback_way = 'H' THEN '半流程单案件'
            WHEN pr.postback_way = 'W' THEN '全流程'
            ELSE '未知'
        END AS 回传方式,
        COUNT(DISTINCT inv.C_INV_NO) AS 发票数,
        -- 如果发票数大于8，计算超出8的数量，否则为0
        CASE
            WHEN COUNT(DISTINCT inv.C_INV_NO) > 8 THEN COUNT(DISTINCT inv.C_INV_NO) - 8
            ELSE 0
        END AS chao8,
        '结案' AS is_back
    FROM claim_ods.postback_record pr
    -- 左连接相关表，用于关联更多案件相关信息
    LEFT JOIN claim_ods.apply_claim ac ON pr.app_no = ac.apply_no AND ac.delete_flag = '0'
    LEFT JOIN claim_ods.claim_policy cp ON cp.policy_no = ac.policy_part_no
    LEFT JOIN claim_ods.claim_policy_customer cpc ON cpc.customer_no = cp.customer_no
    LEFT JOIN claim_ods.ply p ON cp.group_policy_no = p.C_PLY_NO
    LEFT JOIN claim_ods.`clm_visit_inv_info` inv ON pr.app_no = inv.C_CUSTOM_APP_NO AND inv.C_DEL_FLAG = '0'
    LEFT JOIN claim_ods.`accept_list_record` alr ON pr.accept_num = alr.accept_num
    LEFT JOIN claim_ods.claim c ON c.acceptance_no = alr.ACCEPT_NUM
    WHERE pr.INSURE_COMPANY_CHANNEL = 'TK07'
      AND cp.channel_group_policy_no NOT IN ('2853017521816', '2853017514284', '2853017405982', '2853017440422', '2853017337887', '2853017337885', '2853017337886', '2853017326791', '2853017326792', '2853017326788', '2853018603519', '2853018634607', '2853018703876','28530110279650','28530110359737','28530110382495')
      AND pr.back_status IN ('2', '21')
      AND pr.is_deleted = 'N'
      AND substr(pr.back_time,1,7)>='{six_months}'
    GROUP BY pr.insurance_claim_no

    UNION ALL

    -- 第二个子查询，处理另一种情况下的案件数据并进行分组统计
    SELECT DISTINCT
        CASE
            WHEN TRIM(alr.claim_source) IN ('1', '3') THEN '线下'
            WHEN TRIM(alr.claim_source) IN ('2', '4') THEN '线上'
        END AS claim_source,
        pr.insurance_case_no AS 受理编号,
        SUBSTR(pr.back_time, 1, 7) AS 时间,
        CASE
            WHEN pr1.postback_way = 'H' AND INSTR(pr1.insurance_claim_no, ',') > 0 THEN '半流程多案件'
            WHEN pr1.postback_way = 'H' THEN '半流程单案件'
            WHEN pr1.postback_way = 'W' THEN '全流程'
            ELSE '未知'
        END AS '回传方式',
        COUNT(DISTINCT inv.C_INV_NO) AS 发票数,
        CASE
            WHEN COUNT(DISTINCT inv.C_INV_NO) > 8 THEN COUNT(DISTINCT inv.C_INV_NO) - 8
            ELSE 0
        END AS chao8,
        '结案' AS is_back
    FROM claim_ods.`insurance_company_case` pr
    LEFT JOIN claim_ods.claim_policy cp ON cp.policy_no = pr.policy_no
    LEFT JOIN claim_ods.claim_policy_customer cpc ON cpc.customer_no = cp.customer_no
    LEFT JOIN claim_ods.ply p ON cp.group_policy_no = p.C_PLY_NO
    LEFT JOIN claim_ods.`clm_visit_inv_info` inv ON pr.app_no = inv.C_CUSTOM_APP_NO AND inv.C_DEL_FLAG = '0'
    LEFT JOIN claim_ods.`accept_list_record` alr ON pr.accept_num = alr.accept_num
    LEFT JOIN claim_ods.claim c ON c.acceptance_no = alr.ACCEPT_NUM
    LEFT JOIN claim_ods.postback_record pr1 ON pr.app_no = pr1.app_no AND pr1.is_deleted = 'N'
    WHERE pr.INSURE_COMPANY_CHANNEL = 'TK07'
      AND pr.is_deleted = 'N'
      AND pr.case_status IN ('05', '08')
      AND cp.channel_group_policy_no IN ('2853017521816', '2853017514284', '2853017405982', '2853017440422', '2853017337887', '2853017337885', '2853017337886', '2853017326791', '2853017326792', '2853017326788', '2853018603519', '2853018634607', '2853018703876','28530110279650','28530110359737','28530110382495')
    AND substr(pr.back_time,1,7)>='{six_months}'
    GROUP BY pr.insurance_case_no

    UNION ALL

    -- 第三个子查询，处理撤案相关的案件数据并进行分组统计
    SELECT
        CASE
            WHEN alr.claim_source = '1' THEN '线下'
            WHEN alr.claim_source = '2' THEN '线上'
            WHEN alr.claim_source = '3' THEN '线上转线下'
            WHEN alr.claim_source = '4' THEN '线下转线上'
        END AS claim_source,
        alr.ACCEPT_NUM AS 受理编号,
        -- 根据案件撤销时间是否为空来确定时间取值
        CASE
            WHEN c.cancle_time IS NULL THEN SUBSTR(alr.T_UPD_TIME, 1, 7)
            ELSE SUBSTR(c.cancle_time, 1, 7)
        END AS 时间,
        CASE alr.business_mode
            WHEN 'I' THEN '半流程'
            WHEN 'A' THEN '全流程'
            ELSE '全流程'
        END AS 回传方式,
        COUNT(DISTINCT inv.C_INV_NO) AS 发票数,
        CASE
            WHEN COUNT(DISTINCT inv.C_INV_NO) > 8 THEN COUNT(DISTINCT inv.C_INV_NO) - 8
            ELSE 0
        END AS chao8,
        '撤案' AS is_back
    FROM claim_ods.accept_list_record alr
    LEFT JOIN claim_ods.claim c ON c.acceptance_no = alr.ACCEPT_NUM
    LEFT JOIN claim_ods.apply_claim ac ON c.claim_no = ac.apply_no
    LEFT JOIN claim_ods.claim_policy cp ON cp.policy_no = ac.policy_part_no
    LEFT JOIN claim_ods.claim_policy_customer cpc ON cpc.customer_no = cp.customer_no
    LEFT JOIN claim_ods.ply p ON cp.group_policy_no = p.C_PLY_NO
    LEFT JOIN claim_ods.`clm_visit_inv_info` inv ON c.claim_no = inv.C_CUSTOM_APP_NO AND inv.C_DEL_FLAG = '0'
    WHERE alr.INSURE_COMPANY_CHANNEL = 'TK07'
      AND alr.DEL_FLAG = '0'
      AND (alr.ACCEPT_STATUS = '5' OR c.clm_process_status = '11')
      AND (substr(c.cancle_time,1,7)>='{six_months}' OR substr(alr.T_UPD_TIME,1,7)>='{six_months}')
    GROUP BY alr.ACCEPT_NUM
),
-- 使用公共表达式（CTE）创建临时结果集ff，基于tt进行进一步聚合和计算价格等操作
ff AS (
    SELECT
        时间,
        '泰康养老广东分公司' AS INSURE_COMPANY_CHANNEL,
        claim_source,
        回传方式,
        is_back,
        受理编号,
        chao8,
        -- 根据不同的业务来源、回传方式和案件状态等条件确定价格
        CASE
            WHEN claim_source = '线上' AND 回传方式 = '全流程' AND is_back = '结案' THEN 13.8
            WHEN claim_source = '线上' AND 回传方式 = '半流程单案件' AND is_back = '结案' THEN 12.5
            WHEN claim_source = '线上' AND 回传方式 = '半流程多案件' AND is_back = '结案' THEN 6.2
            WHEN claim_source = '线下' AND 回传方式 = '全流程' AND is_back = '结案' THEN 16.8
            WHEN claim_source = '线下' AND 回传方式 = '半流程单案件' AND is_back = '结案' THEN 15.5
            WHEN claim_source = '线下' AND 回传方式 = '半流程多案件' AND is_back = '结案' THEN 9.2
            WHEN claim_source = '线下' AND 回传方式 = '半流程' AND is_back = '撤案' THEN 6.9
            WHEN claim_source IN ('线上', '线上转线下') AND 回传方式 IN ('半流程') AND is_back = '撤案' THEN 6.3
            WHEN claim_source = '线下' AND 回传方式 = '全流程' AND is_back = '撤案' THEN 8.4
            WHEN claim_source IN ('线上', '线上转线下') AND 回传方式 IN ('全流程') AND is_back = '撤案' THEN 7.8
            ELSE 0
        END AS price
    FROM tt
)
-- 最终查询，从ff中选择相关字段，并进行类型转换和日期处理等操作后返回结果
SELECT
    INSURE_COMPANY_CHANNEL,
    时间,
    claim_source,
    回传方式,
    is_back,
    count(1) AS 案件量,
    CAST(price AS DECIMAL(10, 2)) AS price,
    CAST( (sum(price) + sum(price*chao8/8))*0.9 AS DECIMAL(10, 2)),
    REPLACE(CURDATE(), '-', '')
FROM ff  group by INSURE_COMPANY_CHANNEL, 时间, claim_source, 回传方式, is_back ;

"""


def truncate_table(table_name='CLAIM_DWD.DWD_TK07_TOTAL'):
    with DatabaseConnection() as conn:
        truncate_sql = f"delete from  {table_name}  where  YEAR_MONTH>='{six_months}' "
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