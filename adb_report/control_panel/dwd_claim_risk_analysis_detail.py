# import sys
# sys.path.append(r"E:\pycharm\database")
from database import DatabaseConnection
from datetime import datetime



sql_query = f"""
  -- @description: 控制台明细数据
  -- @param:
  -- @return:
  -- @alter:
  -- @time: 2024-06-20 15:01:06
  -- @author: 01
  -- @version: 1.0.0
insert into claim_dwd.`dwd_claim_risk_analysis_detail`
SELECT
    s_dim.channel_value AS 渠道,
    s_dim.channel_order AS 排序,
    s_dim.channel_status AS 案件状态,
    CASE
        WHEN total IS null THEN 0
        ELSE total
    END 总计,
    CASE
        WHEN s_dim.channel_status NOT in('当天进件量','当天完成量') AND distribution IS null THEN 0
        ELSE distribution
    END 分配,
    CASE
        WHEN s_dim.channel_status NOT in('当天进件量','当天完成量') AND preAudit IS null THEN 0
        ELSE preAudit
    END 预审,
    CASE
        WHEN s_dim.channel_status NOT in('当天进件量','当天完成量') AND match_sum IS null THEN 0
        ELSE match_sum
    END 匹配,
    CASE
        WHEN s_dim.channel_status NOT in('当天进件量','当天完成量') AND sign_sum IS null THEN 0
        ELSE sign_sum
    END 规则匹配,
    CASE
        WHEN s_dim.channel_status NOT in('当天进件量','当天完成量') AND deduct IS null THEN 0
        ELSE deduct
    END 扣费,
    CASE
        WHEN s_dim.channel_status NOT in('当天进件量','当天完成量') AND autoAudit IS null THEN 0
        ELSE autoAudit
    END 自动审核,
    CASE
        WHEN s_dim.channel_status NOT in('当天进件量','当天完成量') AND audit_sum IS null THEN 0
        ELSE audit_sum
    END 审核,
    CASE
        WHEN s_dim.channel_status NOT in('当天进件量','当天完成量') AND review IS null THEN 0
        ELSE review
    END 复核,
    CASE
        WHEN s_dim.channel_status NOT in('当天进件量','当天完成量') AND insureCompanyReview IS null THEN 0
        ELSE insureCompanyReview
    END 保司复核,
    CASE
        WHEN s_dim.channel_status NOT in('当天进件量','当天完成量') AND pending IS null THEN 0
        ELSE pending
    END 待回销,
    CASE
        WHEN s_dim.channel_status NOT in('当天进件量','当天完成量') AND postback IS null THEN 0
        ELSE postback
    END 回传,
    CASE
        WHEN s_dim.channel_status NOT in('当天进件量','当天完成量') AND entry IS null THEN 0
        ELSE entry
    END 录入,
    CASE
        WHEN s_dim.channel_status NOT in('当天进件量','当天完成量') AND quest IS null THEN 0
        ELSE quest
    END 问题件,
   case when  s_dim.channel_status ='待处理（总）' then 1
         when  s_dim.channel_status ='今日待处理' then 2
         when  s_dim.channel_status ='超时' then 3
         when  s_dim.channel_status ='预警' then 4 else '' end  状态排序,
    case when s_dim.channel_value = '中智'  then  8
when s_dim.channel_value = '大家养老'  then  12
when s_dim.channel_value = '太保产险大连分公司'  then  13
when s_dim.channel_value = '太保产险宁波分公司'  then  14
when s_dim.channel_value = '太保产险浙江分公司'  then  15
when s_dim.channel_value = '太保产险苏州分公司'  then  16
when s_dim.channel_value = '太保健康'  then  10
when s_dim.channel_value = '宁波普惠'  then  17
when s_dim.channel_value = '平安产险'  then  7
when s_dim.channel_value = '平安保险'  then  20
when s_dim.channel_value = '暖哇科技'  then  9
when s_dim.channel_value = '泰康养老上海分公司'  then  2
when s_dim.channel_value = '泰康养老北京分公司'  then  3
when s_dim.channel_value = '泰康养老厦门分公司'  then  6
when s_dim.channel_value = '泰康养老山东分公司'  then  5
when s_dim.channel_value = '泰康养老广东分公司'  then  1
when s_dim.channel_value = '泰康养老河南分公司'  then  4
when s_dim.channel_value = '渤海人寿'  then  11
when s_dim.channel_value = '现代财产险'  then  19
when s_dim.channel_value = '蓝云保-亚太财'  then  18  else ''  end  渠道排序

FROM
    CLAIM_DIM.DIM_INSURE_COMPANY_CHANNEL_S s_dim
LEFT JOIN (
SELECT
        insure_company_channel,
        案件状态,
        distribution +preAudit +match_sum + sign_sum + deduct  + audit_sum + review + insureCompanyReview + pending + postback + entry + quest AS total,
        distribution,
        preAudit,match_sum,sign_sum,deduct, autoAudit,audit_sum,review,insureCompanyReview,pending,postback,entry,quest

    FROM
    (
        SELECT
        insure_company_channel,
        '待处理（总）' 案件状态,
        SUM(CASE
                WHEN node = 1 THEN 1
                ELSE 0
            END) distribution,
        SUM(CASE
                WHEN node = 2 THEN 1
                ELSE 0
            END) preAudit,
        SUM(CASE
                WHEN node = 3 THEN 1
                ELSE 0
            END) match_sum,
        SUM(CASE
                WHEN node = 4 THEN 1
                ELSE 0
            END) sign_sum,
        SUM(CASE
                WHEN node = 5 THEN 1
                ELSE 0
            END) deduct,
        SUM(CASE
                WHEN node = 6 THEN 1
                ELSE 0
            END) autoAudit,
        SUM(CASE
                WHEN node = 7 THEN 1
                ELSE 0
            END) audit_sum,
        SUM(CASE
                WHEN node = 8 THEN 1
                ELSE 0
            END) review,
        SUM(CASE
                WHEN node = 9 THEN 1
                ELSE 0
            END) insureCompanyReview,
        SUM(CASE
                WHEN node = 10 THEN 1
                ELSE 0
            END) pending,
        SUM(CASE
                WHEN node = 11 THEN 1
                ELSE 0
            END) postback,
        SUM(CASE
                WHEN node = 12 THEN 1
                ELSE 0
            END) entry,
        SUM(CASE
                WHEN node = 13 THEN 1
                ELSE 0
            END) quest
    FROM
        claim_ods.claim_console_record ccr
    WHERE
        is_end = 0  and is_deleted = 'N'
    GROUP BY
        insure_company_channel
    ) c
    UNION ALL
    SELECT
        insure_company_channel,
        案件状态,
        distribution +preAudit +match_sum + sign_sum + deduct + autoAudit + audit_sum + review + insureCompanyReview + pending + postback + entry + quest AS total,
        distribution,
        preAudit,match_sum,sign_sum,deduct, autoAudit,audit_sum,review,insureCompanyReview,pending,postback,entry,quest

    FROM
    (
        SELECT
        insure_company_channel,
        CASE

            WHEN time_effect_status = 1 or time_effect_status is null THEN '待处理（总）'
            WHEN time_effect_status=2 THEN '预警'
            WHEN time_effect_status=3 THEN '超时'
            ELSE '其他'
        END 案件状态,
        SUM(CASE
                WHEN node = 1 THEN 1
                ELSE 0
            END) distribution,
        SUM(CASE
                WHEN node = 2 THEN 1
                ELSE 0
            END) preAudit,
        SUM(CASE
                WHEN node = 3 THEN 1
                ELSE 0
            END) match_sum,
        SUM(CASE
                WHEN node = 4 THEN 1
                ELSE 0
            END) sign_sum,
        SUM(CASE
                WHEN node = 5 THEN 1
                ELSE 0
            END) deduct,
        SUM(CASE
                WHEN node = 6 THEN 1
                ELSE 0
            END) autoAudit,
        SUM(CASE
                WHEN node = 7 THEN 1
                ELSE 0
            END) audit_sum,
        SUM(CASE
                WHEN node = 8 THEN 1
                ELSE 0
            END) review,
        SUM(CASE
                WHEN node = 9 THEN 1
                ELSE 0
            END) insureCompanyReview,
        SUM(CASE
                WHEN node = 10 THEN 1
                ELSE 0
            END) pending,
        SUM(CASE
                WHEN node = 11 THEN 1
                ELSE 0
            END) postback,
        SUM(CASE
                WHEN node = 12 THEN 1
                ELSE 0
            END) entry,
        SUM(CASE
                WHEN node = 13 THEN 1
                ELSE 0
            END) quest
    FROM
        claim_ods.claim_console_record ccr
    WHERE
        is_end = 0 and time_effect_status in(2,3)  and is_deleted = 'N'
    GROUP BY
        insure_company_channel,
        time_effect_status
    ) a
UNION ALL
    SELECT
        insure_company_channel,
        案件状态,
        distribution +preAudit +match_sum + sign_sum + deduct  + audit_sum + review + insureCompanyReview + pending + postback + entry + quest AS total,
        distribution,
        preAudit,match_sum,sign_sum,deduct,  autoAudit,audit_sum,review,insureCompanyReview,pending,postback,entry,quest
    FROM
    (
        SELECT
        insure_company_channel,
        '今日待处理' 案件状态,

        SUM(CASE
                WHEN node = 1 THEN 1
                ELSE 0
            END) distribution,
        SUM(CASE
                WHEN node = 2 THEN 1
                ELSE 0
            END) preAudit,
        SUM(CASE
                WHEN node = 3 THEN 1
                ELSE 0
            END) match_sum,
        SUM(CASE
                WHEN node = 4 THEN 1
                ELSE 0
            END) sign_sum,
        SUM(CASE
                WHEN node = 5 THEN 1
                ELSE 0
            END) deduct,
        SUM(CASE
                WHEN node = 6 THEN 1
                ELSE 0
            END) autoAudit,
        SUM(CASE
                WHEN node = 7 THEN 1
                ELSE 0
            END) audit_sum,
        SUM(CASE
                WHEN node = 8 THEN 1
                ELSE 0
            END) review,
        SUM(CASE
                WHEN node = 9 THEN 1
                ELSE 0
            END) insureCompanyReview,
        SUM(CASE
                WHEN node = 10 THEN 1
                ELSE 0
            END) pending,
        SUM(CASE
                WHEN node = 11 THEN 1
                ELSE 0
            END) postback,
        SUM(CASE
                WHEN node = 12 THEN 1
                ELSE 0
            END) entry,
        SUM(CASE
                WHEN node = 13 THEN 1
                ELSE 0
            END) quest
    FROM
        claim_ods.claim_console_record
    WHERE
        is_end = 0  and is_deleted = 'N'
        AND claim_dead_time <= DATE_ADD(NOW(), INTERVAL 33 HOUR) and node is not null
    GROUP BY
        insure_company_channel
    ))      b ON b.insure_company_channel = s_dim.channel_key
    AND b.案件状态 = s_dim.channel_status
"""
def truncate_table(table_name='CLAIM_DWD.dwd_claim_risk_analysis_detail'):
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
