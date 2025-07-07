import os
import pandas as pd
from sqlalchemy import create_engine
from datetime import datetime, date, timedelta
from EmailSender_New import EmailSender_New
from dateutil.relativedelta import relativedelta

# 数据库连接信息
host = 'rr-uf636kzrob17osr7j.mysql.rds.aliyuncs.com'
port = 3306
user = 'prd_readonly'
password = '7MmY^nEJ3fQhysj=B'
db_name = 'claim_prd'

# 获取当前日期
today = date.today()
# 计算7天前的日期
formatted_date = today.strftime("%Y%m%d")

# 计算昨天的日期
yesterday = today - timedelta(days=1)

# 转义 SQL 查询中的 % 符号
sql_query_1 = f"""
  -- @description: 湖南医惠保--周度任务统计数据
  -- @param:
  -- @return:
  -- @alter:
  -- @time: 2024-11-25 15:01:06
  -- @author: 01
  -- @version: 1.0.0
   


SELECT
    '总体赔付情况'  as '概况',
    '已决案件' as '类型',
    COUNT(DISTINCT alr.ACCEPT_NUM) 赔付案件数,
    SUM(case  when JSON_EXTRACT(alr.extra, '$.ybClaimRequest.settClaSumamt')  > 0 then JSON_EXTRACT(alr.extra, '$.ybClaimRequest.settClaSumamt')
        else  cai.N_FINAL_COMPENSATE_AMT end ) 赔付金额,
    COUNT(DISTINCT alr.danger_id_no) 人数,
    SUM(case  when JSON_EXTRACT(alr.extra, '$.ybClaimRequest.settClaSumamt')  > 0 then JSON_EXTRACT(alr.extra, '$.ybClaimRequest.settClaSumamt')
        else  cai.N_FINAL_COMPENSATE_AMT end ) / COUNT(DISTINCT alr.danger_id_no) 人均金额,
    AVG(DATEDIFF(cpd.pay_time, alr.ACCEPT_DATE))/ 30  结算平均周期
FROM
    accept_list_record alr
LEFT JOIN
    claim c ON alr.ACCEPT_NUM = c.acceptance_no AND c.delete_flag = '0' AND c.insure_company_channel = 'YH01'
LEFT JOIN
    postback_record pr ON c.claim_no = pr.app_no AND pr.insure_company_channel = 'YH01' AND pr.is_deleted = 'N'    and pr.receiver='I'
LEFT JOIN
    clm_app_info cai ON cai.C_CUSTOM_APP_NO = c.claim_no AND cai.INSURANCE_COMPANY = 'YH01' AND cai.C_DEL_FLAG = '0'
LEFT JOIN
    claim_pay_detail cpd ON cpd.app_no = pr.app_no AND cpd.is_deleted = 'N' AND cpd.pay_status = '1'
WHERE
    alr.INSURE_COMPANY_CHANNEL = 'YH01'
    AND alr.DEL_FLAG = '0'
     and alr.ACCEPT_STATUS <> '5'   AND   c.clm_process_status <> '11' 
  and   ( cai.N_FINAL_COMPENSATE_AMT > 0  or JSON_EXTRACT(alr.extra, '$.ybClaimRequest.settClaSumamt')  > 0 )
    AND cpd.app_no IS NOT NULL
    and  DATE_FORMAT(FROM_UNIXTIME(JSON_EXTRACT(alr.extra, '$.ybClaimRequest.enddate') / 1000), '%%Y-%%m-%%d') <='{yesterday}'
and   substr(alr.T_CRT_TIME,1,10)  <='{yesterday}'

union all

SELECT
        '总体赔付情况'  as '概况',
    '未决案件（含一站式和未报案）' as '类型',
    COUNT(DISTINCT alr.ACCEPT_NUM) 赔付案件数,
    SUM(case  when JSON_EXTRACT(alr.extra, '$.ybClaimRequest.settClaSumamt')  > 0 then JSON_EXTRACT(alr.extra, '$.ybClaimRequest.settClaSumamt')
        else  cai.N_FINAL_COMPENSATE_AMT end ) 赔付金额,
    COUNT(DISTINCT alr.danger_id_no) 人数,
    SUM(case  when JSON_EXTRACT(alr.extra, '$.ybClaimRequest.settClaSumamt')  > 0 then JSON_EXTRACT(alr.extra, '$.ybClaimRequest.settClaSumamt')
        else  cai.N_FINAL_COMPENSATE_AMT end ) / COUNT(DISTINCT alr.danger_id_no) 人均金额,''
FROM
    accept_list_record alr
LEFT JOIN
    claim c ON alr.ACCEPT_NUM = c.acceptance_no AND c.delete_flag = '0' AND c.insure_company_channel = 'YH01'
LEFT JOIN
    postback_record pr ON c.claim_no = pr.app_no AND pr.insure_company_channel = 'YH01' AND pr.is_deleted = 'N'    and pr.receiver='I'
LEFT JOIN
    clm_app_info cai ON cai.C_CUSTOM_APP_NO = c.claim_no AND cai.INSURANCE_COMPANY = 'YH01' AND cai.C_DEL_FLAG = '0'
LEFT JOIN
    claim_pay_detail cpd ON cpd.app_no = pr.app_no AND cpd.is_deleted = 'N' AND cpd.pay_status = '1'
WHERE
    alr.INSURE_COMPANY_CHANNEL = 'YH01'
    AND alr.DEL_FLAG = '0'
     and alr.ACCEPT_STATUS <> '5'   AND   c.clm_process_status <> '11' 
  and   ( cai.N_FINAL_COMPENSATE_AMT > 0  or JSON_EXTRACT(alr.extra, '$.ybClaimRequest.settClaSumamt')  > 0 )
    AND cpd.app_no IS  NULL
        and  DATE_FORMAT(FROM_UNIXTIME(JSON_EXTRACT(alr.extra, '$.ybClaimRequest.enddate') / 1000), '%%Y-%%m-%%d') <='{yesterday}'
and   substr(alr.T_CRT_TIME,1,10)  <='{yesterday}'

union all


SELECT
    '一站式结算'  as '概况',
    '已决案件' as '类型',
    COUNT(DISTINCT alr.ACCEPT_NUM) 赔付案件数,
    SUM(JSON_EXTRACT(alr.extra, '$.ybClaimRequest.settClaSumamt')) 赔付金额,
    COUNT(DISTINCT alr.danger_id_no) 人数,
    SUM(JSON_EXTRACT(alr.extra, '$.ybClaimRequest.settClaSumamt')) / COUNT(DISTINCT alr.danger_id_no) 人均金额,
    AVG(DATEDIFF(cpd.pay_time, alr.ACCEPT_DATE))/ 30  结算平均周期
FROM
    accept_list_record alr
LEFT JOIN
    claim c ON alr.ACCEPT_NUM = c.acceptance_no AND c.delete_flag = '0' AND c.insure_company_channel = 'YH01'
LEFT JOIN
    postback_record pr ON c.claim_no = pr.app_no AND pr.insure_company_channel = 'YH01' AND pr.is_deleted = 'N'    and pr.receiver='I'
LEFT JOIN
    clm_app_info cai ON cai.C_CUSTOM_APP_NO = c.claim_no AND cai.INSURANCE_COMPANY = 'YH01' AND cai.C_DEL_FLAG = '0'
LEFT JOIN
    claim_pay_detail cpd ON cpd.app_no = pr.app_no AND cpd.is_deleted = 'N' AND cpd.pay_status = '1'
WHERE
    alr.INSURE_COMPANY_CHANNEL = 'YH01'
    AND alr.DEL_FLAG = '0'
     and alr.ACCEPT_STATUS <> '5'   AND   c.clm_process_status <> '11' 
  and  JSON_EXTRACT(alr.extra, '$.ybClaimRequest.oneStpFlag')='1'
    AND cpd.app_no IS NOT NULL
    and  JSON_EXTRACT(alr.extra, '$.ybClaimRequest.settClaSumamt')  > 0
           and  DATE_FORMAT(FROM_UNIXTIME(JSON_EXTRACT(alr.extra, '$.ybClaimRequest.enddate') / 1000), '%%Y-%%m-%%d') <='{yesterday}'
and   substr(alr.T_CRT_TIME,1,10)  <='{yesterday}'

union all

SELECT
    '一站式结算'  as '概况',
    '未决案件（含一站式和未报案）' as '类型',
    COUNT(DISTINCT alr.ACCEPT_NUM) 赔付案件数,
    SUM(JSON_EXTRACT(alr.extra, '$.ybClaimRequest.settClaSumamt')) 赔付金额,
    COUNT(DISTINCT alr.danger_id_no) 人数,
    SUM(JSON_EXTRACT(alr.extra, '$.ybClaimRequest.settClaSumamt')) / COUNT(DISTINCT alr.danger_id_no) 人均金额,''
FROM
    accept_list_record alr
LEFT JOIN
    claim c ON alr.ACCEPT_NUM = c.acceptance_no AND c.delete_flag = '0' AND c.insure_company_channel = 'YH01'
LEFT JOIN
    postback_record pr ON c.claim_no = pr.app_no AND pr.insure_company_channel = 'YH01' AND pr.is_deleted = 'N'    and pr.receiver='I'
LEFT JOIN
    clm_app_info cai ON cai.C_CUSTOM_APP_NO = c.claim_no AND cai.INSURANCE_COMPANY = 'YH01' AND cai.C_DEL_FLAG = '0'
LEFT JOIN
    claim_pay_detail cpd ON cpd.app_no = pr.app_no AND cpd.is_deleted = 'N' AND cpd.pay_status = '1'
WHERE
    alr.INSURE_COMPANY_CHANNEL = 'YH01'
    AND alr.DEL_FLAG = '0'
     and alr.ACCEPT_STATUS <> '5'   AND   c.clm_process_status <> '11' 
  and  JSON_EXTRACT(alr.extra, '$.ybClaimRequest.oneStpFlag')='1'
    AND cpd.app_no IS  NULL
     and  JSON_EXTRACT(alr.extra, '$.ybClaimRequest.settClaSumamt')  > 0
    and  DATE_FORMAT(FROM_UNIXTIME(JSON_EXTRACT(alr.extra, '$.ybClaimRequest.enddate') / 1000), '%%Y-%%m-%%d') <='{yesterday}'
and   substr(alr.T_CRT_TIME,1,10)  <='{yesterday}'
union all

SELECT
    '非一站式结算'  as '概况',
    '已决案件' as '类型',
    COUNT(DISTINCT alr.ACCEPT_NUM) 赔付案件数,
    SUM(case when cai.N_FINAL_COMPENSATE_AMT  is not null then  cai.N_FINAL_COMPENSATE_AMT  else JSON_EXTRACT(alr.extra, '$.ybClaimRequest.settClaSumamt') end  ) 赔付金额,
    COUNT(DISTINCT alr.danger_id_no) 人数,
     SUM(  cai.N_FINAL_COMPENSATE_AMT  )  / COUNT(DISTINCT alr.danger_id_no) 人均金额,
    AVG(DATEDIFF(cpd.pay_time, alr.ACCEPT_DATE))/ 30  结算平均周期
FROM
    accept_list_record alr
LEFT JOIN
    claim c ON alr.ACCEPT_NUM = c.acceptance_no AND c.delete_flag = '0' AND c.insure_company_channel = 'YH01'
LEFT JOIN
    postback_record pr ON c.claim_no = pr.app_no AND pr.insure_company_channel = 'YH01' AND pr.is_deleted = 'N'    and pr.receiver='I'
LEFT JOIN
    clm_app_info cai ON cai.C_CUSTOM_APP_NO = c.claim_no AND cai.INSURANCE_COMPANY = 'YH01' AND cai.C_DEL_FLAG = '0'
LEFT JOIN
    claim_pay_detail cpd ON cpd.app_no = pr.app_no AND cpd.is_deleted = 'N' AND cpd.pay_status = '1'
WHERE
    alr.INSURE_COMPANY_CHANNEL = 'YH01'
    AND alr.DEL_FLAG = '0'
     and alr.ACCEPT_STATUS <> '5'   AND   c.clm_process_status <> '11' 
   and   ( cai.N_FINAL_COMPENSATE_AMT > 0  or JSON_EXTRACT(alr.extra, '$.ybClaimRequest.settClaSumamt')  > 0 )
  and  JSON_EXTRACT(alr.extra, '$.ybClaimRequest.oneStpFlag')<>'1'
    AND cpd.app_no IS NOT NULL
     and  DATE_FORMAT(FROM_UNIXTIME(JSON_EXTRACT(alr.extra, '$.ybClaimRequest.enddate') / 1000), '%%Y-%%m-%%d') <='{yesterday}'
and   substr(alr.T_CRT_TIME,1,10)  <='{yesterday}'
union all

SELECT
    '非一站式结算'  as '概况',
    '未决案件（含一站式和未报案）' as '类型',
    COUNT(DISTINCT alr.ACCEPT_NUM) 赔付案件数,
    SUM(  case when cai.N_FINAL_COMPENSATE_AMT  is not null then  cai.N_FINAL_COMPENSATE_AMT  else JSON_EXTRACT(alr.extra, '$.ybClaimRequest.settClaSumamt') end ) 赔付金额,
    COUNT(DISTINCT alr.danger_id_no) 人数,
     SUM(  cai.N_FINAL_COMPENSATE_AMT  )  / COUNT(DISTINCT alr.danger_id_no) 人均金额,''
FROM
    accept_list_record alr
LEFT JOIN
    claim c ON alr.ACCEPT_NUM = c.acceptance_no AND c.delete_flag = '0' AND c.insure_company_channel = 'YH01'
LEFT JOIN
    postback_record pr ON c.claim_no = pr.app_no AND pr.insure_company_channel = 'YH01' AND pr.is_deleted = 'N'    and pr.receiver='I'
LEFT JOIN
    clm_app_info cai ON cai.C_CUSTOM_APP_NO = c.claim_no AND cai.INSURANCE_COMPANY = 'YH01' AND cai.C_DEL_FLAG = '0'
LEFT JOIN
    claim_pay_detail cpd ON cpd.app_no = pr.app_no AND cpd.is_deleted = 'N' AND cpd.pay_status = '1'
WHERE
    alr.INSURE_COMPANY_CHANNEL = 'YH01'
    AND alr.DEL_FLAG = '0'
     and alr.ACCEPT_STATUS <> '5'   AND   c.clm_process_status <> '11' 
  and   ( cai.N_FINAL_COMPENSATE_AMT > 0  or JSON_EXTRACT(alr.extra, '$.ybClaimRequest.settClaSumamt')  > 0 )
  and  JSON_EXTRACT(alr.extra, '$.ybClaimRequest.oneStpFlag')<>'1'
    AND cpd.app_no IS  NULL
     and  DATE_FORMAT(FROM_UNIXTIME(JSON_EXTRACT(alr.extra, '$.ybClaimRequest.enddate') / 1000), '%%Y-%%m-%%d') <='{yesterday}'
and   substr(alr.T_CRT_TIME,1,10)  <='{yesterday}'
union all

SELECT
    '医院结算情况'  as '概况',
    '已决案件' as '类型',
    COUNT(DISTINCT alr.ACCEPT_NUM) 赔付案件数,
    SUM(cpd.pay_amt) 赔付金额,
    COUNT(DISTINCT alr.danger_id_no) 人数,
    SUM(cpd.pay_amt) / COUNT(DISTINCT alr.danger_id_no) 人均金额,''
FROM
    accept_list_record alr
LEFT JOIN
    claim c ON alr.ACCEPT_NUM = c.acceptance_no AND c.delete_flag = '0' AND c.insure_company_channel = 'YH01'
LEFT JOIN
    postback_record pr ON c.claim_no = pr.app_no AND pr.insure_company_channel = 'YH01' AND pr.is_deleted = 'N'    and pr.receiver='I'
LEFT JOIN
    clm_app_info cai ON cai.C_CUSTOM_APP_NO = c.claim_no AND cai.INSURANCE_COMPANY = 'YH01' AND cai.C_DEL_FLAG = '0'
LEFT JOIN
    claim_pay_detail cpd ON cpd.app_no = pr.app_no AND cpd.is_deleted = 'N' AND cpd.pay_status = '1'
WHERE
    alr.INSURE_COMPANY_CHANNEL = 'YH01'
    AND alr.DEL_FLAG = '0'
     and alr.ACCEPT_STATUS <> '5'   AND   c.clm_process_status <> '11' 
  and  JSON_EXTRACT(alr.extra, '$.ybClaimRequest.settClaSumamt')  > 0
    AND cpd.app_no IS not  NULL
      and  DATE_FORMAT(FROM_UNIXTIME(JSON_EXTRACT(alr.extra, '$.ybClaimRequest.enddate') / 1000), '%%Y-%%m-%%d') <='{yesterday}'
and   substr(alr.T_CRT_TIME,1,10)  <='{yesterday}'
union all

SELECT
    '医院结算情况'  as '概况',
    '未决案件（含一站式和未报案）' as '类型',
    COUNT(DISTINCT alr.ACCEPT_NUM) 赔付案件数,
    SUM(cpd.pay_amt) 赔付金额,
    COUNT(DISTINCT alr.danger_id_no) 人数,
    SUM(cpd.pay_amt) / COUNT(DISTINCT alr.danger_id_no) 人均金额,''
FROM
    accept_list_record alr
LEFT JOIN
    claim c ON alr.ACCEPT_NUM = c.acceptance_no AND c.delete_flag = '0' AND c.insure_company_channel = 'YH01'
LEFT JOIN
    postback_record pr ON c.claim_no = pr.app_no AND pr.insure_company_channel = 'YH01' AND pr.is_deleted = 'N'    and pr.receiver='I'
LEFT JOIN
    clm_app_info cai ON cai.C_CUSTOM_APP_NO = c.claim_no AND cai.INSURANCE_COMPANY = 'YH01' AND cai.C_DEL_FLAG = '0'
LEFT JOIN
    claim_pay_detail cpd ON cpd.app_no = pr.app_no AND cpd.is_deleted = 'N' AND cpd.pay_status = '1'
WHERE
    alr.INSURE_COMPANY_CHANNEL = 'YH01'
    AND alr.DEL_FLAG = '0'
     and alr.ACCEPT_STATUS <> '5'   AND   c.clm_process_status <> '11' 
  and  JSON_EXTRACT(alr.extra, '$.ybClaimRequest.settClaSumamt')  > 0
    AND cpd.app_no IS   NULL
      and  DATE_FORMAT(FROM_UNIXTIME(JSON_EXTRACT(alr.extra, '$.ybClaimRequest.enddate') / 1000), '%%Y-%%m-%%d') <='{yesterday}'
and   substr(alr.T_CRT_TIME,1,10)  <='{yesterday}'
union all

SELECT
    '',
    case when JSON_EXTRACT(alr.extra, '$.ybClaimRequest.mdtrtInfo.insutype')='390' then '居民'
        when JSON_EXTRACT(alr.extra, '$.ybClaimRequest.mdtrtInfo.insutype')='310' then '城镇' else '' end  '类型',
    COUNT(DISTINCT alr.ACCEPT_NUM) 赔付案件数,
    SUM(case  when JSON_EXTRACT(alr.extra, '$.ybClaimRequest.settClaSumamt')  > 0 then JSON_EXTRACT(alr.extra, '$.ybClaimRequest.settClaSumamt')
        else  cai.N_FINAL_COMPENSATE_AMT end ) 赔付金额,
    COUNT(DISTINCT alr.danger_id_no) 人数,
    SUM(case  when JSON_EXTRACT(alr.extra, '$.ybClaimRequest.settClaSumamt')  > 0 then JSON_EXTRACT(alr.extra, '$.ybClaimRequest.settClaSumamt')
        else  cai.N_FINAL_COMPENSATE_AMT end ) / COUNT(DISTINCT alr.danger_id_no) 人均金额,''
FROM
    accept_list_record alr
LEFT JOIN
    claim c ON alr.ACCEPT_NUM = c.acceptance_no AND c.delete_flag = '0' AND c.insure_company_channel = 'YH01'
LEFT JOIN
    postback_record pr ON c.claim_no = pr.app_no AND pr.insure_company_channel = 'YH01' AND pr.is_deleted = 'N'    and pr.receiver='I'
LEFT JOIN
    clm_app_info cai ON cai.C_CUSTOM_APP_NO = c.claim_no AND cai.INSURANCE_COMPANY = 'YH01' AND cai.C_DEL_FLAG = '0'
WHERE
    alr.INSURE_COMPANY_CHANNEL = 'YH01'
    AND alr.DEL_FLAG = '0'
     and alr.ACCEPT_STATUS <> '5'   AND   c.clm_process_status <> '11' 
  and   ( cai.N_FINAL_COMPENSATE_AMT > 0  or JSON_EXTRACT(alr.extra, '$.ybClaimRequest.settClaSumamt')  > 0 )
    and  DATE_FORMAT(FROM_UNIXTIME(JSON_EXTRACT(alr.extra, '$.ybClaimRequest.enddate') / 1000), '%%Y-%%m-%%d') <='{yesterday}'
    and   substr(alr.T_CRT_TIME,1,10)  <='{yesterday}'
group by  JSON_EXTRACT(alr.extra, '$.ybClaimRequest.mdtrtInfo.insutype')

union all

SELECT
        '',
    CASE
        WHEN CAST(SUBSTRING(alr.danger_id_no, 17, 1) AS UNSIGNED) %% 2 = 1 THEN '男'
        ELSE '女'
    END AS  '类型',
    COUNT(DISTINCT alr.ACCEPT_NUM) 赔付案件数,
    SUM(case  when JSON_EXTRACT(alr.extra, '$.ybClaimRequest.settClaSumamt')  > 0 then JSON_EXTRACT(alr.extra, '$.ybClaimRequest.settClaSumamt')
        else  cai.N_FINAL_COMPENSATE_AMT end ) 赔付金额,
    COUNT(DISTINCT alr.danger_id_no) 人数,
    SUM(case  when JSON_EXTRACT(alr.extra, '$.ybClaimRequest.settClaSumamt')  > 0 then JSON_EXTRACT(alr.extra, '$.ybClaimRequest.settClaSumamt')
        else  cai.N_FINAL_COMPENSATE_AMT end ) / COUNT(DISTINCT alr.danger_id_no) 人均金额,''
FROM
    accept_list_record alr
LEFT JOIN
    claim c ON alr.ACCEPT_NUM = c.acceptance_no AND c.delete_flag = '0' AND c.insure_company_channel = 'YH01'
LEFT JOIN
    postback_record pr ON c.claim_no = pr.app_no AND pr.insure_company_channel = 'YH01' AND pr.is_deleted = 'N'    and pr.receiver='I'
LEFT JOIN
    clm_app_info cai ON cai.C_CUSTOM_APP_NO = c.claim_no AND cai.INSURANCE_COMPANY = 'YH01' AND cai.C_DEL_FLAG = '0'
LEFT JOIN claim_policy cp on cp.policy_no = cai.C_PLY_NO
LEFT JOIN claim_policy_customer cpc on cpc.customer_no = cp.customer_no
WHERE
    alr.INSURE_COMPANY_CHANNEL = 'YH01'
    AND alr.DEL_FLAG = '0'
     and alr.ACCEPT_STATUS <> '5'   AND   c.clm_process_status <> '11' 
  and   ( cai.N_FINAL_COMPENSATE_AMT > 0  or JSON_EXTRACT(alr.extra, '$.ybClaimRequest.settClaSumamt')  > 0 )
    and  DATE_FORMAT(FROM_UNIXTIME(JSON_EXTRACT(alr.extra, '$.ybClaimRequest.enddate') / 1000), '%%Y-%%m-%%d') <='{yesterday}'
    and   substr(alr.T_CRT_TIME,1,10)  <='{yesterday}'
group by       CASE
        WHEN CAST(SUBSTRING(alr.danger_id_no, 17, 1) AS UNSIGNED) %% 2 = 1 THEN '男'
        ELSE '女'
    END

union all


SELECT
    '',
    CASE
        WHEN age < 18 THEN '18岁以下'
        WHEN age BETWEEN 18 AND 59 THEN '18 - 59岁'
        WHEN age BETWEEN 60 AND 79 THEN '60 - 79岁'
        ELSE '80岁以上'
    END AS `类型`,
    COUNT(DISTINCT ACCEPT_NUM) 赔付案件数,
    SUM(CASE
        WHEN JSON_EXTRACT(extra, '$.ybClaimRequest.settClaSumamt') > 0 THEN JSON_EXTRACT(extra, '$.ybClaimRequest.settClaSumamt')
        ELSE N_FINAL_COMPENSATE_AMT
    END) 赔付金额,
    COUNT(DISTINCT CARD_NUM) 人数,
    SUM(CASE
        WHEN JSON_EXTRACT(extra, '$.ybClaimRequest.settClaSumamt') > 0 THEN JSON_EXTRACT(extra, '$.ybClaimRequest.settClaSumamt')
        ELSE N_FINAL_COMPENSATE_AMT
    END) / COUNT(DISTINCT CARD_NUM) 人均金额,
    ''
FROM (
    SELECT
        alr.*,
        cai.N_FINAL_COMPENSATE_AMT,
        CASE
            WHEN alr.danger_id_type = '1' AND LENGTH(alr.danger_id_no) = 18 THEN
                TIMESTAMPDIFF(YEAR,
                    STR_TO_DATE(SUBSTR(alr.danger_id_no, 7, 8), '%%Y%%m%%d'),
                    CURDATE()) + 1
                -
                CASE
                    WHEN DATE_FORMAT(CURDATE(), '%%m%%d') < DATE_FORMAT(STR_TO_DATE(SUBSTR(alr.danger_id_no, 7, 8), '%%Y%%m%%d'), '%%m%%d')
                    THEN 1
                    ELSE 0
                END
            ELSE NULL
        END AS age
    FROM
        accept_list_record alr
    LEFT JOIN
        claim c ON alr.ACCEPT_NUM = c.acceptance_no AND c.delete_flag = '0' AND c.insure_company_channel = 'YH01'
    LEFT JOIN
        postback_record pr ON c.claim_no = pr.app_no AND pr.insure_company_channel = 'YH01' AND pr.is_deleted = 'N' AND pr.receiver = 'I'
    LEFT JOIN
        clm_app_info cai ON cai.C_CUSTOM_APP_NO = c.claim_no AND cai.INSURANCE_COMPANY = 'YH01' AND cai.C_DEL_FLAG = '0'
    LEFT JOIN claim_policy cp ON cp.policy_no = cai.C_PLY_NO
    LEFT JOIN claim_policy_customer cpc ON cpc.customer_no = cp.customer_no
    WHERE
        alr.INSURE_COMPANY_CHANNEL = 'YH01'
        AND alr.DEL_FLAG = '0'
         and alr.ACCEPT_STATUS <> '5'   AND   c.clm_process_status <> '11' 
        AND (cai.N_FINAL_COMPENSATE_AMT > 0 OR JSON_EXTRACT(alr.extra, '$.ybClaimRequest.settClaSumamt') > 0)
        AND  DATE_FORMAT(FROM_UNIXTIME(JSON_EXTRACT(alr.extra, '$.ybClaimRequest.enddate') / 1000), '%%Y-%%m-%%d') <= '{yesterday}'
        and   substr(alr.T_CRT_TIME,1,10)  <='{yesterday}'
) subquery
GROUP BY
    CASE
        WHEN age < 18 THEN '18岁以下'
        WHEN age BETWEEN 18 AND 59 THEN '18 - 59岁'
        WHEN age BETWEEN 60 AND 79 THEN '60 - 79岁'
        ELSE '80岁以上'
    END

union all

SELECT
    '',
            CASE
        WHEN substr(replace(JSON_EXTRACT(alr.extra, '$.ybClaimRequest.insuOptins'),'"',''),1,4) = '4301' THEN '长沙市'
        WHEN substr(replace(JSON_EXTRACT(alr.extra, '$.ybClaimRequest.insuOptins'),'"',''),1,4) = '4302' THEN '株洲市'
        WHEN substr(replace(JSON_EXTRACT(alr.extra, '$.ybClaimRequest.insuOptins'),'"',''),1,4) = '4303' THEN '湘潭市'
        WHEN substr(replace(JSON_EXTRACT(alr.extra, '$.ybClaimRequest.insuOptins'),'"',''),1,4) = '4304' THEN '衡阳市'
        WHEN substr(replace(JSON_EXTRACT(alr.extra, '$.ybClaimRequest.insuOptins'),'"',''),1,4) = '4305' THEN '邵阳市'
        WHEN substr(replace(JSON_EXTRACT(alr.extra, '$.ybClaimRequest.insuOptins'),'"',''),1,4) = '4306' THEN '岳阳市'
        WHEN substr(replace(JSON_EXTRACT(alr.extra, '$.ybClaimRequest.insuOptins'),'"',''),1,4) = '4307' THEN '常德市'
        WHEN substr(replace(JSON_EXTRACT(alr.extra, '$.ybClaimRequest.insuOptins'),'"',''),1,4) = '4308' THEN '张家界'
        WHEN substr(replace(JSON_EXTRACT(alr.extra, '$.ybClaimRequest.insuOptins'),'"',''),1,4) = '4309' THEN '益阳市'
        WHEN substr(replace(JSON_EXTRACT(alr.extra, '$.ybClaimRequest.insuOptins'),'"',''),1,4)  = '4310' THEN '郴州市'
        WHEN substr(replace(JSON_EXTRACT(alr.extra, '$.ybClaimRequest.insuOptins'),'"',''),1,4) = '4311' THEN '永州市'
        WHEN substr(replace(JSON_EXTRACT(alr.extra, '$.ybClaimRequest.insuOptins'),'"',''),1,4) = '4312' THEN '怀化市'
        WHEN substr(replace(JSON_EXTRACT(alr.extra, '$.ybClaimRequest.insuOptins'),'"',''),1,4) = '4313' THEN '娄底市'
        WHEN substr(replace(JSON_EXTRACT(alr.extra, '$.ybClaimRequest.insuOptins'),'"',''),1,4) = '4331' THEN '湘西州'
        WHEN substr(replace(JSON_EXTRACT(alr.extra, '$.ybClaimRequest.insuOptins'),'"',''),1,4) = '4399' THEN '省本级'
        ELSE substr(replace(JSON_EXTRACT(alr.extra, '$.ybClaimRequest.insuOptins'),'"',''),1,4)
    END AS '类型' ,
    COUNT(DISTINCT alr.ACCEPT_NUM) 赔付案件数,
    SUM(case  when JSON_EXTRACT(alr.extra, '$.ybClaimRequest.settClaSumamt')  > 0 then JSON_EXTRACT(alr.extra, '$.ybClaimRequest.settClaSumamt')
        else  cai.N_FINAL_COMPENSATE_AMT end ) 赔付金额,
    COUNT(DISTINCT alr.danger_id_no) 人数,
    SUM(case  when JSON_EXTRACT(alr.extra, '$.ybClaimRequest.settClaSumamt')  > 0 then JSON_EXTRACT(alr.extra, '$.ybClaimRequest.settClaSumamt')
        else  cai.N_FINAL_COMPENSATE_AMT end ) / COUNT(DISTINCT alr.danger_id_no) 人均金额,''
FROM
    accept_list_record alr
LEFT JOIN
    claim c ON alr.ACCEPT_NUM = c.acceptance_no AND c.delete_flag = '0' AND c.insure_company_channel = 'YH01'
LEFT JOIN
    postback_record pr ON c.claim_no = pr.app_no AND pr.insure_company_channel = 'YH01' AND pr.is_deleted = 'N'    and pr.receiver='I'
LEFT JOIN
    clm_app_info cai ON cai.C_CUSTOM_APP_NO = c.claim_no AND cai.INSURANCE_COMPANY = 'YH01' AND cai.C_DEL_FLAG = '0'
LEFT JOIN claim_policy cp on cp.policy_no = cai.C_PLY_NO
LEFT JOIN claim_policy_customer cpc on cpc.customer_no = cp.customer_no
WHERE
    alr.INSURE_COMPANY_CHANNEL = 'YH01'
    AND alr.DEL_FLAG = '0'
     and alr.ACCEPT_STATUS <> '5'   AND   c.clm_process_status <> '11' 
  and   ( cai.N_FINAL_COMPENSATE_AMT > 0  or JSON_EXTRACT(alr.extra, '$.ybClaimRequest.settClaSumamt')  > 0 )
   and  DATE_FORMAT(FROM_UNIXTIME(JSON_EXTRACT(alr.extra, '$.ybClaimRequest.enddate') / 1000), '%%Y-%%m-%%d') <='{yesterday}'
   and   substr(alr.T_CRT_TIME,1,10)  <='{yesterday}'
group by           CASE
        WHEN substr(replace(JSON_EXTRACT(alr.extra, '$.ybClaimRequest.insuOptins'),'"',''),1,4) = '4301' THEN '长沙市'
        WHEN substr(replace(JSON_EXTRACT(alr.extra, '$.ybClaimRequest.insuOptins'),'"',''),1,4) = '4302' THEN '株洲市'
        WHEN substr(replace(JSON_EXTRACT(alr.extra, '$.ybClaimRequest.insuOptins'),'"',''),1,4) = '4303' THEN '湘潭市'
        WHEN substr(replace(JSON_EXTRACT(alr.extra, '$.ybClaimRequest.insuOptins'),'"',''),1,4) = '4304' THEN '衡阳市'
        WHEN substr(replace(JSON_EXTRACT(alr.extra, '$.ybClaimRequest.insuOptins'),'"',''),1,4) = '4305' THEN '邵阳市'
        WHEN substr(replace(JSON_EXTRACT(alr.extra, '$.ybClaimRequest.insuOptins'),'"',''),1,4) = '4306' THEN '岳阳市'
        WHEN substr(replace(JSON_EXTRACT(alr.extra, '$.ybClaimRequest.insuOptins'),'"',''),1,4) = '4307' THEN '常德市'
        WHEN substr(replace(JSON_EXTRACT(alr.extra, '$.ybClaimRequest.insuOptins'),'"',''),1,4) = '4308' THEN '张家界'
        WHEN substr(replace(JSON_EXTRACT(alr.extra, '$.ybClaimRequest.insuOptins'),'"',''),1,4) = '4309' THEN '益阳市'
        WHEN substr(replace(JSON_EXTRACT(alr.extra, '$.ybClaimRequest.insuOptins'),'"',''),1,4)  = '4310' THEN '郴州市'
        WHEN substr(replace(JSON_EXTRACT(alr.extra, '$.ybClaimRequest.insuOptins'),'"',''),1,4) = '4311' THEN '永州市'
        WHEN substr(replace(JSON_EXTRACT(alr.extra, '$.ybClaimRequest.insuOptins'),'"',''),1,4) = '4312' THEN '怀化市'
        WHEN substr(replace(JSON_EXTRACT(alr.extra, '$.ybClaimRequest.insuOptins'),'"',''),1,4) = '4313' THEN '娄底市'
        WHEN substr(replace(JSON_EXTRACT(alr.extra, '$.ybClaimRequest.insuOptins'),'"',''),1,4) = '4331' THEN '湘西州'
        WHEN substr(replace(JSON_EXTRACT(alr.extra, '$.ybClaimRequest.insuOptins'),'"',''),1,4) = '4399' THEN '省本级'
        ELSE substr(replace(JSON_EXTRACT(alr.extra, '$.ybClaimRequest.insuOptins'),'"',''),1,4)
    END
union all
   SELECT
    '',
    赔付金额区间 `类型`,
    COUNT(DISTINCT ACCEPT_NUM) 赔付案件数,
    SUM(赔付金额) 赔付金额,
    '','',
    ''
FROM (SELECT alr.*,
             CASE
                 WHEN JSON_EXTRACT(alr.extra, '$.ybClaimRequest.settClaSumamt') > 0
                     THEN JSON_EXTRACT(alr.extra, '$.ybClaimRequest.settClaSumamt')
                 ELSE N_FINAL_COMPENSATE_AMT
                 END AS 赔付金额,
             CASE
                 WHEN (CASE
                           WHEN JSON_EXTRACT(alr.extra, '$.ybClaimRequest.settClaSumamt') > 0
                               THEN JSON_EXTRACT(alr.extra, '$.ybClaimRequest.settClaSumamt')
                           ELSE N_FINAL_COMPENSATE_AMT
                     END) < 3000 THEN '3000以下'
                 WHEN (CASE
                           WHEN JSON_EXTRACT(alr.extra, '$.ybClaimRequest.settClaSumamt') > 0
                               THEN JSON_EXTRACT(alr.extra, '$.ybClaimRequest.settClaSumamt')
                           ELSE N_FINAL_COMPENSATE_AMT
                     END) BETWEEN 3000 AND 10000 THEN '3000-10000'
                 WHEN (CASE
                           WHEN JSON_EXTRACT(alr.extra, '$.ybClaimRequest.settClaSumamt') > 0
                               THEN JSON_EXTRACT(alr.extra, '$.ybClaimRequest.settClaSumamt')
                           ELSE N_FINAL_COMPENSATE_AMT
                     END) BETWEEN 10000 AND 30000 THEN '10000-30000'
                 WHEN (CASE
                           WHEN JSON_EXTRACT(alr.extra, '$.ybClaimRequest.settClaSumamt') > 0
                               THEN JSON_EXTRACT(alr.extra, '$.ybClaimRequest.settClaSumamt')
                           ELSE N_FINAL_COMPENSATE_AMT
                     END) BETWEEN 30000 AND 50000 THEN '30000-50000'
                 WHEN (CASE
                           WHEN JSON_EXTRACT(alr.extra, '$.ybClaimRequest.settClaSumamt') > 0
                               THEN JSON_EXTRACT(alr.extra, '$.ybClaimRequest.settClaSumamt')
                           ELSE N_FINAL_COMPENSATE_AMT
                     END) BETWEEN 50000 AND 100000 THEN '50000-100000'
                 ELSE '100000以上'
                 END AS 赔付金额区间
      FROM accept_list_record alr
               LEFT JOIN
           claim c ON alr.ACCEPT_NUM = c.acceptance_no AND c.delete_flag = '0' AND c.insure_company_channel = 'YH01'
               LEFT JOIN
           postback_record pr
           ON c.claim_no = pr.app_no AND pr.insure_company_channel = 'YH01' AND pr.is_deleted = 'N' AND
              pr.receiver = 'I'
               LEFT JOIN
           clm_app_info cai
           ON cai.C_CUSTOM_APP_NO = c.claim_no AND cai.INSURANCE_COMPANY = 'YH01' AND cai.C_DEL_FLAG = '0'
               LEFT JOIN claim_policy cp ON cp.policy_no = cai.C_PLY_NO
               LEFT JOIN claim_policy_customer cpc ON cpc.customer_no = cp.customer_no
      WHERE alr.INSURE_COMPANY_CHANNEL = 'YH01'
        AND alr.DEL_FLAG = '0'
         and alr.ACCEPT_STATUS <> '5'   AND   c.clm_process_status <> '11' 
        AND (cai.N_FINAL_COMPENSATE_AMT > 0 OR JSON_EXTRACT(alr.extra, '$.ybClaimRequest.settClaSumamt') > 0)
        AND DATE_FORMAT(FROM_UNIXTIME(JSON_EXTRACT(alr.extra, '$.ybClaimRequest.enddate') / 1000), '%%Y-%%m-%%d') <='{yesterday}'
        AND substr(alr.ACCEPT_DATE, 1, 10) <= '{yesterday}') subquery
GROUP BY  赔付金额区间
"""


def send_weekly_report_email(formatted_date, excel_filename1, excel_filename2=None):
    # 配置邮件信息
    email_config = {
        'sender': 'messages-noreply@insuresmart.com.cn',
        'password': 'M&H@&2z9E65q',  # SMTP授权码
        # 'recipients': ['cuisl@insuresmart.com.cn'],
          'recipients': ['cuisl@insuresmart.com.cn', 'xuzy@insuresmart.com.cn','lixiaodan18@hun.picc.com.cn','chengong06@hun.picc.com.cn'],
        'smtp_server': 'smtp.exmail.qq.com',
        'smtp_port': 465
    }

    sender = EmailSender_New(
        email_config['sender'],
        email_config['password'],
        email_config['recipients'],
        email_config['smtp_server'],
        email_config['smtp_port']
    )

    # 构造邮件内容
    subject = '医惠保案件周报数据'
    message = f"各位老师好！\n\n 医惠保案件 {formatted_date}数据统计！\n\n"
    message += "具体信息见附件：\n"

    # 获取文件名部分
    if excel_filename2 is None:
        sender.send_email_with_attachment(subject, message, excel_filename1)
    else:
        sender.send_email_with_attachments(subject, message, [excel_filename1, excel_filename2])


def execute_data(sql_query, excel_filename):
    try:
        # 创建数据库引擎
        engine = create_engine(f'mysql+pymysql://{user}:{password}@{host}:{port}/{db_name}')
        df = pd.read_sql(sql_query, engine)
        df.to_excel(excel_filename, index=False)
    except Exception as err:
        print(f"数据库连接或查询出错: {err}")
    finally:
        if 'engine' in locals():
            engine.dispose()


if __name__ == "__main__":
    excel_filename1 = "/opt/adb_report/report_claim/output/" + f"医惠保周报{formatted_date}.xlsx"
    start_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    print("程序开始时间：", start_time)
    execute_data(sql_query_1, excel_filename1)
    if os.path.exists(excel_filename1):
        send_weekly_report_email(formatted_date, excel_filename1)
    else:
        print("Excel 文件未生成，无法发送邮件。")
    end_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    print("程序结束时间：", end_time)