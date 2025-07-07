import pymysql
# import sys
#
# sys.path.append(r"E:\pycharm\database")
from database import DatabaseConnection
from datetime import datetime
from datetime import datetime, timedelta


# 获取当前日期时间
current_date = datetime.now()
# 计算6个月前的日期
six_months_ago_date = current_date - timedelta(days=30)
six_months = six_months_ago_date.strftime('%Y-%m')


sql_query = f"""
  -- @description: 泰康对账明细表-去掉一拆多
  -- @param:
  -- @return:
  -- @alter:
  -- @time: 2024-06-20 15:01:06
  -- @author: 01
  -- @version: 1.0.0
INSERT  INTO `CLAIM_DWD`.`DWD_TK_BILL_DETAIL_NEW`
SELECT
    alr.insure_company_channel AS INSURE_COMPANY_CHANNEL,
    CAST(CASE
        WHEN alr.ACCEPT_STATUS = '5' OR c.clm_process_status = '11' THEN
            CASE
                WHEN c.cancle_time IS NOT NULL THEN c.cancle_time
                ELSE alr.T_UPD_TIME
            END
        ELSE pr.sucess_time
    END AS DATE ) AS comm_date,
    alr.ACCEPT_NUM AS ACCEPT_NUM,
    CASE
        WHEN alr.claim_source IN ('1', '3') THEN '线下'
        WHEN alr.claim_source IN ('2', '4') THEN '线上'
        ELSE ''
    END AS CLAIM_TYPE,
    CASE
        WHEN alr.ACCEPT_STATUS = '5' OR c.clm_process_status = '11' THEN '撤案'
        WHEN pr.back_status = '2' THEN '结案'
        ELSE ''
    END AS is_back,
    COUNT(DISTINCT inv.C_INV_NO) AS C_INV_NO,
    CASE
        WHEN GROUP_CONCAT(DISTINCT inv.C_RESPONSE_DESC) LIKE '%住院%' THEN '住院'
        ELSE '门诊'
    END AS MEDICAL_TYPE,
    CASE
        WHEN COUNT(DISTINCT inv.C_INV_NO) > 8 THEN COUNT(DISTINCT inv.C_INV_NO) - 8
        ELSE 0
    END AS CHAO8,
    REPLACE(CURDATE(), '-', '') AS REPORT_DATE,group_concat(distinct inv.C_RESPONSE_DESC)
FROM
    claim_ods.accept_list_record alr
LEFT JOIN
    claim_ods.claim c ON c.acceptance_no = alr.ACCEPT_NUM AND c.insure_company_channel IN ('TK01','TK02','TK04','TK06','TK07')
LEFT JOIN
    claim_ods.clm_app_info cai ON cai.C_CUSTOM_APP_NO = c.claim_no AND cai.C_DEL_FLAG = '0' AND cai.INSURANCE_COMPANY IN ('TK01','TK02','TK04','TK06','TK07')
LEFT JOIN
    claim_ods.claim_policy cp ON cp.policy_no = cai.C_PLY_NO
LEFT JOIN
    claim_ods.claim_policy_customer cpc ON cpc.customer_no = cp.customer_no
LEFT JOIN
    claim_ods.postback_record pr ON pr.accept_num = alr.ACCEPT_NUM AND pr.is_deleted = 'N' AND pr.receiver = 'I' AND pr.insure_company_channel IN ('TK01','TK02','TK04','TK06','TK07')
LEFT JOIN
    claim_ods.clm_visit_inv_info inv ON inv.C_CUSTOM_APP_NO = c.claim_no AND inv.C_PLY_NO = cai.C_PLY_NO AND inv.C_BILL_TYP <> '3' AND inv.C_DEL_FLAG = '0' AND inv.INSURANCE_COMPANY IN ('TK01','TK02','TK04','TK06','TK07')
WHERE
    alr.DEL_FLAG = '0' AND alr.insure_company_channel IN ('TK01','TK02','TK04','TK06','TK07')
    AND ((pr.back_status='2' AND substr(pr.back_time,1,7)>='{six_months}') OR ((alr.ACCEPT_STATUS = '5' OR c.clm_process_status = '11') AND (substr(c.cancle_time,1,7)>='{six_months}' OR substr(alr.T_UPD_TIME,1,7)>='{six_months}')))
GROUP BY
    alr.insure_company_channel,
    CASE
        WHEN alr.ACCEPT_STATUS = '5' OR c.clm_process_status = '11' THEN
            CASE
                WHEN c.cancle_time IS NOT NULL THEN c.cancle_time
                ELSE alr.T_UPD_TIME
            END
        ELSE pr.sucess_time
    END,
    alr.ACCEPT_NUM,
    CASE
        WHEN alr.claim_source IN ('1', '3') THEN '线下'
        WHEN alr.claim_source IN ('2', '4') THEN '线上'
        ELSE ''
    END,
    CASE
        WHEN alr.ACCEPT_STATUS = '5' OR c.clm_process_status = '11' THEN '撤案'
        WHEN pr.back_status = '2' THEN '结案'
        ELSE ''
    END,
    CASE
        WHEN  inv.C_RESPONSE_DESC LIKE '%住院%' THEN '住院'
        ELSE '门诊'
    END

"""


def truncate_table(table_name='CLAIM_DWD.DWD_TK_BILL_DETAIL_NEW'):
    with DatabaseConnection() as conn:
        truncate_sql = f"delete from  {table_name}  where  substr(comm_date,1,7)>='{six_months}' "
        with conn.cursor() as cursor:
            cursor.execute(truncate_sql)
            conn.commit()


update_sql = """
    UPDATE CLAIM_DWD.DWD_TK_BILL_DETAIL_NEW duc
    JOIN claim_ods.dim_insure_company_channel dim ON duc.insure_company_channel = dim.channel_key
    SET duc.insure_company_channel = dim.channel_value
    """


def update():
    """
    清空指定表的数据
    """
    with DatabaseConnection() as conn:
        with conn.cursor() as cursor:
            cursor.execute(update_sql)
            conn.commit()

def insert_data(sql_query):
    with DatabaseConnection() as conn:
        with conn.cursor() as cursor:
            cursor.execute(sql_query)
            conn.commit()


if __name__ == "__main__":
    start_time = datetime.now().strftime("%Y-%m-d %H:%M:%S")
    print("程序开始时间：", start_time)
    truncate_table()
    insert_data(sql_query)
    update()
    end_time = datetime.now().strftime("%Y-%m-d %H:%M:%S")
    print("程序结束时间：", end_time)