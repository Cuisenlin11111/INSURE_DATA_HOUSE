# import sys
# sys.path.append(r"E:\pycharm\database")
from database import DatabaseConnection
from datetime import datetime



sql_query = f"""
  -- @description: 农商行退票清单
  -- @param:
  -- @return:
  -- @alter:
  -- @time: 2024-08-29 15:01:06
  -- @author: 01
  -- @version: 1.0.0
INSERT INTO CLAIM_DWD.DWD_TK04_BANK_RETURN_INFO
SELECT DISTINCT
    alr.accept_batch_no AS `受理批次号`,
    c.acceptance_no AS `受理编号`,
    CASE c.clm_process_status
        WHEN 0 THEN '预审'
        WHEN 1 THEN '标准化'
        WHEN 2 THEN '扣费'
        WHEN 3 THEN '定责'
        WHEN 4 THEN '理算'
        WHEN 5 THEN '风控'
        WHEN 6 THEN '审核'
        WHEN 7 THEN '复核'
        WHEN 8 THEN CASE WHEN alr.ACCEPT_SUB_STATUS = '91' THEN '保险公司待复核'
                       WHEN alr.ACCEPT_SUB_STATUS = '92' THEN '保险公司暂存'
                       WHEN alr.ACCEPT_SUB_STATUS = '93' THEN '保险公司复核通过'
                       ELSE '内部结案' END
        WHEN 9 THEN CASE WHEN alr.ACCEPT_SUB_STATUS = '91' THEN '保险公司待复核'
                       WHEN alr.ACCEPT_SUB_STATUS = '92' THEN '保险公司暂存'
                       WHEN alr.ACCEPT_SUB_STATUS = '93' THEN '保险公司复核通过'
                       ELSE '回传' END
        WHEN 10 THEN '已回传保司中介'
        WHEN 11 THEN '撤案' END AS `案件状态`,
    iat.enterprise AS `登记部门`,
    CASE WHEN c.claim_source IN ('1', '3') THEN '线下'
         WHEN c.claim_source IN ('2', '4') THEN '线上' END AS `来源`,
    cai.C_APP_NME AS `姓名`,
    cai.C_APP_CERT_CDE AS `证件号码`,
    inv.C_INV_NO AS `发票号`,
    CAST(inv.N_SUM_AMT AS DECIMAL(10, 2)) AS `发票金额`,
    CAST(inv.N_SUM_AMT AS DECIMAL(10, 2)) AS `退票金额`,
    CASE WHEN inv.C_COMPENSATE_RESULT = '4' THEN REPLACE(REPLACE(inv.C_INTERNAL_CONCLUSION, CHAR(10), ''), CHAR(13), '')
         WHEN inv.C_COMPENSATE_RESULT = '1' THEN '' END AS `拒赔原因`,
    '1' AS `发票张数`,
    '' AS `备注`
FROM claim_ods.claim c
         LEFT JOIN claim_ods.claim c1 ON c.claim_no = c1.source_claim_no
         INNER JOIN claim_ods.clm_app_info cai ON cai.C_CUSTOM_APP_NO = c.claim_no AND cai.C_DEL_FLAG = '0'
         LEFT JOIN claim_ods.clm_visit_inv_info inv ON inv.C_CUSTOM_APP_NO = c.claim_no AND inv.C_PLY_NO = cai.C_PLY_NO AND inv.c_del_flag = '0' AND inv.C_BILL_TYP <> '3' AND inv.C_IS_NEED_SHOW = '0'
         INNER JOIN claim_ods.accept_list_record alr ON alr.ACCEPT_NUM = cai.ACCEPTANCE_NO
         LEFT JOIN claim_ods.claim_policy cp ON cai.C_PLY_NO = cp.policy_no
         LEFT JOIN claim_ods.`image_assign_task` iat ON alr.accept_batch_no = iat.accept_batch_no AND iat.is_deleted = 'N'
         LEFT JOIN claim_ods.postback_record pr ON c.claim_no = pr.app_no AND pr.is_deleted = 'N'
WHERE c.INSURE_COMPANY_CHANNEL = 'TK04'
  AND c.delete_flag = '0'
  AND c.claim_no NOT LIKE '%BL%'
  AND c1.id IS NULL
  AND inv.C_COMPENSATE_RESULT = '4';
"""
def truncate_table(table_name='CLAIM_DWD.DWD_TK04_BANK_RETURN_INFO'):
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
