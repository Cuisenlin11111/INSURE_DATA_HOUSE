# import sys
# sys.path.append(r"E:\pycharm\database")
from database import DatabaseConnection
from datetime import datetime



sql_query = f"""
  -- @description: 总发票数
  -- @param:
  -- @return:
  -- @alter:
  -- @time: 2024-08-29 15:01:06
  -- @author: 01
  -- @version: 1.0.0
INSERT INTO CLAIM_DWD.DWD_TK07_BATCH_INFO
SELECT
    alr.accept_batch_no AS `收单批次号`,
    alr.ACCEPT_NUM AS `报案号`,
    pr.insurance_batch_no AS `保司批次号`,
    CASE WHEN cp.channel_group_policy_no IS NULL THEN alr.policy_no ELSE cp.channel_group_policy_no END AS `保单号`,
    pr.insurance_claim_no AS `保司赔案号`,
    cp.channel_customer_no AS `客户号`,
    c.customer_name AS `出险人姓名`,
    c.id_no AS `证件号码`,
    CASE WHEN inv.C_DOC_TYP = '1' THEN '门诊'
         WHEN inv.C_DOC_TYP = '2' THEN '住院' END AS `就诊类型`,
    inv.C_RESPONSE_DESC AS `责任`,
    inv.C_INV_NO AS `发票号码`,
    inv.T_VISIT_BGN_TM AS `就诊起始日`,
    inv.T_VISIT_END_TM AS `就诊终止日`,
    CAST(inv.N_SUM_AMT AS DECIMAL(10, 2)) AS `发票总金额`,
    CAST(inv.N_REASONABLE_AMT AS DECIMAL(10, 2)) AS `社保范围内`,
    CAST(inv.N_CATEG_SELFPAY AS DECIMAL(10, 2)) AS `部分项目自付`,
    CAST(inv.N_SELF_EXPENSE AS DECIMAL(10, 2)) AS `自费`,
    CAST(inv.N_PERSONAL_PAY_AMOUNT AS DECIMAL(10, 2)) AS `个人自付总额`,
    CAST(inv.N_SOCIAL_GIVE_AMOUNT AS DECIMAL(10, 2)) AS `社保统筹金额`,
    '' AS `个人现金支付`,
    CAST(inv.N_DEDUCTLE_AMT AS DECIMAL(10, 2)) AS `发票使用免赔额`,
    CAST(inv.N_FINAL_PAY AS DECIMAL(10, 2)) AS `发票赔付金额`,
    CASE WHEN inv.C_COMPENSATE_RESULT = '4' THEN '拒赔'
         WHEN inv.C_COMPENSATE_RESULT = '1' THEN '正常赔付' END AS `发票层赔付结论`,
    REPLACE(REPLACE(REPLACE(inv.C_INTERNAL_CONCLUSION, CHAR(10), ''), CHAR(13), ''), CHAR(9), '') AS `内部结论`,
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
         cat.T_UPD_TM 结案时间,
    REPLACE(REPLACE(alr.WITHDRAWAL_REASON, CHAR(10), ''), CHAR(13), '') AS `撤案原因`,
    REPLACE(REPLACE(qt.generation_reason, CHAR(10), ''), CHAR(13), '') AS `问题件生成原因`,
    iat.insure_accept_batch_no AS `保司收单批次号`,
    IF((IF(cpc.phone IS NULL OR cpc.phone = '', c.phone, cpc.phone) IS NULL OR IF(cpc.phone IS NULL OR cpc.phone = '', c.phone, cpc.phone) = ''), pf.C_PHONE, IF(cpc.phone IS NULL OR cpc.phone = '', c.phone, cpc.phone)) AS `电话号码`,
    c.phone AS `录入商采集电话号码`,
    cpc.phone AS `保全电话号码`,
    pf.C_PHONE AS `领款人手机号码`
FROM claim_ods.claim c
         LEFT JOIN claim_ods.clm_app_info cai ON cai.C_CUSTOM_APP_NO = c.claim_no AND cai.C_DEL_FLAG = '0'
         LEFT JOIN claim_ods.clm_visit_inv_info inv ON inv.C_CUSTOM_APP_NO = c.claim_no AND inv.C_PLY_NO = cai.C_PLY_NO AND inv.C_DEL_FLAG = '0'
         LEFT JOIN claim_ods.claim_policy cp ON cp.policy_no = cai.C_PLY_NO
         LEFT JOIN claim_ods.claim_policy_customer cpc ON cp.customer_no = cpc.customer_no
         LEFT JOIN claim_ods.accept_list_record alr ON alr.ACCEPT_NUM = c.acceptance_no
         LEFT JOIN claim_ods.postback_record pr ON pr.app_no = c.claim_no AND pr.is_deleted = 'N'
         LEFT JOIN claim_ods.question_claim qc ON c.claim_no = qc.claim_no AND qc.is_deleted = 'N'
         LEFT JOIN claim_ods.quest_subtype qt ON qc.id = qt.quest_id AND qt.is_deleted = 'N'
         LEFT JOIN claim_ods.`image_assign_task` iat ON alr.accept_batch_no = iat.accept_batch_no
         LEFT JOIN claim_ods.`ply_favoree` pf ON c.claim_no = pf.C_BATCH_NO AND pf.C_DEL_FLAG = '0'
         left join claim_ods.`case_audit_task` cat on cai.C_CUSTOM_APP_NO = cat.C_CLAIM_CASE_NO and cat.C_DEL_FLAG = '0'
WHERE c.INSURE_COMPANY_CHANNEL = 'TK07'  and substr(c.create_time,1,10) >=(CURRENT_DATE - INTERVAL '100' DAY)  and c.delete_flag='0'
ORDER BY c.ACCEPTANCE_NO;
"""
def truncate_table(table_name='CLAIM_DWD.DWD_TK07_BATCH_INFO'):
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
