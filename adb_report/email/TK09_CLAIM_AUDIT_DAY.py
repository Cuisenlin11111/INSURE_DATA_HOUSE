# import sys
# sys.path.append(r"E:\pycharm\database")
import os
from database import DatabaseConnection
from datetime import datetime, date, timedelta
from EmailSender_New import EmailSender_New
from dateutil.relativedelta import relativedelta

# 获取当前日期
today = date.today()
# 计算7天前的日期
formatted_date = today.strftime("%Y%m%d")
seven_days_ago = today - timedelta(days=7)
# 计算昨天的日期
yesterday = today - timedelta(days=1)


sql_query = f"""
  -- @description: 泰康江苏保司复核案件层数据
  -- @param:
  -- @return:
  -- @alter:
  -- @time: 2024-11-12 15:01:06
  -- @author: 01
  -- @version: 1.0.0
SELECT DISTINCT
    c.customer_name AS "姓名",
    c.id_no AS "证件号",
    c.acceptance_no AS "受理编号",
    cp.channel_group_policy_no AS "保单号",
    cai.N_INVOICE_SUM AS "申请金额",
    cai.C_CATEG_SELFPAY AS "分类自付",
    cai.C_SELF_EXPENSE AS "自费",
    cai.N_OVERALL_AMT AS "医保金额",
    cai.N_DEDUCT_AMT AS "扣减金额",
    cai.C_SELF_EXPENSE AS "自费金额",
    cai.C_CATEG_SELFPAY AS "分类自付",
    cai.N_THIRD_PAY_AMT AS "第三方支付金额",
    cai.N_REASONABLE_AMT AS "可理算金额",
    cai.N_FINAL_COMPENSATE_AMT AS "赔付金额",
    CASE
        WHEN cai.C_COMPENSATE_RESULT = '1' THEN '正常赔付'
        WHEN cai.C_COMPENSATE_RESULT = '4' THEN '拒赔'
    END AS "审核结论（申请层）",
    cai.C_INTERNAL_CONCLUSION AS "结论描述（申请层）"
FROM
    claim_ods.claim c
        LEFT JOIN claim_ods.clm_app_info cai ON cai.C_CUSTOM_APP_NO = c.claim_no AND cai.C_DEL_FLAG = '0'
        LEFT JOIN claim_ods.clm_visit_inv_info inv ON inv.C_CUSTOM_APP_NO = c.claim_no AND inv.C_PLY_NO = cai.C_PLY_NO AND inv.c_del_flag = '0' AND inv.C_BILL_TYP <> '3'
        LEFT JOIN claim_ods.accept_list_record alr ON alr.ACCEPT_NUM = cai.ACCEPTANCE_NO AND alr.del_flag = '0'
        LEFT JOIN claim_ods.claim_policy cp ON cai.C_PLY_NO = cp.policy_no
WHERE
    alr.INSURE_COMPANY_CHANNEL = 'TK09'
    AND alr.DEL_FLAG = '0'
    AND c.clm_process_status = '9'
    AND c.clm_process = '90';


"""


sql_query_2 = f"""
  -- @description: 泰康江苏保司复核发票层数据
  -- @param:
  -- @return:
  -- @alter:
  -- @time: 2024-11-12 15:01:06
  -- @author: 01
  -- @version: 1.0.0

SELECT DISTINCT
    c.customer_name AS "姓名",
    c.id_no AS "证件号",
    c.acceptance_no AS "受理编号",
    cp.channel_group_policy_no AS "保单号",
    inv.C_INV_NO AS "发票号",
    inv.C_RESPONSE_DESC AS "责任",
    inv.N_SUM_AMT AS "总金额",
    inv.N_OVERALL_AMT AS "医保支付金额",
    inv.N_DEDUCT_AMT AS "扣减金额",
    inv.N_THIRD_PAY_AMT AS "第三方支付金额",
    inv.N_REASONABLE_AMT AS "可理算金额",
    inv.N_FINAL_PAY AS "赔付金额",
    CASE 
        WHEN inv.C_COMPENSATE_RESULT = '1' THEN '正常赔付'
        WHEN inv.C_COMPENSATE_RESULT = '4' THEN '拒赔'
    END AS "审核结论（账单层）",
    inv.C_INTERNAL_CONCLUSION AS "结论描述（账单层）"
FROM claim_ods.claim c
LEFT JOIN claim_ods.clm_app_info cai ON 
    cai.C_CUSTOM_APP_NO = c.claim_no 
    AND cai.C_DEL_FLAG = '0'
    AND cai.INSURANCE_COMPANY = 'TK09'
LEFT JOIN claim_ods.clm_visit_inv_info inv ON 
    inv.C_CUSTOM_APP_NO = c.claim_no 
    AND inv.C_PLY_NO = cai.C_PLY_NO 
    AND inv.c_del_flag = '0' 
    AND inv.C_BILL_TYP <> '3' 
    AND inv.INSURANCE_COMPANY = 'TK09'
LEFT JOIN claim_ods.accept_list_record alr ON 
    alr.ACCEPT_NUM = cai.ACCEPTANCE_NO 
    AND alr.del_flag = '0'
    AND alr.INSURE_COMPANY_CHANNEL = 'TK09'
LEFT JOIN claim_ods.postback_record ic ON 
    ic.app_no = c.claim_no 
    AND ic.is_deleted = 'N'
    AND ic.insure_company_channel = 'TK09'
LEFT JOIN claim_ods.claim_policy cp ON 
    cai.C_PLY_NO = cp.policy_no
WHERE 
    c.INSURE_COMPANY_CHANNEL = 'TK09'
    AND alr.DEL_FLAG = '0'
    AND inv.C_IS_NEED_SHOW = '0'
    AND inv.c_del_flag = '0'
    AND c.clm_process_status = '9'
    AND c.clm_process = '90'

"""

def send_weekly_report_email(formatted_date, excel_filename1, excel_filename2=None):
    # 配置邮件信息
    email_config = {
        'sender':'messages-noreply@insuresmart.com.cn',
        'password': 'M&H@&2z9E65q',  # SMTP授权码
        # 'recipients': ['cuisl@insuresmart.com.cn'],
        'recipients': ['cuisl@insuresmart.com.cn',
                       'sunyz10@taikanglife.com',
                        'xingyq04@taikanglife.com',
                        'tkyl@insuresmart.com.cn',
                        'wanghh@insuresmart.com.cn',
                        'chensy@insuresmart.com.cn'],
        'smtp_server':'smtp.exmail.qq.com',
        'smtp_port': 465
    }

    # 创建EmailSender实例
    sender = EmailSender_New(
        email_config['sender'],
        email_config['password'],
        email_config['recipients'],
        email_config['smtp_server'],
        email_config['smtp_port']
    )

    # 构造邮件内容
    subject = '泰康江苏保司复核案件数据'
    message = f"各位老师好！\n\n 请查收泰康江苏保司复核案件 {formatted_date} 数据统计！\n\n"
    message += "具体信息见附件：\n"

    # 发送邮件，根据是否有第二个附件进行不同处理
    if excel_filename2 is None:
        sender.send_email_with_attachment(subject, message, excel_filename1)
    else:
        sender.send_email_with_attachments(subject, message, [excel_filename1, excel_filename2])
def execute_data(sql_query,excel_filename):
    with DatabaseConnection() as db:
        df = db.execute_query_to_dataframe(sql_query)
        #print(df.head(3))
        #excel_filename = "E:\\pycharm\\output\\" + f"平安机构进件量_{formatted_date}.xlsx"
        #print(excel_filename)
        df.to_excel(excel_filename, index=False)




if __name__ == "__main__":
    excel_filename1 = "/opt/adb_report/report_claim/output/" + f"泰康江苏-复核-案件层-{formatted_date}.xlsx"
    excel_filename2 = "/opt/adb_report/report_claim/output/" + f"泰康江苏-复核-发票层-{formatted_date}.xlsx"
    start_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    print("程序开始时间：", start_time)
    execute_data(sql_query,excel_filename1)
    execute_data(sql_query_2, excel_filename2)
    send_weekly_report_email(formatted_date, excel_filename1,excel_filename2)
    end_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    print("程序结束时间：", end_time)

