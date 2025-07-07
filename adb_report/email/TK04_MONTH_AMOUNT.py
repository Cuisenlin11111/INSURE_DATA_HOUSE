# import sys
# sys.path.append(r"E:\pycharm\database")
import os
from database import DatabaseConnection
from datetime import datetime, date, timedelta
from EmailSender_New import EmailSender_New
from dateutil.relativedelta import relativedelta

# 获取当前日期
current_time = datetime.now()
last_month_time = current_time - timedelta(days=current_time.day)
last_month_time = last_month_time.replace(day=1)
last_month = last_month_time.strftime('%Y-%m')
last_month_formatted = last_month_time.strftime('%Y%m')


sql_query = f"""
  -- @description: 泰康北分燃气----金额-----数据
  -- @param:
  -- @return:
  -- @alter:
  -- @time: 2024-11-12 15:01:06
  -- @author: 01
  -- @version: 1.0.0
SELECT 
    temp.claim_no 案件号,
    temp.accept_num 受理编号, 
    temp.channel_group_policy_no 保单号, 
    temp.insured_name 姓名,
    temp.insured_id_no 证件号,
    temp.insurance_claim_no 保司赔案号,
    CASE 
        WHEN temp.C_RESPONSE_DESC = '门诊' THEN compensation_amount 
        ELSE 0 
    END AS 门诊赔付金额,
    CASE 
        WHEN temp.C_RESPONSE_DESC = '住院' THEN compensation_amount 
        ELSE 0 
    END AS 住院赔付金额,
    CASE 
        WHEN temp.C_RESPONSE_DESC = '额度药' THEN compensation_amount 
        ELSE 0 
    END AS 额度药,
    temp.back_time 回传保司时间
FROM
(
    SELECT 
        DISTINCT
        c.claim_no,
        alr.accept_num, 
        cp.channel_group_policy_no,
        pr.insurance_claim_no,
        cpc.insured_name,
        cpc.insured_id_no,
        inv.C_RESPONSE_DESC,
        SUM(inv.N_FINAL_PAY) compensation_amount,
        pr.back_time
    FROM 
        claim_ods.accept_list_record alr
    LEFT JOIN 
        claim_ods.claim c ON c.acceptance_no = alr.ACCEPT_NUM AND c.INSURE_COMPANY_CHANNEL = 'TK04'
    LEFT JOIN 
        claim_ods.clm_app_info cai ON cai.C_CUSTOM_APP_NO = c.claim_no AND cai.C_DEL_FLAG = '0' AND cai.INSURANCE_COMPANY = 'TK04'
    LEFT JOIN 
        claim_ods.claim_policy cp ON cp.policy_no = cai.C_PLY_NO AND cp.INSURE_COMPANY_CHANNEL = 'TK04'
    LEFT JOIN 
        claim_ods.claim_policy_customer cpc ON cpc.customer_no = cp.customer_no AND cpc.INSURE_COMPANY_CHANNEL = 'TK04'
    LEFT JOIN 
        claim_ods.postback_record pr ON pr.accept_num = alr.ACCEPT_NUM AND pr.insure_company_channel = 'TK04' AND pr.is_deleted = 'N'
    LEFT JOIN 
        claim_ods.clm_visit_info inv ON inv.C_CUSTOM_APP_NO = c.claim_no AND inv.C_DEL_FLAG = '0'
    WHERE 
        alr.INSURE_COMPANY_CHANNEL = 'TK04'
        AND cp.channel_group_policy_no IN ('2851014830623', '2851015275105') 
        AND substr(pr.back_time, 1, 7) = '{last_month}'
        AND alr.DEL_FLAG = '0'
        AND pr.back_status IN ('2', '21')
    GROUP BY 
        c.claim_no,
        alr.accept_num,
        pr.insurance_claim_no,
        inv.C_RESPONSE_DESC
) temp
ORDER BY 
    temp.claim_no,
    temp.accept_num,
    temp.insurance_claim_no;


"""



def send_weekly_report_email(formatted_date, excel_filename1, excel_filename2=None):
    # 配置邮件信息
    email_config = {
        'sender':'messages-noreply@insuresmart.com.cn',
        'password': 'M&H@&2z9E65q',  # SMTP授权码
        # 'recipients': ['cuisl@insuresmart.com.cn'],
        'recipients': ['cuisl@insuresmart.com.cn', 'wangle@insuresmart.com.cn','dangrm@insuresmart.com.cn'],
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
    subject = '泰康北分燃气月度赔付金额'
    message = f"各位老师好！\n\n 泰康北分燃气月度赔付金额 {formatted_date} 数据统计！\n\n"
    message += "具体信息见附件：\n"

    # 发送邮件，根据是否有第二个附件进行不同处理
    if excel_filename2 is None:
        sender.send_email_with_attachment(subject, message, excel_filename1)
    else:
        sender.send_email_with_attachments(subject, message, [excel_filename1, excel_filename2])
def execute_data(sql_query,excel_filename):
    with DatabaseConnection() as db:
        df = db.execute_query_to_dataframe(sql_query)
        df.to_excel(excel_filename, index=False)




if __name__ == "__main__":
    excel_filename1 = "/opt/adb_report/report_claim/output/" + f"泰康北分燃气赔付金额-{last_month_formatted}.xlsx"
    start_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    print("程序开始时间：", start_time)
    execute_data(sql_query,excel_filename1)
    send_weekly_report_email(last_month_formatted, excel_filename1)
    end_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    print("程序结束时间：", end_time)

