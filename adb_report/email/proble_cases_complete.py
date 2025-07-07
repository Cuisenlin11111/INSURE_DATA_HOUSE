# import sys
# sys.path.append(r"E:\pycharm\database")
import os
from database import DatabaseConnection
from datetime import datetime, date, timedelta
from EmailSender_New import EmailSender_New
from dateutil.relativedelta import relativedelta

today = datetime.now()

# 计算昨天的日期
yesterday = (today - timedelta(days=1)).strftime("%Y-%m-%d")

# 计算 7 天前的日期
seven_days_ago = (today - timedelta(days=7)).strftime("%Y-%m-%d")



sql_query = f"""
  -- @description: 问题件回销---数据
  -- @param:
  -- @return:
  -- @alter:
  -- @time: 2024-11-12 15:01:06
  -- @author: 01
  -- @version: 1.0.0

select
    qc.belong_company,
    count(distinct qc.claim_no ) 回销案件量
FROM
    claim_ods.`question_claim` qc
WHERE
     qc.is_deleted = 'N'
and  substr(qc.conclusion_time,1,10) between '{seven_days_ago}'  and  '{yesterday}'
group by  qc.belong_company

"""



def send_weekly_report_email(formatted_date, excel_filename1, excel_filename2=None):
    # 配置邮件信息
    email_config = {
        'sender':'messages-noreply@insuresmart.com.cn',
        'password': 'M&H@&2z9E65q',  # SMTP授权码
         # 'recipients': ['cuisl@insuresmart.com.cn'],
        'recipients': ['cuisl@insuresmart.com.cn', 'wangle@insuresmart.com.cn','yeky@insuresmart.com.cn','yangzy@insuresmart.com.cn','fanxue@insuresmart.com.cn','liyl@insuresmart.com.cn','rjcl@insuresmart.com.cn'],
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
    subject = '问题件回销 -案件量统计'
    message = f"各位老师好！\n\n 问题件回销周 {yesterday} 数据统计！\n\n"
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
    excel_filename1 = "/opt/adb_report/report_claim/output/" + f"问题件回销-{yesterday}.xlsx"
    start_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    print("程序开始时间：", start_time)
    execute_data(sql_query,excel_filename1)
    send_weekly_report_email(yesterday, excel_filename1)
    end_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    print("程序结束时间：", end_time)

