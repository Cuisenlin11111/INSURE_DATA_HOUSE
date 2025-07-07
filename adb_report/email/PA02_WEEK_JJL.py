# import sys
# sys.path.append(r"E:\pycharm\database")
from database import DatabaseConnection
from datetime import datetime, date, timedelta
from emailSender import EmailSender

today = date.today()
# 计算7天前的日期
formatted_date = today.strftime("%Y%m%d")
seven_days_ago = today - timedelta(days=7)
# 计算昨天的日期
yesterday = today - timedelta(days=1)

sql_query = f"""
  -- @description: 平安各部门进件量数据
  -- @param:
  -- @return:
  -- @alter:
  -- @time: 2024-06-20 15:01:06
  -- @author: 01
  -- @version: 1.0.0
with tmp_11 as (
		select
		        insure_company_channel,
	             substr(T_CRT_TIME,1,10)   T_CRT_TIME,
	             substr(ACCEPT_DATE,1,10)  ACCEPT_DATE,
		         accept_num,
	             ACCEPT_STATUS,
	             claim_source,
	             department_code
  from claim_ods.accept_list_record
      where DEL_FLAG = '0'
    and insure_company_channel = 'PA02'
      and substr(T_CRT_TIME,1,10) >='{seven_days_ago}'
      and substr(T_CRT_TIME,1,10) <='{yesterday}')
      select
	department_code 部门,
	count(*) 进件量
	from tmp_11
 where  insure_company_channel = 'PA02'  and ACCEPT_STATUS<>'1'
	group by department_code
order by  进件量 desc

"""

def send_weekly_report_email(seven_days_ago, yesterday):
    # 配置邮件信息
    email_config = {
        'sender': 'messages-noreply@insuresmart.com.cn',
        'password': 'M&H@&2z9E65q',  # SMTP授权码
        'recipients': ['cuisl@insuresmart.com.cn','wangxy@insuresmart.com.cn'],
        'smtp_server': 'smtp.exmail.qq.com',
        'smtp_port': 465
    }

    # 创建EmailSender实例
    sender = EmailSender(
        email_config['sender'],
        email_config['password'],
        email_config['recipients'],
        email_config['smtp_server'],
        email_config['smtp_port']
    )

    # 构造邮件内容
    subject = '平安各部门进件量统计周报'
    message = f"各位老师好！\n\n 请查收 平安财产险各机构{seven_days_ago} 至 {yesterday} 的进件量统计 ！\n\n"
    message += "具体信息见附件：\n"  # 这里您可以添加查询结果或更详细的信息
    # excel_filename = f"E:\pycharm\output\平安机构进件量_{formatted_date}.xlsx"
    excel_filename = f"/opt/adb_report/report_claim/output/平安机构进件量_{formatted_date}.xlsx"

    # 发送带有附件的邮件
    sender.send_email_with_attachment(subject, message, excel_filename)
def execute_data(sql_query):
    with DatabaseConnection() as db:
        df = db.execute_query_to_dataframe(sql_query)
        # excel_filename = "E:\\pycharm\\output\\" + f"平安机构进件量_{formatted_date}.xlsx"
        excel_filename = "/opt/adb_report/report_claim/output/" + f"平安机构进件量_{formatted_date}.xlsx"
        #print(excel_filename)
        df.to_excel(excel_filename, index=False)




if __name__ == "__main__":

    start_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    print("程序开始时间：", start_time)
    execute_data(sql_query)
    send_weekly_report_email(seven_days_ago, yesterday)
    end_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    print("程序结束时间：", end_time)

