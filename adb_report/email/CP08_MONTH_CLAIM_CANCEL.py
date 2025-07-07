# import sys
# sys.path.append(r"E:\pycharm\database")
from database import DatabaseConnection
from datetime import datetime, date, timedelta
from emailSender import EmailSender

today = datetime.today()
# 计算昨天的日期
yesterday = today - timedelta(days=1)

# 提取昨天的年份和月份，并格式化为 "YYYY-MM" 格式
formatted_date = yesterday.strftime('%Y-%m')

sql_query = f"""
  -- @description: 太保健康撤案月度数据
  -- @param:
  -- @return:
  -- @alter:
  -- @time: 2024-08-09 15:01:06
  -- @author: 01
  -- @version: 1.0.0

SELECT
    DISTINCT
c.claim_no 案件号,
if(d.full_name is null ,e.full_name,d.full_name) 撤案人用户名称,
count(distinct  case when inv.C_INV_NO is not null  then inv.C_INV_NO  else  b.bill_no end) 发票数量

 FROM claim_ods.accept_list_record alr
LEFT JOIN claim_ods.claim c on c.acceptance_no = alr.ACCEPT_NUM
     left join claim_ods.`clm_visit_inv_info` inv on c.claim_no = inv.C_CUSTOM_APP_NO  and inv.C_DEL_FLAG = '0'  and inv.C_IS_NEED_SHOW='0'
      left join claim_ods.bill b on c.id = b.claim_id and b.delete_flag = '0'
left join claim_ods.sys_users d
on c.update_operator =d.id
 left join claim_ods.sys_users e
on alr.C_UPD_CDE =e.id

WHERE alr.INSURE_COMPANY_CHANNEL = 'CP08'
 and  ( substr(c.cancle_time,1,7) = '{formatted_date}'  or ( c.cancle_time is null  and  substr(alr.T_UPD_TIME,1,7)='{formatted_date}'))
and alr.DEL_FLAG = '0'
 and (alr.ACCEPT_STATUS = '5' or c.clm_process_status = '11')

GROUP BY alr.ACCEPT_NUM

"""

def send_weekly_report_email(formatted_date,excel_filename):
    # 配置邮件信息
    email_config = {
        'sender': 'messages-noreply@insuresmart.com.cn',
        'password': 'M&H@&2z9E65q',  # SMTP授权码
        #'recipients': ['cuisl@insuresmart.com.cn'],
        'recipients': ['cuisl@insuresmart.com.cn', 'fanxue@insuresmart.com.cn'],
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
    subject = '太保健康撤案月度数据'
    message = f"各位老师好！\n\n 请查收 太保健康 {formatted_date}  撤案数据 统计 ！\n\n"
    message += "具体信息见附件：\n"  # 这里您可以添加查询结果或更详细的信息


    # 发送带有附件的邮件
    sender.send_email_with_attachment(subject, message, excel_filename)
def execute_data(sql_query,excel_filename):
    with DatabaseConnection() as db:
        df = db.execute_query_to_dataframe(sql_query)
        df.to_excel(excel_filename, index=False)




if __name__ == "__main__":
    excel_filename = "/opt/adb_report/report_claim/output/" + f"太保健康撤案_{formatted_date}.xlsx"
    start_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    print("程序开始时间：", start_time)
    execute_data(sql_query,excel_filename)
    send_weekly_report_email(formatted_date,excel_filename)
    end_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    print("程序结束时间：", end_time)

