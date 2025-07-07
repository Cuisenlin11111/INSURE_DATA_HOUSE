# import sys
# sys.path.append(r"E:\pycharm\database")
from database import DatabaseConnection
from datetime import datetime, date, timedelta
from emailSender import EmailSender
from dateutil.relativedelta import relativedelta
today = datetime.now()
# 获取当前日期
yesterday = today - timedelta(days=1)
# 格式化日期为 YYYY-MM-DD
formatted_yesterday = yesterday.strftime('%Y-%m-%d')





sql_query = f"""
  -- @description: 太保财超过5000案件数据
  -- @param:
  -- @return:
  -- @alter:
  -- @time: 2024-09-04 15:01:06
  -- @author: 01
  -- @version: 1.0.0
select distinct
    cpc.insured_name 姓名,
    cai.N_COMPENSATE_AMT 赔付金额,
    pr.insurance_claim_no  保司赔案号,
    p.C_DEPT_NAME 投保单位,
    alr.department_code 分支,
    pr.back_time  回传时间
 FROM claim_ods.accept_list_record alr
LEFT JOIN claim_ods.claim c on c.acceptance_no = alr.ACCEPT_NUM
LEFT JOIN claim_ods.clm_app_info cai on cai.C_CUSTOM_APP_NO = c.claim_no and cai.C_DEL_FLAG = '0'
LEFT JOIN claim_ods.claim_policy cp on cp.policy_no = cai.C_PLY_NO
LEFT JOIN claim_ods.claim_policy_customer cpc on cpc.customer_no = cp.customer_no
LEFT JOIN claim_ods.postback_record pr on pr.accept_num=alr.ACCEPT_NUM  and pr.is_deleted = 'N'
 left join claim_ods.ply p on cp.group_policy_no = p.C_PLY_NO
WHERE alr.INSURE_COMPANY_CHANNEL = 'CP10'
 AND  cai.N_COMPENSATE_AMT > 5000
 and alr.department_code='大连分公司'
 and  substring(pr.back_time,1,10) ='{formatted_yesterday}'

"""


def execute_data(sql_query, excel_filename):
    with DatabaseConnection() as db:
        df = db.execute_query_to_dataframe(sql_query)
        # 检查DataFrame是否为空
        if df.empty:
            #print("没有找到符合条件的数据记录。")
            return None
        else:
            # 如果有数据，将其保存到Excel文件中
            df.to_excel(excel_filename, index=False)
            return df


def send_weekly_report_email(formatted_date, excel_filename, data_exists=True):
    # 配置邮件信息
    email_config = {
        'sender': 'messages-noreply@insuresmart.com.cn',
        'password': 'M&H@&2z9E65q',  # SMTP授权码
         'recipients': ['cuisl@insuresmart.com.cn','wangheng@insuresmart.com.cn','renjc@insuresmart.com.cn'],
        #'recipients': ['cuisl@insuresmart.com.cn'],
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

    # 根据是否有数据构造邮件内容
    if data_exists:
        subject = '太保财-大连超5000赔付数据'
        message = f"各位老师好！\n\n 请查收 太保财超过5000案件数据 {formatted_date}  理赔明细数据 统计 ！\n\n"
        message += "具体信息见附件：\n"  # 这里您可以添加查询结果或更详细的信息
    else:
        subject = '太保财-大连超5000赔付数据 - 无数据'
        message = f"各位老师好！\n\n 在{formatted_date}这天，没有找到符合条件的数据记录。一切正常。\n\n"

    # 发送带有附件的邮件
    if data_exists:
        sender.send_email_with_attachment(subject, message, excel_filename)
    else:
        sender.send_email(subject, message )


if __name__ == "__main__":
    #excel_filename = "E:\\pycharm\\output\\" + f"太保财超过5000_{formatted_yesterday}.xlsx"
    excel_filename = "/opt/adb_report/report_claim/output/" + f"太保财超过5000_{formatted_yesterday}.xlsx"
    start_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    print("程序开始时间：", start_time)

    # 执行SQL查询并获取结果
    df = execute_data(sql_query, excel_filename)

    # 根据查询结果发送邮件
    send_weekly_report_email(formatted_yesterday, excel_filename, data_exists=df is not None)

    end_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    print("程序结束时间：", end_time)
