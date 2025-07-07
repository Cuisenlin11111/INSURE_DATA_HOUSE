# import sys
# sys.path.append(r"E:\pycharm\database")
from database import DatabaseConnection
from datetime import datetime, date, timedelta
from EmailSender_New import EmailSender_New

# 获取当前时间
current_time = datetime.now()
last_month_time = current_time - timedelta(days=current_time.day)
last_month_time = last_month_time.replace(day=1)
last_month = last_month_time.strftime('%Y-%m')
last_month_formatted = last_month_time.strftime('%Y%m')



sql_query_1 = f"""
  -- @description: 中智案件明细月度任务统计数据
  -- @param:
  -- @return:
  -- @alter:
  -- @time: 2024-11-06 15:01:06
  -- @author: 01
  -- @version: 1.0.0
select DISTINCT
a.accept_num 受理编号,
(case when a.claim_source = '1' then '线下' when a.claim_source = '2' then '线上' when a.claim_source = '3' then '线上转线下' when a.claim_source = '4' then '线下转线上' end )渠道,
a.T_CRT_TIME 受理时间,
case c.clm_process_status
WHEN 0 then '预审'
WHEN 1 then '标准化'
WHEN 2 then '扣费'
WHEN 3 then '定责'
WHEN 4 then '理算'
when 5 then '风控'
WHEN 6 then '审核'
WHEN 7 then '复核'
WHEN 8 then '内部结案'
WHEN 9 then '已回传保司'
WHEN 10 THEN '已回传保司中介'
when 11 then '撤案'
end as 案件状态,
cat.T_UPD_TM 结案时间,
fr.create_time 回传中智时间,
TIMESTAMPDIFF(HOUR, if(a.claim_source = '1',a.T_CRT_TIME,a.ACCEPT_DATE),cat.T_UPD_TM) 结案时长,
IF(q.claim_no is not null, '是', '') 是否发起过问题件,
MAX(q.conclusion_time) 问题件最后一次回销时间
FROM claim_ods.`accept_list_record` a
join claim_ods.claim c on a.accept_num = c.acceptance_no and c.delete_flag = '0' and c.insure_company_channel = 'CP01'
LEFT JOIN claim_ods.front_seq_record fr on fr.app_no = c.claim_no and fr.is_deleted = 'N' and fr.state in ('3','4')
left join claim_ods.question_claim q on q.claim_no = c.claim_no and q.belong_company = 'CP01'
left join claim_ods.`case_audit_task` cat on c.claim_no = cat.C_CLAIM_CASE_NO and cat.C_DEL_FLAG = '0' and cat.C_SUB_STATUS = '79' and cat.insure_company_channel = 'CP01'
WHERE a.DEL_FLAG = '0'
and a.INSURE_COMPANY_CHANNEL = 'CP01'
and a.ACCEPT_STATUS <> '1'
and  substr(a.T_CRT_TIME,1,7) ='{last_month}'
group by a.accept_num
order by  a.accept_num
"""







def send_weekly_report_email(formatted_date, excel_filename1 ,excel_filename2=None):
    # 配置邮件信息
    email_config = {
       'sender':'messages-noreply@insuresmart.com.cn',
        'password': 'M&H@&2z9E65q',  # SMTP授权码
       # 'recipients': ['cuisl@insuresmart.com.cn'],
        'recipients': ['cuisl@insuresmart.com.cn','wangle@insuresmart.com.cn'],
       'smtp_server':'smtp.exmail.qq.com',
       'smtp_port': 465
    }

    # 创建EmailSender实例（这里可以简化，直接使用smtplib相关功能）
    sender = EmailSender_New(
        email_config['sender'],
        email_config['password'],
        email_config['recipients'],
        email_config['smtp_server'],
        email_config['smtp_port']
    )

    # 构造邮件内容
    subject = '中智案件明细月报数据'
    message = f"各位老师好！\n\n 中智案件明细统计月报数据 {formatted_date}数据统计！\n\n"
    message += "具体信息见附件：\n"


    # 获取文件名部分
    if excel_filename2 is None:
        sender.send_email_with_attachment(subject, message, excel_filename1)
    else:
        sender.send_email_with_attachments(subject, message, [excel_filename1, excel_filename2])


def execute_data(sql_query,excel_filename):
    with DatabaseConnection() as db:
        df = db.execute_query_to_dataframe(sql_query)
        df.to_excel(excel_filename, index=False)




if __name__ == "__main__":
    excel_filename1 = "/opt/adb_report/report_claim/output/" + f"中智案件明细-{last_month_formatted}.xlsx"
    start_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    print("程序开始时间：", start_time)
    execute_data(sql_query_1,excel_filename1)
    send_weekly_report_email(last_month_formatted, excel_filename1)
    end_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    print("程序结束时间：", end_time)

