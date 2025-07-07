# import sys
# sys.path.append(r"E:\pycharm\database")
import os
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
  -- @description: 中智案件监控月度任务统计数据
  -- @param:
  -- @return:
  -- @alter:
  -- @time: 2024-11-06 15:01:06
  -- @author: 01
  -- @version: 1.0.0
SELECT
     fsr.file_num 中智案卷号,
case when alr.claim_source = '1' then '线下'
 WHEN alr.claim_source = '2' THEN '线上'
 WHEN alr.claim_source = '3' THEN '线上转线下'
 WHEN alr.claim_source = '4' THEN '线下转线上'
end as '进件渠道',
alr.ACCEPT_NUM 受理编号,
c.claim_no 案件号,
if(alr.claim_source = '1',alr.T_CRT_TIME,alr.ACCEPT_DATE)  受理时间,
cai.C_APP_CERT_CDE 证件号,
case
when qc.gmt_created IS NOT NUll then '是'
else null END as 是否问题件,
sum(cai.N_FINAL_COMPENSATE_AMT) as 中智赔付金额,
cp.channel_customer_no as 雇员编号,
 ccit.影像数量 AS 影像数量,
bb.bill_num_1 as 门诊发票数量,
bb.bill_num_2 as 住院发票数量,
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
WHEN 10 THEN '已回传中智'
when 11 then '撤案'
end as 案件状态,
c.create_time 录入时间,
cat.T_CLOSING_CASE_TM 结案日期 ,
fsr.create_time 回传中智日期 ,
cat.C_HANDLE_STAFF 审核人,
C_REVIEWER_STAFF 复核人
FROM claim_ods.accept_list_record alr
LEFT JOIN  claim_ods.claim c ON c.acceptance_no = alr.ACCEPT_NUM AND  c.insure_company_channel = 'CP01'  and c.delete_flag = '0'
LEFT JOIN  (select claim_no,gmt_created
from claim_ods.question_claim
where belong_company = 'CP01'
and is_deleted = 'N'
group by claim_no) qc on qc.claim_no = c.claim_no
LEFT JOIN  claim_ods.case_audit_task cat ON cat.C_CLAIM_CASE_NO = c.claim_no AND  cat.INSURE_COMPANY_CHANNEL = 'CP01' and cat.C_DEL_FLAG = '0'
LEFT JOIN  claim_ods.clm_app_info cai ON cai.C_CUSTOM_APP_NO = c.claim_no and cai.C_DEL_FLAG = '0' AND cai.CHANNEL_TYPE = 'M'
LEFT JOIN  claim_ods.claim_policy cp ON cp.policy_no = cai.C_PLY_NO  and cp.is_deleted = 'N' AND cp.CHANNEL_TYPE = 'M'
LEFT JOIN  claim_ods.front_seq_record fsr ON fsr.accept_num = alr.ACCEPT_NUM AND   fsr.is_deleted = 'N' and fsr.INSURE_COMPANY_CHANNEL = 'CP01' and fsr.module_type = '1' and fsr.state in ('3','4')
left JOIN  (select  ACCEPTANCE_NUM,count(distinct  id)  as 影像数量
from claim_ods.clm_case_image_track
where  INSURE_COMPANY_CHANNEL = 'CP01'
and   DEL_FLAG = '0'
group by ACCEPTANCE_NUM) ccit ON alr.ACCEPT_NUM = ccit.ACCEPTANCE_NUM
left JOIN  (select claim_no,
       count(DISTINCT case  WHEN treatment_date is NOT NULL then bill_no ELSE  null  end ) bill_num_1 ,
       count(DISTINCT case  WHEN treatment_date is  NULL then bill_no ELSE  null  end )  bill_num_2
from claim_ods.bill
where INSURE_COMPANY_CHANNEL = 'CP01'
  and delete_flag = '0'
group by claim_no)   bb ON bb.claim_no = c.claim_no
WHERE alr.DEL_FLAG = '0'
and alr.INSURE_COMPANY_CHANNEL = 'CP01'
and alr.ACCEPT_STATUS <> '1'
and fsr.`state` in ('4')
and (substring(fsr.file_num,7,6) = '{last_month_formatted}' or
   (substring(fsr.file_num,9,6) = '{last_month_formatted}' AND substring(fsr.file_num,7,2) = 'WW'))
GROUP BY alr.ACCEPT_NUM
"""


import os

def send_weekly_report_email(formatted_date, excel_filename1 ,excel_filename2=None):
    # 配置邮件信息
    email_config = {
       'sender':'messages-noreply@insuresmart.com.cn',
        'password': 'M&H@&2z9E65q',  # SMTP授权码
       #'recipients': ['wangle@insuresmart.com.cn'],
       'recipients': ['cuisl@insuresmart.com.cn','wangle@insuresmart.com.cn'],
       'smtp_server':'smtp.exmail.qq.com',
       'smtp_port': 465
    }

    sender = EmailSender_New(
        email_config['sender'],
        email_config['password'],
        email_config['recipients'],
        email_config['smtp_server'],
        email_config['smtp_port']
    )

    # 构造邮件内容
    subject = '中智案件监控月报数据'
    message = f"各位老师好！\n\n 中智案件监控月报数据 {formatted_date}数据统计！\n\n"
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
    excel_filename1 = "/opt/adb_report/report_claim/output/" + f"中智案件监控—{last_month_formatted}.xlsx"
    start_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    print("程序开始时间：", start_time)
    execute_data(sql_query_1,excel_filename1)
    send_weekly_report_email(last_month_formatted, excel_filename1)
    end_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    print("程序结束时间：", end_time)

