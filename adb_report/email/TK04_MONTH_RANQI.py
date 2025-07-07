# import sys
# sys.path.append(r"E:\pycharm\database")
from database import DatabaseConnection
from datetime import datetime, date, timedelta
from emailSender import EmailSender

today = datetime.today()
# 计算昨天的日期
yesterday = today - timedelta(days=1)

# 获取上一个月的日期
prev_month_date = (yesterday.replace(day=1) - timedelta(days=1))
# 格式化为 "YYYY-MM" 格式
formatted_prev_month = prev_month_date.strftime('%Y-%m')
# 提取昨天的年份和月份，并格式化为 "YYYY-MM" 格式
formatted_date = yesterday.strftime('%Y-%m')

sql_query = f"""
  -- @description: 泰康北分燃气月度理赔明细数据
  -- @param:
  -- @return:
  -- @alter:
  -- @time: 2024-08-09 15:01:06
  -- @author: 01
  -- @version: 1.0.0
select distinct
case when alr.claim_source = '1' then '线下'
 WHEN alr.claim_source = '2' THEN '线上'
 WHEN alr.claim_source = '3' THEN '线上转线下'
 WHEN alr.claim_source = '4' THEN '线下转线上'
 END as '线上/线下', alr.accept_batch_no 收单批次号,pr.insurance_claim_no 甲方案件号,alr.ACCEPT_NUM 受理编号,
case when cp.channel_group_policy_no is null then alr.policy_no else cp.channel_group_policy_no end 团单号, p.C_DEPT_NAME 投保单位,
cpc.insured_name 出险人姓名,
cpc.insured_id_no 证件号码,
group_concat(distinct inv.c_response_desc) 赔付责任,
-- count(distinct inv.C_INV_NO)
count(distinct inv.C_INV_NO) 账单数量,cai.N_FINAL_COMPENSATE_AMT 赔付金额, alr.ACCEPT_DATE as 受理时间, icrr.end_case_date 结案时间,
case c.clm_process_status
WHEN 0 then '预审'
WHEN 1 then '标准化'
WHEN 2 then '扣费'
WHEN 3 then '定责'
WHEN 4 then '理算'
when 5 then '风控'
WHEN 6 then '审核'
WHEN 7 then '复核'
WHEN 8 then
case when alr.ACCEPT_SUB_STATUS = '91' then '保险公司待复核'
when alr.ACCEPT_SUB_STATUS = '92' then '保险公司暂存'
when alr.ACCEPT_SUB_STATUS = '93' then '保险公司复核通过'
else '内部结案' END
WHEN 9 then
case when alr.ACCEPT_SUB_STATUS = '91' then '保险公司待复核'
when alr.ACCEPT_SUB_STATUS = '92' then '保险公司暂存'
when alr.ACCEPT_SUB_STATUS = '93' then '保险公司复核通过'
else '回传'
end
WHEN 10 THEN '已回传保司中介'
when 11 then '撤案'
end as 案件状态,
REPLACE(REPLACE(REPLACE(cai.C_INTERNAL_CONCLUSION,CHAR(10),''),CHAR(13),''),CHAR(9),'') 赔付结论,
DATE_FORMAT(cp.insure_effective_date,'%Y') 赔付年度, sum(case when inv.C_RESPONSE_DESC = '额度药' then inv.N_FINAL_PAY else 0 end) 额度药使用,
               cp.grade_name 人员层级, cp.retirement_time 退休时间

 FROM claim_ods.accept_list_record alr
LEFT JOIN claim_ods.claim c on c.acceptance_no = alr.ACCEPT_NUM
LEFT JOIN claim_ods.clm_app_info cai on cai.C_CUSTOM_APP_NO = c.claim_no and cai.C_DEL_FLAG = '0'
LEFT JOIN claim_ods.claim_policy cp on cp.policy_no = cai.C_PLY_NO
     LEFT JOIN claim_ods.claim_policy_customer cpc on cpc.customer_no = cp.customer_no
    left join claim_ods.ply p on cp.group_policy_no = p.C_PLY_NO
LEFT JOIN claim_ods.postback_record pr on pr.accept_num=alr.ACCEPT_NUM  and pr.is_deleted = 'N'
LEFT JOIN claim_ods.clm_visit_inv_info  inv on inv.C_CUSTOM_APP_NO=c.claim_no  and inv.C_BILL_TYP <> '3' and inv.C_DEL_FLAG = '0' AND inv.INSURANCE_COMPANY like 'TK%'
left join claim_ods.`case_audit_task` cat on c.claim_no = cat.C_CLAIM_CASE_NO and cat.C_DEL_FLAG = '0'
left join claim_ods.`insurance_company_review_record` icrr on c.claim_no = icrr.app_no and icrr.is_deleted='N'
WHERE alr.INSURE_COMPANY_CHANNEL = 'TK04'
and    substr(pr.back_time,1,7)   in  ('{formatted_date}','{formatted_prev_month}')
and cp.channel_group_policy_no in ('2851014830623')
# and alr.business_mode = 'I'
and alr.DEL_FLAG = '0'
GROUP BY alr.ACCEPT_NUM

"""

def send_weekly_report_email(formatted_date,excel_filename):
    # 配置邮件信息
    email_config = {
        'sender': 'messages-noreply@insuresmart.com.cn',
        'password': 'M&H@&2z9E65q',  # SMTP授权码
        'recipients': ['cuisl@insuresmart.com.cn','bjgasreimburse@163.com','tkyl@insuresmart.com.cn','dangrm@insuresmart.com.cn'],
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
    subject = '泰康北分燃气月度理赔明细数据'
    message = f"各位老师好！\n\n 请查收 泰康北分燃气月度 {formatted_date}  理赔明细数据 统计 ！\n\n"
    message += "具体信息见附件：\n"  # 这里您可以添加查询结果或更详细的信息


    # 发送带有附件的邮件
    sender.send_email_with_attachment(subject, message, excel_filename)
def execute_data(sql_query,excel_filename):
    with DatabaseConnection() as db:
        df = db.execute_query_to_dataframe(sql_query)
        #print(df.head(3))
        #excel_filename = "E:\\pycharm\\output\\" + f"平安机构进件量_{formatted_date}.xlsx"
        #print(excel_filename)
        df.to_excel(excel_filename, index=False)




if __name__ == "__main__":
    # excel_filename = "E:\\pycharm\\output\\" + f"北分燃气案件数据_{formatted_date}.xlsx"
    excel_filename = "/opt/adb_report/report_claim/output/" + f"北分燃气案件数据_{formatted_date}.xlsx"
    start_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    print("程序开始时间：", start_time)
    execute_data(sql_query,excel_filename)
    send_weekly_report_email(formatted_date,excel_filename)
    end_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    print("程序结束时间：", end_time)

