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
SELECT DISTINCT
alr.ACCEPT_NUM 订单号,
alr.report_no 保司赔案号,
alr.department_code 部门,
alr.T_CRT_TIME 受理时间,
replace(json_extract(cp.extra,"$.productName"),'"','') 产品名称,
case alr.product_type when 'Z' then '雇主' when 'P' then '意健险-个险' when 'G' then '意健险-团险' when 'C' then '车险' else '未知' end 产品类别,
c.claim_no 案件号,
pr.back_time 回传时间,
case when pr.postback_way = 'H' then '半流程'
when pr.postback_way = 'W' then '全流程'
end as 回传方式,
REPLACE(REPLACE(REPLACE(b.bill_no,CHAR(10),''),CHAR(13),''),CHAR(9),'') 发票号,
case b.person_flag when 'Y' then '是' else '否' end 是否转人工,
IF(b.treatment_date is not null,'门诊','住院') 就诊类型,
case b.bill_type when '1' then '普通'  when '2' then '医保' else '结算单' end 发票类型,
case b.accident_type when 'YW' then '意外' when 'JB' then '疾病' when 'ALL' then '全部' else '' end 意外或疾病,
IF(b.treatment_date is not null,b.treatment_date,b.in_hospital_date) 就诊起期,
IF(b.treatment_date is not null,b.treatment_date,b.out_hospital_date) 就诊止期,
substring_index(b.match_diagnose_name,'|',1) 诊断描述,
b.hospital_name 医院名称,
concat(bhlv3.province_name,bhlv3.city_name) 医院所属地,
case when inv.N_SUM_AMT is null then b.bill_amount else inv.N_SUM_AMT end 总金额,
case when inv.N_CATEG_SELFPAY is null then b.class_pay_amount else inv.N_CATEG_SELFPAY end 分类自付,
case when inv.N_SELF_EXPENSE is null then b.own_pay_amount else inv.N_SELF_EXPENSE end 自费金额,
case when inv.N_SOCIAL_GIVE_AMOUNT is null then b.social_pay_amount else inv.N_SOCIAL_GIVE_AMOUNT end 医保支付金额付,
case when inv.N_THIRD_PAY_AMT is null then b.third_party_pay_amount else inv.N_THIRD_PAY_AMT end 第三方支付金额,
case when inv.N_EXTRA_PAY_AMOUNT is null then b.extra_pay_amount else inv.N_EXTRA_PAY_AMOUNT end 附加支付金额,
inv.N_DEDUCT_AMT 扣减金额,
inv.N_DEDUCTLE_AMT 免赔额,
inv.N_FINAL_PAY 最终赔付金额,
inv.C_RESPONSE_DESC 责任名称,
case when inv.C_COMPENSATE_RESULT = '4' then '拒赔'
WHEN inv.C_COMPENSATE_RESULT = '1' THEN '正常赔付'
end as 发票层赔付结论,
REPLACE(REPLACE(inv.C_INTERNAL_CONCLUSION,CHAR(10),''),CHAR(13),'') 发票层结论描述
FROM claim_ods.accept_list_record alr
inner JOIN claim_ods.claim c on c.acceptance_no = alr.ACCEPT_NUM and c.delete_flag = '0' and c.insure_company_channel = 'PA02'
left join claim_ods.bill b on c.id = b.claim_id and b.delete_flag = '0' and b.insure_company_channel = 'PA02'
LEFT JOIN claim_ods.clm_app_info cai on cai.C_CUSTOM_APP_NO = c.claim_no and cai.C_DEL_FLAG = '0' and cai.INSURANCE_COMPANY = 'PA02'
left join claim_ods.clm_visit_inv_info inv on inv.C_CUSTOM_APP_NO = cai.C_CUSTOM_APP_NO and inv.C_PLY_NO = cai.C_PLY_NO and b.bill_no = inv.C_INV_NO and inv.C_DEL_FLAG = '0' AND inv.INSURANCE_COMPANY = 'PA02'
LEFT JOIN claim_ods.claim_policy cp on cp.policy_no = cai.C_PLY_NO and cp.insure_company_channel = 'PA02'
LEFT JOIN claim_ods.postback_record pr on pr.app_no = c.claim_no and pr.is_deleted = 'N' and pr.insure_company_channel = 'PA02'
left join claim_ods.base_hospital_label_v3 bhlv3 on bhlv3.hospital_code = b.match_hospital_code and bhlv3.is_deleted = 'N'
#inner join (select app_no,min(send_time) 首次回传时间  from postback_trial_log where insure_company_channel = 'PA02' AND is_deleted = 'N' group by app_no having 首次回传时间 >= '2024-07-01' and 首次回传时间 < '2024-08-01') ptl on ptl.app_no = pr.app_no
where 1=1
-- and alr.T_CRT_TIME > '2023-10-01' and alr.T_CRT_TIME < '2023-12-13 00:00:00'
-- and alr.INSURE_COMPANY_CHANNEL = 'PA02' and alr.DEL_FLAG ='0'
and  substr(pr.back_time,1,10) >='{seven_days_ago}'
and  substr(pr.back_time,1,10) <='{yesterday}'
# and  alr.department_code='上海分公司'
and alr.INSURE_COMPANY_CHANNEL = 'PA02' and alr.DEL_FLAG ='0'

"""

def send_weekly_report_email(seven_days_ago, yesterday):
    # 配置邮件信息
    email_config = {
        'sender': 'messages-noreply@insuresmart.com.cn',
        'password': 'M&H@&2z9E65q',  # SMTP授权码
        #'recipients': ['cuisl@insuresmart.com.cn'],
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
    subject = '平安财发票层统计周报'
    message = f"各位老师好！\n\n 请查收 平安财发票层{seven_days_ago} 至 {yesterday} 的数据报表统计 ！\n\n"
    message += "具体信息见附件：\n"  # 这里您可以添加查询结果或更详细的信息
    #excel_filename = f"E:\pycharm\output\平安财上海分公司发票层_{formatted_date}.xlsx"
    excel_filename = f"/opt/adb_report/report_claim/output/平安财发票层_{formatted_date}.xlsx"

    # 发送带有附件的邮件
    sender.send_email_with_attachment(subject, message, excel_filename)
def execute_data(sql_query):
    with DatabaseConnection() as db:
        df = db.execute_query_to_dataframe(sql_query)
        #excel_filename = "E:\\pycharm\\output\\" + f"平安财上海分公司发票层_{formatted_date}.xlsx"
        excel_filename = "/opt/adb_report/report_claim/output/" + f"平安财发票层_{formatted_date}.xlsx"
        #print(excel_filename)
        df.to_excel(excel_filename, index=False)




if __name__ == "__main__":

    start_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    print("程序开始时间：", start_time)
    execute_data(sql_query)
    send_weekly_report_email(seven_days_ago, yesterday)
    end_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    print("程序结束时间：", end_time)

