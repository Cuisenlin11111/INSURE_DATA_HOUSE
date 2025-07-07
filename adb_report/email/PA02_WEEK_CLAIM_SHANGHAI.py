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
  -- @description: 平安财案件层数据
  -- @param:
  -- @return:
  -- @alter:
  -- @time: 2024-06-20 15:01:06
  -- @author: 01
  -- @version: 1.0.0
SELECT
alr.ACCEPT_NUM 订单号,
alr.report_no 保司赔案号,
alr.department_code 部门,
alr.T_CRT_TIME 受理时间,
      CASE substr( c.claim_no, 13, 4 )
  WHEN 'SPIC' THEN '施博'
  WHEN 'KNVS' THEN '成都视觉'
  WHEN 'GTRS' THEN '广纳'
  WHEN 'ZZTN' THEN '智在'
  WHEN 'YSJU' THEN '因朔桔'
  ELSE '未知' END  录入商,
case when alr.ACCEPT_STATUS = '1' then '待完善'
when alr.ACCEPT_STATUS = '2' then '已提交'
when alr.ACCEPT_STATUS = '3' then '受理中'
when alr.ACCEPT_STATUS = '4' then '审核中'
when alr.ACCEPT_STATUS = '5' then '撤案'
when alr.ACCEPT_STATUS = '7' then '结案'
when alr.ACCEPT_STATUS = '9' then '保险公司复核'
else '未知' end 受理状态,
alr.danger_name 出险人姓名,
 cpc.insured_name 被保人姓名,
cpc.insured_gender 性别,
cpc.insured_birthday 出生年月,
 TIMESTAMPDIFF(YEAR,cpc.insured_birthday,curdate())AS '年龄',
case
when alr.insured_type = '01' then '标的车'
when alr.insured_type = '02' then '三者车'
when alr.insured_type = '03' then '标的车内物'
when alr.insured_type = '04' then '三者车内物'
when alr.insured_type = '05' then '三者车外物'
when alr.insured_type = '06' then '三者车内人'
when alr.insured_type = '07' then '三者车外人'
when alr.insured_type = '08' then '司机'
when alr.insured_type = '09' then '乘客'
when alr.insured_type = '10' then '雇员'
when alr.insured_type = '11' then '三者'
when alr.insured_type = '12' then '标的'
when alr.insured_type = '13' then '本人'
when alr.insured_type = '14' then '父母'
when alr.insured_type = '15' then '配偶'
when alr.insured_type = '16' then '子女'
when alr.insured_type = '17' then '兄弟姐妹'
when alr.insured_type = '18' then '其他'
 end as 出险类型,
case when pr.back_status = '0' then '待回传'
when pr.back_status = '2' then '回传成功'
when pr.back_status = '21' then '回传成功，待确认'
when pr.back_status = '3' THEN '回传失败'
end as 回传状态,
ptl.send_time 首次保司回传时间,
cp.grade_name  投保层级,
cai.N_FINAL_COMPENSATE_AMT YSJ回传赔付金额,
replace(json_extract(cp.extra,"$.productCode"),'"','') 投保等级代码,
replace(json_extract(cp.extra,"$.productName"),'"','') 投保等级名,
case alr.product_type when 'Z' then '雇主' when 'P' then '意健险-个险' when 'G' then '意健险-团险' when 'C' then '车险' else '未知' end 产品类别,
'' 产品代码,
'' 产品名称,
  case when pr.postback_way = 'H' then '半流程'
when pr.postback_way = 'W' then '全流程'
end as 回传方式,
cp.apply_policy_no 保单号,
replace(json_extract(cp.extra,"$.departmentName"),'"','') 保单机构,
case ol.oper_type
when '1' then '发起问题件'
when '2' then '领取任务'
when '3' then '发送短信'
when '4' then '写备注'
when '5' then '重新分配'
when '6' then '回销'
else '未知' end 操作环节,
qt.type_desc 问题件类型,
ol.operate_time 操作时间,
qt.conclusion_time 回销时间,
qt.modifier 问题件回销人,
replace(json_extract(ol.extra,"$.questionRemark"),'"','') 备注内容,
ptl.max_send_time 最后回传保司时间,

 json_extract(cp.extra,"$.applyClientName") 投保单位名称,
 case when cai.N_INVOICE_SUM is null then s.billAmount else cai.N_INVOICE_SUM end 申请金额,
 case when cai.C_CATEG_SELFPAY is null then s.classAmount else cai.C_CATEG_SELFPAY end 分类自付,
 case when cai.C_SELF_EXPENSE is null then s.ownAmount else cai.C_SELF_EXPENSE end 全自费,
 case when cai.N_OVERALL_AMT is null then s.socialAmount else cai.N_OVERALL_AMT end 统筹支付,
 case when cai.N_THIRD_PAY_AMT is null then s.thirdAmount else cai.N_THIRD_PAY_AMT end 第三方,
 case when cai.N_EXTRA_PAY_AMOUNT is null then s.extraAmount else cai.N_EXTRA_PAY_AMOUNT end 附加支付,
cai.N_DEDUCT_AMT 扣减金额,
cai.N_DEDUCTLE_AMT 免赔额,
 cai.N_FINAL_COMPENSATE_AMT 最终赔付金额,
 b.is_accident 是否交通意外,
 b.major_diseases_type 是否重疾,
 cat.C_HANDLE_STAFF 审核人
FROM claim_ods.accept_list_record alr
LEFT JOIN claim_ods.claim c ON c.acceptance_no = alr.ACCEPT_NUM AND c.insure_company_channel = 'PA02'
LEFT JOIN claim_ods.apply_claim ac ON c.claim_no = ac.apply_no AND ac.delete_flag = '0' AND ac.insure_company_channel = 'PA02'
LEFT JOIN claim_ods.postback_record pr ON pr.app_no = c.claim_no AND pr.is_deleted = 'N' AND pr.insure_company_channel = 'PA02'
LEFT JOIN  (select  ptl.app_no,min(ptl.send_time) send_time,
                max(ptl.send_time) max_send_time
 FROM  claim_ods.postback_trial_log ptl
 where INSURE_COMPANY_CHANNEL = 'PA02'
 and is_deleted = 'N' group by ptl.app_no)  ptl ON pr.app_no = ptl.app_no
LEFT JOIN claim_ods.clm_app_info cai ON cai.C_CUSTOM_APP_NO = c.claim_no AND cai.C_DEL_FLAG = '0' AND cai.INSURANCE_COMPANY = 'PA02'
LEFT JOIN claim_ods.claim_policy cp ON cp.policy_no = ac.policy_part_no AND cp.is_deleted = 'N' AND cp.insure_company_channel = 'PA02'
LEFT JOIN claim_ods.claim_policy_customer cpc ON cp.customer_no = cpc.customer_no AND cpc.insure_company_channel = 'PA02'
LEFT JOIN claim_ods.case_audit_task cat ON cat.C_CLAIM_CASE_NO = cai.C_CUSTOM_APP_NO AND cat.C_DEL_FLAG = '0' AND cat.insure_company_channel = 'PA02'
LEFT JOIN claim_ods.quest_type qt ON c.claim_no = qt.claim_no AND qt.is_deleted = 'N' AND qt.INSURE_COMPANY_CHANNEL = 'PA02'
LEFT JOIN claim_ods.operation_log ol ON CONVERT(qt.quest_id, CHAR) = ol.busi_no AND ol.busi_type = '1' AND ol.is_deleted = 'N'
LEFT JOIN (SELECT b.claim_no, SUM(b.bill_amount) billAmount, SUM(b.social_pay_amount) socialAmount, SUM(b.extra_pay_amount) extraAmount, SUM(b.class_pay_amount) classAmount, SUM(b.own_pay_amount) ownAmount, SUM(b.third_party_pay_amount) thirdAmount FROM claim_ods.bill b WHERE b.insure_company_channel = 'PA02' AND b.delete_flag = '0' GROUP BY b.claim_no) s ON c.claim_no = s.claim_no
LEFT JOIN claim_ods.bill b ON b.claim_no = c.claim_no AND b.delete_flag = '0'

WHERE 1=1
AND alr.INSURE_COMPANY_CHANNEL = 'PA02' AND alr.DEL_FLAG = '0'
and  substr(pr.back_time,1,10) >='{seven_days_ago}'
and  substr(pr.back_time,1,10) <='{yesterday}'
# and  alr.department_code='上海分公司'
GROUP BY
  订单号

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
    subject = '平安财案件层统计周报'
    message = f"各位老师好！\n\n 请查收 平安财案件层{seven_days_ago} 至 {yesterday} 的数据报表统计 ！\n\n"
    message += "具体信息见附件：\n"  # 这里您可以添加查询结果或更详细的信息
    #excel_filename = f"E:\pycharm\output\平安财上海分公司案件层_{formatted_date}.xlsx"
    excel_filename = f"/opt/adb_report/report_claim/output/平安财案件层_{formatted_date}.xlsx"

    # 发送带有附件的邮件
    sender.send_email_with_attachment(subject, message, excel_filename)
def execute_data(sql_query):
    with DatabaseConnection() as db:
        df = db.execute_query_to_dataframe(sql_query)
        #excel_filename = "E:\\pycharm\\output\\" + f"平安财上海分公司案件层_{formatted_date}.xlsx"
        excel_filename = "/opt/adb_report/report_claim/output/" + f"平安财案件层_{formatted_date}.xlsx"
        #print(excel_filename)
        df.to_excel(excel_filename, index=False)




if __name__ == "__main__":

    start_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    print("程序开始时间：", start_time)
    execute_data(sql_query)
    send_weekly_report_email(seven_days_ago, yesterday)
    end_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    print("程序结束时间：", end_time)

