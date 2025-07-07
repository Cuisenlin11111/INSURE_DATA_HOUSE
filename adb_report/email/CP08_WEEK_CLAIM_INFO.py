# import sys
# sys.path.append(r"E:\pycharm\database")
import os
from database import DatabaseConnection
from datetime import datetime, date, timedelta
from emailSender import EmailSender
from dateutil.relativedelta import relativedelta

# 获取当前日期
today = date.today()
# 计算7天前的日期
formatted_date = today.strftime("%Y%m%d")
seven_days_ago = today - timedelta(days=7)
# 计算昨天的日期
yesterday = today - timedelta(days=1)


sql_query = f"""
  -- @description: 太保健康周数据
  -- @param:
  -- @return:
  -- @alter:
  -- @time: 2024-09-04 15:01:06
  -- @author: 01
  -- @version: 1.0.0

SELECT
    pr.accept_num AS `赔案号`,
    cai.C_APP_NME AS `被保险人姓名`,
    CASE WHEN insured_id_type = '1' THEN '身份证' ELSE '未知' END AS `证件类型`,
    cai.C_APP_CERT_CDE AS `证件号`,
    '咨询' AS `上送原因分类`,
    qs.generation_reason AS `上送原因`,
    '' AS `初审理赔结论`,
    CASE cai.C_COMPENSATE_RESULT
        WHEN '1' THEN '正常赔付'
        WHEN '4' THEN '拒赔'
        ELSE '未知'
    END AS `终审理赔结论`,
    cai.N_FINAL_COMPENSATE_AMT AS `赔付金额`,
    cai.N_DEDUCT_AMT AS `拒赔金额`,
    CASE qt.conclusion
        WHEN '1' THEN '正常赔付'
        WHEN '2' THEN '通融赔付'
        WHEN '3' THEN '协议赔付'
        WHEN '4' THEN '拒赔'
        WHEN '5' THEN '其他'
    END AS `回复结论`,
    qt.conclusion_reason AS `回复意见`,
    substr(pr.sucess_time,1,10) AS `结案日期`,
    qt.modifier AS `保司复核处理人`
FROM
    claim_ods.postback_record pr
LEFT JOIN claim_ods.clm_app_info cai ON pr.app_no = cai.C_CUSTOM_APP_NO AND cai.C_DEL_FLAG = '0'  and cai.INSURANCE_COMPANY = 'CP08'
LEFT JOIN claim_ods.claim_policy cp ON cai.c_ply_no = cp.policy_no
LEFT JOIN claim_ods.claim_policy_customer cpc ON cp.customer_no = cpc.customer_no
LEFT JOIN claim_ods.quest_type qt ON pr.app_no = qt.claim_no AND qt.is_deleted = 'N'  and qt.INSURE_COMPANY_CHANNEL = 'CP08'
LEFT JOIN claim_ods.`quest_subtype` qs ON qt.id = qs.type_id AND qs.is_deleted = 'N' and qs.INSURE_COMPANY_CHANNEL = 'CP08'
WHERE
    qt.type = '1'
    AND substr(pr.sucess_time,1,10) >= '{seven_days_ago}'
    AND substr(pr.sucess_time,1,10) <= '{yesterday}'
    AND pr.insure_company_channel = 'CP08'
    AND pr.is_deleted = 'N'
GROUP BY
    pr.accept_num;

"""


sql_query_2 = f"""
  -- @description: 太保健康周数据
  -- @param:
  -- @return:
  -- @alter:
  -- @time: 2024-09-04 15:01:06
  -- @author: 01
  -- @version: 1.0.0

SELECT
    pr.accept_num AS 赔案号,
    cai.C_APP_NME AS 被保险人姓名,
    CASE WHEN insured_id_type = '1' THEN '身份证' ELSE '未知' END AS 证件类型,
    cai.C_APP_CERT_CDE AS 证件号,
    CASE trl.rule_code
        WHEN 'IRC000000009' THEN '理赔结论为拒赔且拒赔金额超 2000 元案件'
        WHEN 'IRC000000010' THEN '单笔案件赔付金额超过人民币 30 万元的案件'
        WHEN 'IRC000000011' THEN '医疗险赔付金额超 5 万元'
        WHEN 'IRC000000012' THEN '重疾，身故，伤残'
    END AS 上送原因分类,
    CASE trl.rule_code
        WHEN 'IRC000000009' THEN '理赔结论为拒赔且拒赔金额超 2000 元案件'
        WHEN 'IRC000000010' THEN '单笔案件赔付金额超过人民币 30 万元的案件'
        WHEN 'IRC000000011' THEN '医疗险赔付金额超 5 万元'
        WHEN 'IRC000000012' THEN '重疾，身故，伤残'
    END AS 上送原因,
    '' AS 初审理赔结论,
    CASE cai.C_COMPENSATE_RESULT
        WHEN '1' THEN '正常赔付'
        WHEN '4' THEN '拒赔'
        ELSE '未知'
    END AS 终审理赔结论,
    cai.N_FINAL_COMPENSATE_AMT AS 赔付金额,
    cai.N_DEDUCT_AMT AS 拒赔金额,
    IF(ol.id IS NOT NULL, "不同意", "同意") AS 回复结论,
    GROUP_CONCAT(IF(ol.id IS NOT NULL, REPLACE(JSON_EXTRACT(ol.extra, '$."backCause"'), '"', ''), "")) AS 回复意见,
    DATE_FORMAT(pr.sucess_time, '%Y-%m-%d') AS 结案日期,
    icrr.handler AS 保司复核处理人
FROM (
    SELECT DISTINCT pr.app_no
    FROM claim_ods.postback_record pr
    LEFT JOIN claim_ods.trigger_rules_log trl ON pr.app_no = trl.business_key AND trl.insure_company_channel = 'CP08'
    LEFT JOIN claim_ods.`operation_log` ol ON pr.app_no = ol.busi_no AND ol.busi_type = '2' AND ol.oper_type = '16'
    WHERE     substr(pr.sucess_time,1,10) >= '{seven_days_ago}'
        AND substr(pr.sucess_time,1,10) <= '{yesterday}'
        AND pr.insure_company_channel = 'CP08'
        AND pr.is_deleted = 'N'
        AND trl.rule_code IN ('IRC000000009', 'IRC000000010', 'IRC000000011', 'IRC000000012')
) ff
LEFT JOIN claim_ods.postback_record pr ON ff.app_no = pr.app_no
LEFT JOIN claim_ods.clm_app_info cai ON pr.app_no = cai.C_CUSTOM_APP_NO AND cai.C_DEL_FLAG = '0' AND cai.INSURANCE_COMPANY = 'CP08'
LEFT JOIN claim_ods.claim_policy cp ON cai.c_ply_no = cp.policy_no
LEFT JOIN claim_ods.claim_policy_customer cpc ON cp.customer_no = cpc.customer_no
LEFT JOIN claim_ods.`insurance_company_review_record` icrr ON pr.app_no = icrr.app_no AND icrr.is_deleted = 'N' AND icrr.insure_company_channel = 'CP08'
LEFT JOIN claim_ods.`operation_log` ol ON pr.app_no = ol.busi_no AND ol.sub_busi_type = '2' AND ol.oper_type = '16' AND ol.INSURE_COMPANY_CHANNEL = 'CP08'
LEFT JOIN claim_ods.trigger_rules_log trl ON pr.app_no = trl.business_key AND trl.insure_company_channel = 'CP08' AND trl.rule_code IN ('IRC000000009', 'IRC000000010', 'IRC000000011', 'IRC000000012')
WHERE pr.insure_company_channel = 'CP08' AND pr.is_deleted = 'N'
GROUP BY pr.accept_num;

"""

def send_weekly_report_email(formatted_date, excel_filename1, excel_filename2=None):
    # 配置邮件信息
    email_config = {
        'sender':'messages-noreply@insuresmart.com.cn',
        'password': 'M&H@&2z9E65q',  # SMTP授权码
        #'recipients': ['cuisl@insuresmart.com.cn'],
        'recipients': ['cuisl@insuresmart.com.cn', 'fanxue@insuresmart.com.cn'],
        'smtp_server':'smtp.exmail.qq.com',
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
    subject = '因朔桔审阅平台周报数据'
    message = f"各位老师好！\n\n 请查收太保健康上审阅平台 {formatted_date}数据统计！\n\n"
    message += "具体信息见附件：\n"

    # 发送邮件，根据是否有第二个附件进行不同处理
    if excel_filename2 is None:
        sender.send_email_with_attachment(subject, message, excel_filename1)
    else:
        sender.send_email_with_attachments(subject, message, [excel_filename1, excel_filename2])
def execute_data(sql_query,excel_filename):
    with DatabaseConnection() as db:
        df = db.execute_query_to_dataframe(sql_query)
        #print(df.head(3))
        #excel_filename = "E:\\pycharm\\output\\" + f"平安机构进件量_{formatted_date}.xlsx"
        #print(excel_filename)
        df.to_excel(excel_filename, index=False)




if __name__ == "__main__":
    #excel_filename1 = r"E:\\pycharm\\output\\" + f"太保健康保单特别规定_{formatted_date}.xlsx"
    excel_filename1 = "/opt/adb_report/report_claim/output/" + f"太保健康保单特别规定_{formatted_date}.xlsx"
    #excel_filename2 = r"E:\\pycharm\\output\\" + f"太保健康上上审阅平台清单_{formatted_date}.xlsx"
    excel_filename2 = "/opt/adb_report/report_claim/output/" + f"太保健康上上审阅平台清单_{formatted_date}.xlsx"
    start_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    print("程序开始时间：", start_time)
    execute_data(sql_query,excel_filename1)
    execute_data(sql_query_2, excel_filename2)
    send_weekly_report_email(formatted_date, excel_filename1,excel_filename2)
    end_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    print("程序结束时间：", end_time)

