# import sys
# sys.path.append(r"E:\pycharm\database")
import os
from database import DatabaseConnection
from datetime import datetime, date, timedelta
from EmailSender_New import EmailSender_New

# 获取当前时间
# 获取当前日期
today = datetime.now()

# 计算昨天的日期
yesterday = today - timedelta(days=1)

# 以 YYYY-MM-DD 格式输出昨天的日期
yesterday_str = yesterday.strftime('%Y-%m-%d')



sql_query_1 = f"""
  -- @description: 农商行不予受理案件清单--案件层
  -- @param:
  -- @return:
  -- @alter:
  -- @time: 2024-11-01 15:01:06
  -- @author: 01
  -- @version: 1.0.0
SELECT
    alr.accept_batch_no  受理批次号,
    pr.insurance_batch_no 保司批次号,
     alr.insure_accept_batch_no      '保司收单批次号',
    alr.ACCEPT_NUM 受理编号,
    PR.insurance_claim_no  保司赔案号,
    cpc.insured_name 出险人姓名,
    cpc.insured_id_no 证件号码,
    CASE
        WHEN c.clm_process_status = 0 THEN '预审'
        WHEN c.clm_process_status = 1 THEN '标准化'
        WHEN c.clm_process_status = 2 THEN '扣费'
        WHEN c.clm_process_status = 3 THEN '定责'
        WHEN c.clm_process_status = 4 THEN '理算'
        WHEN c.clm_process_status = 5 THEN '风控'
        WHEN c.clm_process_status = 6 THEN '审核'
        WHEN c.clm_process_status = 7 THEN '复核'
        WHEN c.clm_process_status = 8 THEN '内部结案'
        WHEN c.clm_process_status = 9 THEN '已回传保司'
        WHEN c.clm_process_status = 10 THEN '已回传中智'
        WHEN c.clm_process_status = 11 THEN '撤案'
    END AS 案件状态,
        case when cai.C_COMPENSATE_RESULT = '4'
  then '拒赔'
  when cai.C_COMPENSATE_RESULT = '1'
  then '正常赔付'
   end as 申请层赔付结论,
    CASE
        WHEN c.cancle_time IS NULL THEN CAST(alr.T_UPD_TIME AS DATE)
        ELSE CAST(c.cancle_time AS DATE)
    END AS 撤案时间,alr.WITHDRAWAL_REASON 撤案原因
FROM
    claim_ods.accept_list_record alr
LEFT JOIN claim_ods.claim c ON c.acceptance_no = alr.ACCEPT_NUM
LEFT JOIN claim_ods.apply_claim ac ON c.claim_no = ac.apply_no
LEFT JOIN claim_ods.claim_policy cp ON cp.policy_no = ac.policy_part_no
LEFT JOIN claim_ods.claim_policy_customer cpc ON cpc.customer_no = cp.customer_no
LEFT JOIN claim_ods.clm_app_info cai ON ac.apply_no = cai.C_CUSTOM_APP_NO AND ac.policy_part_no = cai.c_ply_no AND cai.C_DEL_FLAG = '0' AND cai.INSURANCE_COMPANY LIKE 'TK%'
#LEFT JOIN claim_ods.`clm_visit_inv_info` inv ON cai.C_CUSTOM_APP_NO = inv.C_CUSTOM_APP_NO AND cai.c_ply_no = inv.c_ply_no AND inv.C_DEL_FLAG = '0' AND inv.INSURANCE_COMPANY LIKE 'TK%' AND inv.C_IS_NEED_SHOW = '0'
LEFT JOIN claim_ods.`image_assign_task` iat ON alr.accept_batch_no = iat.accept_batch_no
LEFT JOIN claim_ods.ply p ON cp.group_policy_no = p.C_PLY_NO
left join  claim_ods.postback_record pr  on c.claim_no =pr.app_no and pr.is_deleted='N'
WHERE
    alr.INSURE_COMPANY_CHANNEL = 'TK04'
    AND alr.DEL_FLAG = '0'
    AND cp.channel_group_policy_no = '2851012385707'
    AND (alr.ACCEPT_STATUS = '5' OR c.clm_process_status = '11')
    AND (
        (CASE
            WHEN c.cancle_time IS NULL THEN CAST(alr.T_UPD_TIME AS DATE)
            ELSE CAST(c.cancle_time AS DATE)
        END) = '{yesterday_str}'
    )
GROUP BY
    alr.ACCEPT_NUM
"""


sql_query_2 = f"""
  -- @description: 农商行不予受理案件清单--发票层
  -- @param:
  -- @return:
  -- @alter:
  -- @time: 2024-11-01 15:01:06
  -- @author: 01
  -- @version: 1.0.0
SELECT
    alr.accept_batch_no  受理批次号,
    pr.insurance_batch_no 保司批次号,
     alr.insure_accept_batch_no      '保司收单批次号',
    alr.ACCEPT_NUM 受理编号,
    PR.insurance_claim_no  保司赔案号,
    cpc.insured_name 出险人姓名,
    cpc.insured_id_no 证件号码,
    CASE
        WHEN c.clm_process_status = 0 THEN '预审'
        WHEN c.clm_process_status = 1 THEN '标准化'
        WHEN c.clm_process_status = 2 THEN '扣费'
        WHEN c.clm_process_status = 3 THEN '定责'
        WHEN c.clm_process_status = 4 THEN '理算'
        WHEN c.clm_process_status = 5 THEN '风控'
        WHEN c.clm_process_status = 6 THEN '审核'
        WHEN c.clm_process_status = 7 THEN '复核'
        WHEN c.clm_process_status = 8 THEN '内部结案'
        WHEN c.clm_process_status = 9 THEN '已回传保司'
        WHEN c.clm_process_status = 10 THEN '已回传中智'
        WHEN c.clm_process_status = 11 THEN '撤案'
    END AS 案件状态,
        case when cai.C_COMPENSATE_RESULT = '4'
  then '拒赔'
  when cai.C_COMPENSATE_RESULT = '1'
  then '正常赔付'
   end as 申请层赔付结论,
    CASE
        WHEN c.cancle_time IS NULL THEN CAST(alr.T_UPD_TIME AS DATE)
        ELSE CAST(c.cancle_time AS DATE)
    END AS 撤案时间,b.bill_no 发票号,case when inv.C_COMPENSATE_RESULT = '4' then '拒赔'
WHEN inv.C_COMPENSATE_RESULT = '1' THEN '正常赔付'
end as 发票层赔付结论,
REPLACE(REPLACE(REPLACE(inv.C_INTERNAL_CONCLUSION,CHAR(10),''),CHAR(13),''),CHAR(9),'') 发票层结论描述
FROM
    claim_ods.accept_list_record alr
LEFT JOIN claim_ods.claim c ON c.acceptance_no = alr.ACCEPT_NUM
LEFT JOIN claim_ods.apply_claim ac ON c.claim_no = ac.apply_no
LEFT JOIN claim_ods.claim_policy cp ON cp.policy_no = ac.policy_part_no
LEFT JOIN claim_ods.claim_policy_customer cpc ON cpc.customer_no = cp.customer_no
 left join claim_ods.bill b on c.id = b.claim_id
LEFT JOIN claim_ods.clm_app_info cai ON ac.apply_no = cai.C_CUSTOM_APP_NO AND ac.policy_part_no = cai.c_ply_no AND cai.C_DEL_FLAG = '0' AND cai.INSURANCE_COMPANY LIKE 'TK%'
LEFT JOIN claim_ods.`clm_visit_inv_info` inv ON cai.C_CUSTOM_APP_NO = inv.C_CUSTOM_APP_NO AND cai.c_ply_no = inv.c_ply_no AND inv.C_DEL_FLAG = '0' AND inv.INSURANCE_COMPANY LIKE 'TK%' AND inv.C_IS_NEED_SHOW = '0' and  b.bill_no=inv.C_INV_NO
LEFT JOIN claim_ods.`image_assign_task` iat ON alr.accept_batch_no = iat.accept_batch_no
LEFT JOIN claim_ods.ply p ON cp.group_policy_no = p.C_PLY_NO
left join  claim_ods.postback_record pr  on c.claim_no =pr.app_no and pr.is_deleted='N'
WHERE
    alr.INSURE_COMPANY_CHANNEL = 'TK04'
    AND alr.DEL_FLAG = '0'
    AND cp.channel_group_policy_no = '2851012385707'
    AND (alr.ACCEPT_STATUS = '5' OR c.clm_process_status = '11')
    AND (
        (CASE
            WHEN c.cancle_time IS NULL THEN CAST(alr.T_UPD_TIME AS DATE)
            ELSE CAST(c.cancle_time AS DATE)
        END)  = '{yesterday_str}'
    )
GROUP BY
    alr.ACCEPT_NUM,b.bill_no

"""










def send_weekly_report_email(formatted_date,excel_filename1, excel_filename2):
    # 配置邮件信息
    email_config = {
       'sender':'messages-noreply@insuresmart.com.cn',
        'password': 'M&H@&2z9E65q',  # SMTP授权码
        # 'recipients': ['cuisl@insuresmart.com.cn'],
        'recipients': ['cuisl@insuresmart.com.cn', 'zhuyc10@taikanglife.com','zhangyan9@taikanglife.com','dangrm@insuresmart.com.cn','maomw@insuresmart.com.cn'],
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
    subject = '农商行不予受理案件清单'
    message = f"各位老师好！\n\n 农商行不予受理案件清单 {formatted_date}数据统计！\n\n"
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
    excel_filename1 = "/opt/adb_report/report_claim/output/" + f"农商行不予受理-案件层-{yesterday_str}.xlsx"
    excel_filename2 = "/opt/adb_report/report_claim/output/" + f"农商行不予受理-发票层-{yesterday_str}.xlsx"
    start_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    print("程序开始时间：", start_time)
    execute_data(sql_query_1,excel_filename1)
    execute_data(sql_query_2, excel_filename2)
    send_weekly_report_email(yesterday_str, excel_filename1, excel_filename2 )
    end_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    print("程序结束时间：", end_time)

