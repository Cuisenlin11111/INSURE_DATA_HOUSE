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
  -- @description: 太保健康--发票层--月度统计数据
  -- @param:
  -- @return:
  -- @alter:
  -- @time: 2024-11-14 15:01:06
  -- @author: 01
  -- @version: 1.0.0
SELECT DISTINCT
    alr.accept_num AS 报案号,
    pr.insurance_claim_no AS 赔案号,
    pr.channel_policy_no AS 保单号,
    date_format(cp.insure_effective_date, '%Y/%m/%d') AS 责任起期,
    date_format(cp.insure_expiry_date, '%Y/%m/%d') AS 责任终期,
    p.C_DEPT_NAME AS 投保人名称,
    c.customer_name AS 出险人姓名,
    c.id_no AS 出险人证件号,
    prdm.third_ins_lv_code AS 人员层级,
    prdm.third_lns_lv_name AS 人员层级计划,
    CASE cpc.insured_gender
        WHEN 'M' THEN '男'
        WHEN 'F' THEN '女'
        ELSE '未知'
    END AS 出险人性别,
    REPLACE(json_extract(cpc.extra, "$.age"), '"', '') AS 年龄,
    REPLACE(json_extract(cpc.extra, "$.occupationTypeName"), '"', '') AS 职业类别,
    IF(cpc.channel_customer_no = cpc.main_channel_customer_no, '主被保险人', '连带被保险人') AS 出险人属性,
    CASE cpc.main_insured_relation
        WHEN '1' THEN '本人'
        WHEN '2' THEN '子女'
        WHEN '3' THEN '配偶'
        WHEN '4' THEN '父母'
        ELSE '其他'
    END AS 被保险人类型,
    cpc2.insured_id_no AS 主被保险人身份号,
    CASE inv.accident_type
        WHEN 'JB' THEN
            CASE WHEN prdm.third_duty_name LIKE '%生育%' THEN '其他' ELSE '疾病' END
        WHEN 'YW' THEN '意外'
    END AS 出险类型,
    GROUP_CONCAT(DISTINCT prdm.third_duty_name) AS 索赔性质,
    date_format(IF(alr.claim_source = '1', alr.T_CRT_TIME, alr.ACCEPT_DATE), '%Y/%m/%d') AS 索赔日期,
    date_format(inv.T_INV_BGN_TM, '%Y/%m/%d') AS 出险日期,
    date_format(pr.sucess_time, '%Y/%m/%d') AS 结案日期,
    date_format(pr.sucess_time, '%m') AS 结案月份,
    inv.C_INV_NO AS 发票号,
    FORMAT(inv.N_SUM_AMT, 2) AS 发票总金额,
    FORMAT(inv.N_FINAL_PAY, 2) AS 结案金额,
    FORMAT(inv.N_FINAL_PAY, 2) AS 给付金额,
    FORMAT(inv.N_SELF_EXPENSE, 2) AS 自费金额,
    FORMAT(inv.N_CATEG_SELFPAY, 2) AS 分类自付,
    FORMAT(inv.N_SOCIAL_GIVE_AMOUNT, 2) AS 医保支付,
    FORMAT(inv.N_THIRD_PAY_AMT, 2) AS 第三方支付,
    FORMAT(inv.N_DEDUCT_AMT, 2) AS 扣减金额,
    IF(c.clm_process = '111', '撤案', '结案') AS 案件状态,
    CASE c.clm_process
        WHEN '111' THEN '不予受理'
        ELSE
            CASE inv.C_COMPENSATE_RESULT
                WHEN '1' THEN '赔付'
                WHEN '4' THEN '拒赔'
                ELSE '未知'
            END
    END AS 理赔结论,
    CASE c.clm_process
        WHEN '111' THEN REPLACE(REPLACE(REPLACE(alr.WITHDRAWAL_REASON, CHAR(10), ''), CHAR(13), ''), CHAR(9), '')
        ELSE
            CASE inv.C_COMPENSATE_RESULT
                WHEN '1' THEN '符合条款约定赔付'
                WHEN '4' THEN REPLACE(REPLACE(REPLACE(inv.C_INTERNAL_CONCLUSION, CHAR(10), ''), CHAR(13), ''), CHAR(9), '')
                ELSE '未知'
            END
    END AS 结论原因,
    prdm.third_risk_name AS 险种,
    GROUP_CONCAT(DISTINCT inv.C_DIAG_NME) AS 疾病名称,
    GROUP_CONCAT(DISTINCT IF(prdm.third_duty_name LIKE '%重大疾病%', '重大疾病', '普通疾病')) AS 疾病分类,
    concat(b.province_name, b.region_name) AS 出险城市,
    inv.C_HOSPITAL_NME AS 医疗机构,
    CASE WHEN max(inv.C_DOC_TYP = '2') THEN '住院'
         ELSE '门诊'
    END AS 票据类型
FROM
    claim_ods.accept_list_record alr
    LEFT JOIN claim_ods.claim c ON c.acceptance_no = alr.ACCEPT_NUM AND c.delete_flag = '0' AND c.insure_company_channel = 'CP08'
    LEFT JOIN claim_ods.clm_app_info cai ON cai.C_CUSTOM_APP_NO = c.claim_no AND cai.C_DEL_FLAG = '0' AND cai.INSURANCE_COMPANY = 'CP08'
    LEFT JOIN claim_ods.`clm_visit_inv_info` inv ON cai.C_CUSTOM_APP_NO = inv.C_CUSTOM_APP_NO AND cai.C_PLY_NO = inv.C_PLY_NO AND inv.C_IS_NEED_SHOW = '0' AND inv.C_DEL_FLAG = '0' AND inv.INSURANCE_COMPANY = 'CP08'
    LEFT JOIN claim_ods.bill b ON c.id = b.claim_id AND b.delete_flag = '0' AND b.bill_no = inv.C_INV_NO
    LEFT JOIN claim_ods.claim_policy cp ON cp.policy_no = cai.C_PLY_NO AND cp.is_deleted = 'N' AND cp.insure_company_channel = 'CP08'
    LEFT JOIN claim_ods.claim_policy_customer cpc ON cp.customer_no = cpc.customer_no AND cpc.insure_company_channel = 'CP08'
    LEFT JOIN claim_ods.claim_policy_customer cpc2 ON cpc.main_customer_no = cpc2.customer_no AND cpc2.insure_company_channel = 'CP08'
    LEFT JOIN claim_ods.ply p ON cp.group_policy_no = p.C_PLY_NO
    LEFT JOIN claim_ods.prod_risk_duty_mapping prdm ON cp.group_policy_no = prdm.policy_no AND cp.grade_code = prdm.ins_lv_code AND inv.C_PROD_NO = prdm.product_code AND inv.C_CVRG_NO = prdm.risk_code AND inv.C_RESPONSE_NO = prdm.duty_code AND prdm.is_deleted = 'N' AND prdm.insure_company_channel = 'CP08'
    LEFT JOIN claim_ods.postback_record pr ON pr.app_no = c.claim_no AND pr.is_deleted = 'N' AND pr.insure_company_channel = 'CP08'
WHERE
    (substr(pr.sucess_time, 1, 4) IN ('2025')
        OR substr(c.cancle_time, 1, 4) IN ('2025'))
    AND alr.INSURE_COMPANY_CHANNEL = 'CP08'
and  substr(pr.sucess_time, 1, 7) <>REPLACE(CURDATE(),1,7)
and (substr(c.cancle_time, 1, 7) <>REPLACE(CURDATE(),1,7)  or COALESCE(c.cancle_time,'')='')
GROUP BY
    报案号, 赔案号, 出险类型, 险种, inv.C_INV_NO;
"""


import os

def send_weekly_report_email(formatted_date, excel_filename1 ,excel_filename2=None):
    # 配置邮件信息
    email_config = {
       'sender':'messages-noreply@insuresmart.com.cn',
        'password': 'M&H@&2z9E65q',  # SMTP授权码
        # 'recipients': ['cuisl@insuresmart.com.cn'],
        'recipients': ['cuisl@insuresmart.com.cn', 'fanxue@insuresmart.com.cn'],
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
    subject = '太保健康--发票层--月报数据'
    message = f"各位老师好！\n\n 太保健康发票层 {formatted_date}数据统计！\n\n"
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
    excel_filename1 = "/opt/adb_report/report_claim/output/" + f"太保健康-发票层—{last_month_formatted}.xlsx"
    start_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    print("程序开始时间：", start_time)
    execute_data(sql_query_1,excel_filename1)
    send_weekly_report_email(last_month_formatted, excel_filename1)
    end_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    print("程序结束时间：", end_time)

