# import sys
# sys.path.append(r"E:\pycharm\database")
import os
from database import DatabaseConnection
from datetime import datetime, date, timedelta
from EmailSender_New import EmailSender_New
from dateutil.relativedelta import relativedelta

today = datetime.now()

# 获取当前日期
now = datetime.now()
# 计算上个月的年份和月份
last_month_year = now.year if now.month > 1 else now.year - 1
last_month_month = now.month - 1 if now.month > 1 else 12

# 格式化输出
formatted_date = f"{last_month_year}-{last_month_month:02d}"


sql_query = f"""
  -- @description: 问题件回销---数据
  -- @param:
  -- @return:
  -- @alter:
  -- @time: 2024-11-12 15:01:06
  -- @author: 01
  -- @version: 1.0.0

SELECT
    DISTINCT
    c.claim_no 申请号,
    c.acceptance_no 受理编号,
    cpc.insured_name 出险人姓名,
    cpc.insured_id_no 证件号码,
    CASE
        WHEN cpc.insured_gender = 'M' THEN '男'
        WHEN cpc.insured_gender = 'F' THEN '女'
    END AS 出险人性别,
    cpc.insured_birthday 出险人出生日期,
    CASE
        WHEN cpc.main_insured_relation = '1' THEN '本人'
        WHEN cpc.main_insured_relation = '2' THEN '子女'
        WHEN cpc.main_insured_relation = '3' THEN '配偶'
    END AS 出险人与主被保险人关系,
    cai.APPLY_TM 申请时间,
    cai.T_ACCIDENT_TM 出险时间,
    c.bill_num 账单张数,
    c.claim_amount 索赔金额,
    cai.N_FINAL_COMPENSATE_AMT 案件赔付金额,
    CASE
        WHEN cai.C_COMPENSATE_RESULT = '4' THEN '拒赔'
        WHEN cai.C_COMPENSATE_RESULT = '1' THEN '正常赔付'
    END AS 赔付结论,
    REPLACE(REPLACE(REPLACE(cai.C_INTERNAL_CONCLUSION, CHAR(10), ''), CHAR(13), ''), CHAR(9), '') 赔付结论描述,
    DATE_FORMAT(REPLACE(REPLACE(alr.ACCEPT_DATE, 'AM', ''), 'PM', ''), '%Y-%m-%d') 受理日期,
    cp.insure_effective_date 保单生效日期,
    cp.insure_expiry_date 保单截止日期,
    CASE
        WHEN alr.claim_source = '1' THEN '线下'
        WHEN alr.claim_source = '2' THEN '线上'
        WHEN alr.claim_source = '3' THEN '线上转线下'
        WHEN alr.claim_source = '4' THEN '线下转线上'
    END AS 申请来源,
    inv.c_ply_no 分单号,
    pr.sucess_time 结案日期,
     cat.T_CLOSING_CASE_TM 内部结案时间,
    inv.C_INV_NO 发票号,
    case inv.C_DOC_TYP when '1' then '门诊'  when '2' then '住院' else '未知' end 发票类型,
    inv.T_VISIT_BGN_TM 就诊起期,
    inv.T_VISIT_END_TM 就诊止期,
    inv.C_RESPONSE_DESC 责任,
    inv.N_SUM_AMT 总金额,
    inv.N_SOCIAL_GIVE_AMOUNT 医保支付金额,
    inv.N_CATEG_SELFPAY 分类自负金额,
    inv.N_SELF_EXPENSE 自费金额,
    inv.N_THIRD_PAY_AMT 第三方支付金额,
    inv.N_EXTRA_PAY_AMOUNT 附加支付,
    inv.N_FINAL_PAY 发票赔付金额,
    CASE
        WHEN inv.C_COMPENSATE_RESULT = '1' THEN '正常赔付'
        WHEN inv.C_COMPENSATE_RESULT = '4' THEN '拒赔'
    END AS 赔付结果,
    REPLACE(REPLACE(REPLACE(inv.C_INTERNAL_CONCLUSION, CHAR(10), ''), CHAR(13), ''), CHAR(9), '') 结论描述,
    inv.N_DEDUCT_AMT 扣减金额,
    inv.N_DEDUCTLE_AMT 免赔额,
    inv.C_HOSPITAL_NME 医院名称,
    CONCAT(bhl.province_name, bhl.city_name) 医院所属地,
    inv.C_DIAG_CDE 疾病码,
    REPLACE(REPLACE(REPLACE(inv.C_DIAG_NME, CHAR(10), ''), CHAR(13), ''), CHAR(9), '') 疾病名称,
    DATE_FORMAT(cp.insure_effective_date, '%Y') 年份,cp.grade_name 投保层级
FROM
    claim_ods.claim c
    INNER JOIN claim_ods.accept_list_record alr ON alr.ACCEPT_NUM = c.acceptance_no AND alr.DEL_FLAG = '0'
    INNER JOIN claim_ods.clm_app_info cai ON cai.C_CUSTOM_APP_NO = c.claim_no AND cai.c_del_flag = '0'
    INNER JOIN claim_ods.clm_visit_inv_info inv ON inv.C_CUSTOM_APP_NO = cai.C_CUSTOM_APP_NO AND inv.C_PLY_NO = cai.C_PLY_NO AND inv.c_del_flag = '0' AND inv.C_BILL_TYP <> '3'
    INNER JOIN claim_ods.claim_policy cp ON cp.policy_no = cai.C_PLY_NO
    INNER JOIN claim_ods.claim_policy_customer cpc ON cpc.customer_no = cp.customer_no
    LEFT JOIN claim_ods.base_hospital_label_v3 bhl ON inv.C_HOSPITAL_NO = bhl.hospital_code AND  bhl.is_deleted='N' AND  BHL.expire_time='2100-12-31 00:00:00'
    LEFT JOIN claim_ods.sys_dict sd ON inv.C_HOSPITAL_DEPART = sd.C_CDE AND sd.C_PARENT = 'HospitalDepart'
        left join claim_ods.ply p on cp.group_policy_no = p.C_PLY_NO
  left join claim_ods.`case_audit_task` cat on c.claim_no = cat.C_CLAIM_CASE_NO and cat.C_DEL_FLAG = '0' and cat.INSURE_COMPANY_CHANNEL = 'CP08'
LEFT JOIN claim_ods.postback_record pr on pr.app_no = c.claim_no and pr.is_deleted = 'N' and pr.insure_company_channel = 'CP08'
WHERE
    c.delete_flag = '0'
    AND c.insure_company_channel = 'CP08'
    AND c.delete_flag = '0'
  and p.C_DEPT_NAME='携程计算机技术(上海)有限公司'
 and   substr(pr.sucess_time,1,7 ) <= '{formatted_date}'
ORDER BY
    c.claim_no DESC

"""



def send_weekly_report_email(formatted_date, excel_filename1, excel_filename2=None):
    # 配置邮件信息
    email_config = {
        'sender':'messages-noreply@insuresmart.com.cn',
        'password': 'M&H@&2z9E65q',  # SMTP授权码
         # 'recipients': ['cuisl@insuresmart.com.cn'],
        'recipients': ['cuisl@insuresmart.com.cn', 'chensy@insuresmart.com.cn','fanxue@insuresmart.com.cn'],
        'smtp_server':'smtp.exmail.qq.com',
        'smtp_port': 465
    }

    # 创建EmailSender实例
    sender = EmailSender_New(
        email_config['sender'],
        email_config['password'],
        email_config['recipients'],
        email_config['smtp_server'],
        email_config['smtp_port']
    )

    # 构造邮件内容
    subject = '携程月度数据'
    message = f"各位老师好！\n\n 携程月度数据案件 {formatted_date} 统计！\n\n"
    message += "具体信息见附件：\n"

    # 发送邮件，根据是否有第二个附件进行不同处理
    if excel_filename2 is None:
        sender.send_email_with_attachment(subject, message, excel_filename1)
    else:
        sender.send_email_with_attachments(subject, message, [excel_filename1, excel_filename2])
def execute_data(sql_query,excel_filename):
    with DatabaseConnection() as db:
        df = db.execute_query_to_dataframe(sql_query)
        df.to_excel(excel_filename, index=False)




if __name__ == "__main__":
    excel_filename1 = "/opt/adb_report/report_claim/output/" + f"携程月度数据-{formatted_date}.xlsx"
    start_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    print("程序开始时间：", start_time)
    execute_data(sql_query,excel_filename1)
    send_weekly_report_email(formatted_date, excel_filename1)
    end_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    print("程序结束时间：", end_time)

