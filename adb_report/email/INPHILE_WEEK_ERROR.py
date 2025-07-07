# import sys
# sys.path.append(r"E:\pycharm\database")
import os
from database import DatabaseConnection
from datetime import datetime, date, timedelta
from EmailSender_New import EmailSender_New

# 获取当前时间
# 获取当前时间
today = date.today()
# 计算7天前的日期
formatted_date = today.strftime("%Y%m%d")
seven_days_ago = today - timedelta(days=7)
# 计算昨天的日期
yesterday = today - timedelta(days=1)



sql_query_1 = f"""
  -- @description: 因朔桔录入系统--差错统计--周度任务统计数据
  -- @param:
  -- @return:
  -- @alter:
  -- @time: 2024-11-25 15:01:06
  -- @author: 01
  -- @version: 1.0.0
SELECT
    claim_no AS 受理编号,
    back_time AS 回传时间,
    bill_no AS 发票号,
    lr_name AS 录入姓名,
    fh_name AS 复核姓名,
    bill_user_name_actual AS 发票姓名实际值,
    bill_payer_entered AS 发票姓名录入值,
    treatment_date_actual AS 就诊日期实际值,
    treatment_date_entered AS 就诊日期录入值,
    hospital_name_actual AS 医院名称实际值,
    hospital_name_entered AS 医院名称录入值,
    bill_amount_actual AS 发票金额实际值,
    total_amount_entered AS 发票金额录入值,
    in_hospital_date_actual AS 入院日期实际值,
    in_hospital_date_entered AS 入院日期录入值,
    out_hospital_date_actual AS 出院日期实际值,
    out_hospital_date_entered AS 出院日期录入值,
    bill_type_actual AS 账单类型实际值,
    bill_type_entered AS 账单类型录入值,
    bill_shape_actual AS 账单形式实际值,
    bill_form_entered AS 账单形式录入值,
    vip_type_actual AS 是否特需实际值,
    special_entered AS 是否特需录入值,
    mergency_type_actual AS 是否急诊实际值,
    emergency_treatment_entered AS 是否急诊录入值,
    health_type_actual AS 是否康复实际值,
    recovery_entered AS 是否康复录入值,
    social_pay_amount_actual AS 社保支付金额实际值,
    social_security_payment_entered AS 社保支付金额录入值,
    personal_pay_amount_actual AS 个人账户支付实际值,
    personal_account_payment_entered AS 个人账户支付录入值,
    deductible_actual AS 起付线实际值,
    start_pay_line_entered AS 起付线录入值,
    class_pay_amount_actual AS 分类自负金额实际值,
    classified_self_payment_entered AS 分类自负金额录入值,
    own_pay_amount_actual AS 自费金额实际值,
    own_payment_entered AS 自费金额录入值,
    extra_pay_amount_actual AS 附加支付金额实际值,
    additional_payment_entered AS 附加支付金额录入值,
    third_party_pay_amount_actual AS 第三方支付金额实际值,
    third_pay_amount_entered AS 第三方支付金额录入值,
    remark_actual AS 账单备注实际值,
    remark_entered AS 账单备注录入值,
    hospital_dept_actual AS 科室实际值,
    department_code_entered AS 科室录入值,
    diagnosis1_actual AS 诊断1实际值,
    diagnosis1_entered AS 诊断1录入值,
    diagnosis2_actual AS 诊断2实际值,
    diagnosis2_entered AS 诊断2录入值,
    diagnosis3_actual AS 诊断3实际值,
    diagnosis3_entered AS 诊断3录入值
FROM CLAIM_DWD.DWD_YSJ_LR_ERROR_INFO  where  
data_dt>='{seven_days_ago}'
and data_dt<='{yesterday}'
"""










def send_weekly_report_email(formatted_date,excel_filename1,excel_filename2=None ):
    # 配置邮件信息
    email_config = {
       'sender':'messages-noreply@insuresmart.com.cn',
        'password': 'M&H@&2z9E65q',  # SMTP授权码
       # 'recipients': ['cuisl@insuresmart.com.cn'],
        'recipients': ['cuisl@insuresmart.com.cn', 'liyl@insuresmart.com.cn'],
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
    subject = '因朔桔录入差错统计周报数据'
    message = f"各位老师好！\n\n 因朔桔录入差错案件 {formatted_date}数据统计！\n\n"
    message += "具体信息见附件：\n"

    # 获取文件名部分
    if excel_filename2 is None:
        sender.send_email_with_attachment(subject, message, excel_filename1)
    else:
        sender.send_email_with_attachments(subject, message, [excel_filename1, excel_filename2 ])


def execute_data(sql_query,excel_filename):
    with DatabaseConnection() as db:
        df = db.execute_query_to_dataframe(sql_query)
        df.to_excel(excel_filename, index=False)




if __name__ == "__main__":
    excel_filename1 = "/opt/adb_report/report_claim/output/" + f"因朔桔录入差错{formatted_date}案件统计.xlsx"
    start_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    print("程序开始时间：", start_time)
    execute_data(sql_query_1,excel_filename1)
    send_weekly_report_email(formatted_date, excel_filename1 )
    end_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    print("程序结束时间：", end_time)

