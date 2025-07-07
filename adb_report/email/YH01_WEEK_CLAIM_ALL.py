import os
import pandas as pd
from sqlalchemy import create_engine
from datetime import datetime, date, timedelta
from EmailSender_New import EmailSender_New
from dateutil.relativedelta import relativedelta

# 数据库连接信息
host = 'rr-uf636kzrob17osr7j.mysql.rds.aliyuncs.com'
port = 3306
user = 'prd_readonly'
password = '7MmY^nEJ3fQhysj=B'
db_name = 'claim_prd'

# 获取当前日期
today = date.today()
# 计算7天前的日期
formatted_date = today.strftime("%Y%m%d")

# 计算昨天的日期
yesterday = today - timedelta(days=1)

# 转义 SQL 查询中的 % 符号
sql_query_1 = f"""
  -- @description: 湖南医惠保--周度任务统计数据
  -- @param:
  -- @return:
  -- @alter:
  -- @time: 2024-11-25 15:01:06
  -- @author: 01
  -- @version: 1.0.0

SELECT
    accept_num AS '案件号',
    case_no AS '结算编号',
    apply_claim_mode AS '案件类型',
    case_type_name AS '案件类型名称',
    danger_name AS '出险人',
    danger_id_type AS '出险人证件类型',
    danger_id_no AS '出险人证件号',
    age AS '年龄',
    gender AS '性别',
    begin_date AS '开始时间',
    end_date AS '结束时间',
    hospital_name AS '医院名称',
    diagnosis_name AS '诊断名称',
    medical_type AS '医疗类别',
    insurance_type AS '险种',
    total_fee AS '总费用',
    full_self_pay AS '全自费',
    class_b_self_pay AS '乙类先自付',
    over_limit_self_pay AS '超限额自付',
    pooling_fund_pay AS '统筹基金自付',
    civil_servant_subsidy AS '公务员补助',
    large_amount_fund_pay AS '大额基金支付',
    critical_illness_insurance_pay AS '大病保险支付',
    medical_assistance_pay AS '医疗救助支付',
    other_fund_pay AS '其他基金支付',
    one_stop_pooling_inside_pay AS '一站式统筹内赔付',
    one_stop_pooling_outside_pay AS '一站式统筹外赔付',
    one_stop_pooling_inside_deductible AS '一站式统筹内起付线',
    one_stop_pooling_outside_deductible AS '一站式统筹外起付线',
    one_stop_final_compensation AS '一站式最终赔付金额',
    insured_area AS '参保地区',
    pooling_inside_deductible AS '统筹内起付线',
    pooling_inside_compensation AS '统筹内赔付',
    pooling_outside_deductible AS '统筹外起付线',
    pooling_outside_compensation AS '统筹外赔付',
    case_status AS '案件状态',
    visit_number AS '就诊号',
    payment_status AS '支付状态',
    is_hang_up AS '是否挂起',
    back_time AS '回传时间',
    insurance_company_back_amount AS '保司回传金额',
    final_pay AS '最终赔付',
    one_step_ind AS '一站式标识',
    zcw_pay AS '政策内金额',
    zcn_pay AS '政策外金额',
    juming_ind AS '城镇居民标识',
    pay_time AS '支付时间'
FROM
    claim_dwd.DWD_YH01_MEDICAL_CLAIM_DATA;

"""


def send_weekly_report_email(formatted_date, excel_filename1, excel_filename2=None):
    # 配置邮件信息
    email_config = {
        'sender': 'messages-noreply@insuresmart.com.cn',
        'password': 'M&H@&2z9E65q',  # SMTP授权码
         # 'recipients': ['cuisl@insuresmart.com.cn'],
        'recipients': ['cuisl@insuresmart.com.cn', 'xuzy@insuresmart.com.cn', 'lixiaodan18@hun.picc.com.cn',
                       'chengong06@hun.picc.com.cn'],
        'smtp_server': 'smtp.exmail.qq.com',
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
    subject = '医惠保案件全量数据'
    message = f"各位老师好！\n\n 医惠保全量案件 {formatted_date}数据统计！\n\n"
    message += "具体信息见附件：\n"

    # 获取文件名部分
    if excel_filename2 is None:
        sender.send_email_with_attachment(subject, message, excel_filename1)
    else:
        sender.send_email_with_attachments(subject, message, [excel_filename1, excel_filename2])


def execute_data(sql_query, excel_filename):
    try:
        # 创建数据库引擎
        engine = create_engine(f'mysql+pymysql://{user}:{password}@{host}:{port}/{db_name}')
        df = pd.read_sql(sql_query, engine)
        df.to_excel(excel_filename, index=False)
    except Exception as err:
        print(f"数据库连接或查询出错: {err}")
    finally:
        if 'engine' in locals():
            engine.dispose()


if __name__ == "__main__":
    excel_filename1 = "/opt/adb_report/report_claim/output/" + f"医惠保全量周报{formatted_date}.xlsx"
    start_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    print("程序开始时间：", start_time)
    execute_data(sql_query_1, excel_filename1)
    if os.path.exists(excel_filename1):
        send_weekly_report_email(formatted_date, excel_filename1)
    else:
        print("Excel 文件未生成，无法发送邮件。")
    end_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    print("程序结束时间：", end_time)