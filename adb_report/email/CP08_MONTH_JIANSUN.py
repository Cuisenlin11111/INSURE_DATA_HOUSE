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

current_time = datetime.now()
last_month_time = current_time - timedelta(days=current_time.day)
last_month_time = last_month_time.replace(day=1)
last_month = last_month_time.strftime('%Y-%m')

# 转义 SQL 查询中的 % 符号
sql_query_1 = f"""
  -- @description: 太保健康----月度任务统计数据
  -- @param:
  -- @return:
  -- @alter:
  -- @time: 2024-11-25 15:01:06
  -- @author: 01
  -- @version: 1.0.0

SELECT DISTINCT
alr.accept_num 受理编号,
p.C_PLY_NAME 团单名称,
pr.channel_policy_no 保单号,
p.C_DEPT_NAME 投保人姓名,
c.customer_name 出险人姓名,
c.id_no 出险人证件号,
if(c.clm_process = '111','撤案','结案') 案件状态,
SUBSTR(if(alr.claim_source = '1',alr.T_CRT_TIME,alr.ACCEPT_DATE),1,10) 案件受理时间,
SUBSTR(inv.T_INV_BGN_TM,1,10) 案件出险日期,
SUBSTR(pr.sucess_time,1,10) 案件结案日期,
alr.department_code 所属分公司,
FORMAT(cai.N_INVOICE_SUM,2) 案件总金额,
FORMAT(cai.N_SOCIAL_GIVE_AMOUNT,2)  案件统筹金额,
FORMAT(sum(inv.N_DEDUCT_AMT - inv.NO_QUANTUM_MERUIT_USE),2) 案件扣减金额,
format(sum(inv.NO_QUANTUM_MERUIT_USE),2) 案件超药量扣减金额,
format(cai.N_DEDUCTLE_AMT,2) 案件免赔额扣减,
FORMAT(cai.C_CATEG_SELFPAY,2) 案件乙类金额,
FORMAT(inv.C_SELF_EXPENSE,2) 案件自费金额,
FORMAT(cai.N_FINAL_COMPENSATE_AMT,2) 案件赔付金额,
case c.clm_process when '111' then '不予受理'
else case inv.C_COMPENSATE_RESULT when '1' then '赔付'
when '4' then '拒赔' else '未知' end
end 理赔结论,
case c.clm_process when '111' then REPLACE(REPLACE(REPLACE(alr.WITHDRAWAL_REASON,CHAR(10),''),CHAR(13),''),CHAR(9),'')
else case cai.C_COMPENSATE_RESULT when '1' then '符合条款约定赔付'
when '4' then REPLACE(REPLACE(REPLACE(cai.C_INTERNAL_CONCLUSION,CHAR(10),''),CHAR(13),''),CHAR(9),'')  else '未知' end
end 案件内部结论描述,
(select group_concat(REPLACE(REPLACE(REPLACE(ao.audit_opinion_desc,CHAR(10),''),CHAR(13),''),CHAR(9),'')) from audit_opinion ao where ao.app_no = cai.C_CUSTOM_APP_NO and ao.ply_part_no = cai.C_PLY_NO and ao.is_deleted = 'N' group by ao.app_no) 人工审核意见,
group_concat(distinct if(inv.EXPLAIN_DESC is null,'',inv.EXPLAIN_DESC)) 拒赔分类,
prdm.third_risk_name 险种,alr.service_line 业务线

FROM claim_prd.accept_list_record alr
LEFT JOIN claim_prd.claim c on c.acceptance_no = alr.ACCEPT_NUM and c.delete_flag = '0' and c.insure_company_channel = 'CP08'
LEFT JOIN claim_prd.clm_app_info cai on cai.C_CUSTOM_APP_NO = c.claim_no and cai.C_DEL_FLAG = '0' and cai.INSURANCE_COMPANY = 'CP08'
left join claim_prd.`clm_visit_inv_info` inv on cai.C_CUSTOM_APP_NO = inv.C_CUSTOM_APP_NO and cai.C_PLY_NO = inv.C_PLY_NO AND inv.C_IS_NEED_SHOW = '0' and inv.C_DEL_FLAG = '0' and inv.INSURANCE_COMPANY = 'CP08'
LEFT JOIN claim_prd.claim_policy cp on cp.policy_no = cai.C_PLY_NO and cp.is_deleted = 'N' and cp.insure_company_channel = 'CP08'
left join claim_prd.claim_policy_customer cpc on cp.customer_no = cpc.customer_no and cpc.insure_company_channel = 'CP08'
left join claim_prd.ply p on cp.group_policy_no = p.C_PLY_NO
left join claim_prd.prod_risk_duty_mapping prdm on cp.group_policy_no = prdm.policy_no and cp.grade_code = prdm.ins_lv_code and inv.C_PROD_NO= prdm.product_code and inv.C_CVRG_NO = prdm.risk_code and inv.C_RESPONSE_NO = prdm.duty_code and prdm.is_deleted = 'N' and prdm.insure_company_channel = 'CP08'
LEFT JOIN claim_prd.postback_record pr on pr.app_no = c.claim_no and pr.is_deleted = 'N' and pr.insure_company_channel = 'CP08'

WHERE
 alr.INSURE_COMPANY_CHANNEL = 'CP08'
and  substr( pr.sucess_time,1,7) = '{last_month}'
group by 受理编号

"""


def send_weekly_report_email(formatted_date, excel_filename1, excel_filename2=None):
    # 配置邮件信息
    email_config = {
        'sender':'messages-noreply@insuresmart.com.cn',
        'password': 'M&H@&2z9E65q',  # SMTP授权码
          # 'recipients': ['cuisl@insuresmart.com.cn'],
         'recipients': ['cuisl@insuresmart.com.cn','fanxue@insuresmart.com.cn'],
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
    subject = '太保健康减损月报数据'
    message = f"各位老师好！\n\n 太保健康减损 {formatted_date}数据统计！\n\n"
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
    excel_filename1 = "/opt/adb_report/report_claim/output/" + f"太保健康减损{last_month}.xlsx"
    start_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    print("程序开始时间：", start_time)
    execute_data(sql_query_1, excel_filename1)
    if os.path.exists(excel_filename1):
        send_weekly_report_email(last_month, excel_filename1)
    else:
        print("Excel 文件未生成，无法发送邮件。")
    end_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    print("程序结束时间：", end_time)