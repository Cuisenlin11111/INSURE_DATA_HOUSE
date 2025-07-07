# import sys
# sys.path.append(r"E:\pycharm\database")
import os
from database import DatabaseConnection
from datetime import datetime, date, timedelta
from EmailSender_New import EmailSender_New
from dateutil.relativedelta import relativedelta

# 获取当前日期
current_time = datetime.now()
last_month_time = current_time - timedelta(days=current_time.day)
last_month_time = last_month_time.replace(day=1)
last_month = last_month_time.strftime('%Y-%m')
last_month_formatted = last_month_time.strftime('%Y%m')


sql_query = f"""
  -- @description: 渤海月度服务费清单----案件层-----数据
  -- @param:
  -- @return:
  -- @alter:
  -- @time: 2024-11-12 15:01:06
  -- @author: 01
  -- @version: 1.0.0

select
c.batch_no 批次号,
c.claim_no 申请号,
t.ACCEPT_NUM 受理编号,
t.case_no 立案号,
cpc.channel_customer_no 出险人雇员编号,
cpc.insured_name 出险人姓名,
cpc.insured_birthday 出险人出生日期,
  case
  when cpc.insured_gender = 'M' then '男'
  when cpc.insured_gender = 'F' then '女'
  end as 出险人性别,
t.T_CRT_TIME 申请日期,
c.chu_xian_time 出险时间,
count(b.id) 账单张数,
c.claim_amount 索赔金额,
cai.N_FINAL_COMPENSATE_AMT 保险公司赔付金额,
case when cai.C_COMPENSATE_RESULT = '1' then '正常赔付'
when cai.C_COMPENSATE_RESULT = '4' then '拒赔'
end as '赔付结论',
REPLACE(REPLACE(REPLACE(cai.C_INTERNAL_CONCLUSION,CHAR(10),''),CHAR(13),''),CHAR(9),'') 赔付结论描述,
t.department_code 部门,
case c.clm_process_status
WHEN 0 then '预审'
WHEN 1 then '匹配'
WHEN 2 then '扣费'
WHEN 3 then '定责'
WHEN 4 then '理算'
when 5 then '风控'
WHEN 6 then '审核'
WHEN 7 then '复核'
WHEN 8 then '内部结案'
WHEN 9 then '保险公司审核'
WHEN 10 THEN '保险中介审核'
when 11 then '撤案'
end as 案件状态,
CASE c.clm_process
    WHEN '00'
    THEN '预审-待处理'
    WHEN '01'
    THEN '预审-处理开始'
    WHEN '02'
    THEN '预审-处理结束'
    WHEN '03'
    THEN '预审-处理失败'
    WHEN '10'
    THEN '匹配-待处理'
    WHEN '11'
    THEN '匹配-处理开始'
    WHEN '12'
    THEN '匹配-处理结束'
    WHEN '13'
    THEN '匹配-处理失败'
    WHEN '20'
    THEN '扣费-待处理'
    WHEN '21'
    THEN '扣费-处理开始'
    WHEN '22'
    THEN '扣费-处理结束'
    WHEN '23'
    THEN '扣费-处理失败'
    WHEN '30'
    THEN '定责-待处理'
    WHEN '31'
    THEN '定责-处理开始'
    WHEN '32'
    THEN '定责-处理结束'
    WHEN '33'
    THEN '定责-处理失败'
    WHEN '40'
    THEN '理算-待处理'
    WHEN '41'
    THEN '理算-处理开始'
    WHEN '42'
    THEN '理算-处理结束'
    WHEN '43'
    THEN '理算-处理失败'
    WHEN '50'
    THEN '风控-待处理'
    WHEN '51'
    THEN '风控-处理开始'
    WHEN '52'
    THEN '风控-处理结束'
    WHEN '53'
    THEN '风控-处理失败'
    WHEN '60'
    THEN '审核-待处理'
    WHEN '61'
    THEN '审核-处理开始'
    WHEN '62'
    THEN '审核-处理结束'
    WHEN '63'
    THEN '审核-处理失败'
    WHEN '70'
    THEN '复核-待处理'
    WHEN '71'
    THEN '复核-处理开始'
    WHEN '72'
    THEN '复核-处理结束'
    WHEN '73'
    THEN '复核-处理失败'
    WHEN '82'
    THEN '内部结案成功'
    WHEN '92'
    THEN '保险公司审核'
    WHEN '101'
    THEN '保险中介待审核'
    WHEN '102'
    THEN '保险中介审核成功'
    WHEN '111'
    THEN '撤案-处理结束'
    ELSE c.clm_process
  END AS '案件子状态',
pr.gmt_modified 结案时间,
cp.policy_no 分单号,
p.C_CUSTOM_PLY_NO 客户团单号,
p.C_DEPT_NAME 投保单位,
  cp.insure_effective_date 保单生效日期,
  cp.insure_expiry_date 保单截止日期
FROM claim_ods.accept_list_record t
INNER JOIN claim_ods.claim c on c.acceptance_no = t.ACCEPT_NUM
left JOIN claim_ods.bill b on c.id = b.claim_id and b.delete_flag = '0'
left JOIN claim_ods.clm_app_info cai on cai.C_CUSTOM_APP_NO = c.claim_no and cai.C_DEL_FLAG = '0'
left JOIN claim_ods.claim_policy cp on cp.policy_no = cai.C_PLY_NO
left JOIN claim_ods.claim_policy_customer cpc on cpc.customer_no = cp.customer_no
left join claim_ods.ply p on cp.group_policy_no = p.C_PLY_NO
left join claim_ods.postback_record pr on c.claim_no = pr.app_no and pr.is_deleted='N'
WHERE
    substr(pr.back_time,1,7)='{last_month}'
and pr.back_status in ('2','21')
and t.INSURE_COMPANY_CHANNEL = 'BH01'
-- and t.department_code = '8612'
and cai.C_DEL_FLAG = '0'
GROUP BY t.ACCEPT_NUM

"""


sql_query_2 = f"""
  -- @description: 渤海月度服务费清单----发票层-----数据
  -- @param:
  -- @return:
  -- @alter:
  -- @time: 2024-11-12 15:01:06
  -- @author: 01
  -- @version: 1.0.0

SELECT
  c.claim_no 申请号,
  inv.serial_no AS '账单序号',
  inv.C_INV_INSURED_NME AS '账单姓名',
  inv.`C_INV_NO` AS '账单号',
  CASE
    WHEN inv.C_DOC_TYP = '1'
    THEN '门诊'
    WHEN inv.C_DOC_TYP = '2'
    THEN '住院'
    WHEN inv.C_DOC_TYP = '3'
    THEN '门诊/住院'
    WHEN inv.C_DOC_TYP = '4'
    THEN '一次性给付'
    ELSE inv.C_DOC_TYP
  END AS '就诊类型',
  inv.T_VISIT_BGN_TM AS '就诊起始日',
  inv.T_VISIT_END_TM AS '就诊终止日',
  inv.C_HOSPITAL_NME AS '医院名称',
  CASE inv.C_HOSPITAL_DEPART
    WHEN '24'
    THEN '内科'
    WHEN '25'
    THEN '外科'
    WHEN '26'
    THEN '肿瘤科'
    WHEN '27'
    THEN '口腔科'
    WHEN '28'
    THEN '特殊口腔科（口腔种植、正畸、修复科室）'
    WHEN '29'
    THEN '中医科'
    WHEN '30'
    THEN '妇科'
    WHEN '31'
    THEN '产科'
    WHEN '32'
    THEN '儿科'
    WHEN '33'
    THEN '康复科'
    WHEN '34'
    THEN '急诊科'
    WHEN '35'
    THEN '整形科'
    WHEN '36'
    THEN '不孕不育计划生育科室'
    WHEN '37'
    THEN '麻醉科'
    WHEN '38'
    THEN '其他科室'
    ELSE inv.C_HOSPITAL_DEPART
  END AS '科室名称',
  REPLACE(REPLACE(REPLACE(inv.C_DIAG_CDE,CHAR(10),''),CHAR(13),''),CHAR(9),'') AS '疾病代码',
  REPLACE(REPLACE(REPLACE(inv.C_DIAG_NME,CHAR(10),''),CHAR(13),''),CHAR(9),'') AS '疾病名称',
  CASE
    WHEN inv.C_BILL_TYP = '1'
    THEN '无医保'
    WHEN inv.C_BILL_TYP = '2'
    THEN '有医保'
    WHEN inv.C_BILL_TYP = '3'
    THEN '结算单'
    ELSE inv.C_BILL_TYP
  END AS '账单类型',
  CASE
    WHEN inv.bill_shape = '1'
    THEN '常规医疗发票'
    WHEN inv.bill_shape = '2'
    THEN '增值税发票'
    WHEN inv.bill_shape = '3'
    THEN '国税地税发票'
    WHEN inv.bill_shape = '4'
    THEN '手写发票'
    WHEN inv.bill_shape = '5'
    THEN '定额发票'
    WHEN inv.bill_shape = '6'
    THEN '电子发票'
    WHEN inv.bill_shape = '7'
    THEN '非有效报销凭证'
    ELSE inv.bill_shape
  END AS '账单形式',
  inv.C_IS_MUST AS '是否特需',
  inv.C_IS_EMERG_TREAT AS '是否急诊',
  inv.IS_HEALTH AS '是否康复',
  inv.N_SUM_AMT AS '账单总金额',
  inv.N_SOCIAL_GIVE_AMOUNT AS '社保支付金额',
  inv.N_EXTRA_PAY_AMOUNT AS '附加支付',
  inv.N_PERSONAL_PAY_AMOUNT AS '个人账户支付',
  inv.N_DEDUCTIBLE AS '起付线',
  inv.N_CATEG_SELFPAY AS '分类自负金额',
  inv.N_SELF_EXPENSE AS '自费金额',
  inv.N_THIRD_PAY_AMT AS '第三方支付金额',
  inv.N_DEDUCT_AMT AS '不合理金额',
  inv.C_PROD_NME AS '产品',
  inv.C_CVRG_NME AS '险种',
  inv.C_RESPONSE_DESC AS '责任',
  CASE
    WHEN inv.C_COMPENSATE_RESULT = '1'
    THEN '正常赔付'
    WHEN inv.C_COMPENSATE_RESULT = '4'
    THEN '拒赔'
    ELSE inv.C_COMPENSATE_RESULT
  END AS '保险公司赔付结论',
  inv.N_FINAL_PAY AS '保险公司赔付金额',
  REPLACE(REPLACE(REPLACE(inv.C_INTERNAL_CONCLUSION,CHAR(10),''),CHAR(13),''),CHAR(9),'') AS '结案结论描述',
  p.C_DEPT_NAME 投保单位,
    p.C_CUSTOM_PLY_NO 客户团单号,
    cp.insure_effective_date 保单生效日期,
  cp.insure_expiry_date 保单截止日期
FROM   claim_ods.claim c
   INNER JOIN claim_ods.clm_app_info cai on cai.C_CUSTOM_APP_NO = c.claim_no
   left   JOIN claim_ods.claim_policy cp on cp.policy_no = cai.C_PLY_NO
  LEFT JOIN claim_ods.clm_visit_inv_info inv ON inv.C_CUSTOM_APP_NO = cai.C_CUSTOM_APP_NO AND inv.C_PLY_NO = cai.C_PLY_NO and inv.C_DEL_FLAG = '0' AND inv.C_IS_NEED_SHOW = '0'
  left join claim_ods.postback_record pr on cai.C_CUSTOM_APP_NO = pr.app_no and pr.is_deleted='N'
  left join claim_ods.accept_list_record t on pr.accept_num = t.accept_num
left join claim_ods.ply p on cp.group_policy_no = p.C_PLY_NO
WHERE 1=1
#  and pr.back_time >= '2024-4-01'
# AND pr.back_time < '2024-07-01'
  and substr(pr.back_time,1,7)='{last_month}'
and pr.back_status in ('2','21')
and cai.INSURANCE_COMPANY = 'BH01'
and cai.C_DEL_FLAG = '0'
-- and t.department_code = '8612'
-- 部门可以区分8611是北京、8612是天津
group by c.claim_no ,inv.c_ply_no,inv.C_INV_NO  
ORDER BY c.claim_no

"""

def send_weekly_report_email(formatted_date, excel_filename1, excel_filename2=None):
    # 配置邮件信息
    email_config = {
        'sender':'messages-noreply@insuresmart.com.cn',
        'password': 'M&H@&2z9E65q',  # SMTP授权码
        # 'recipients': ['cuisl@insuresmart.com.cn'],
        'recipients': ['cuisl@insuresmart.com.cn', 'liyl@insuresmart.com.cn'],
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
    subject = '渤海月度服务费清单数据'
    message = f"各位老师好！\n\n 渤海月度服务费 {formatted_date} 数据统计！\n\n"
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
    excel_filename1 = "/opt/adb_report/report_claim/output/" + f"渤海月度服务费-案件层-{last_month_formatted}.xlsx"
    excel_filename2 = "/opt/adb_report/report_claim/output/" + f"渤海月度服务费-发票层-{last_month_formatted}.xlsx"
    start_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    print("程序开始时间：", start_time)
    execute_data(sql_query,excel_filename1)
    execute_data(sql_query_2, excel_filename2)
    send_weekly_report_email(last_month_formatted, excel_filename1,excel_filename2)
    end_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    print("程序结束时间：", end_time)

