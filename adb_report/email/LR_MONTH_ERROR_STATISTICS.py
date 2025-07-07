# import sys
# sys.path.append(r"E:\pycharm\database")
from database import DatabaseConnection
from datetime import datetime, date, timedelta
from emailSender import EmailSender

# 获取当前时间
current_time = datetime.now()
# 计算上一个月的时间
last_month_time = current_time - timedelta(days=current_time.day)
last_month_time = last_month_time.replace(day=1)
last_month = last_month_time.strftime('%Y-%m')
last_month_formatted = last_month_time.strftime('%Y%m')

sql_query = f"""
  -- @description: 录入商差错统计分析
  -- @param:
  -- @return:
  -- @alter:
  -- @time: 2024-10-21 
  -- @author: 01
  -- @version: 1.0.0
with t as (
select
    pr.insure_company_channel,
    pr.app_no,
    pr.accept_num,
    b.bill_no,
    b.bill_user_name ,
    replace(substr(b.treatment_date,1,10),'-','')  treatment_date,
    b.vip_type,
    inv.is_outpatient_special,
    b.bill_type
from claim_ods.postback_record pr
left join claim_ods.claim c on pr.accept_num = c.acceptance_no and c.delete_flag = '0'
left JOIN claim_ods.bill b on c.id = b.claim_id and b.delete_flag = '0'
left JOIN claim_ods.clm_app_info cai on cai.C_CUSTOM_APP_NO = c.claim_no and cai.C_DEL_FLAG = '0'
LEFT JOIN claim_ods.clm_visit_inv_info inv ON inv.C_CUSTOM_APP_NO = c.claim_no and inv.C_PLY_NO = cai.C_PLY_NO and inv.c_del_flag = '0'  and inv.C_IS_NEED_SHOW = '0' and inv.C_INV_NO=b.bill_no
where substr(pr.back_time,1,7)='{last_month}'
and pr.back_status in ('2','21')
union
select
    fs.insure_company_channel,
    fs.app_no,
    fs.accept_num,
    b.bill_no,
    b.bill_user_name ,
    replace(substr(b.treatment_date,1,10),'-','')  treatment_date,
    b.vip_type,
    inv.is_outpatient_special,
    b.bill_type
from claim_ods.front_seq_record fs
left join claim_ods.claim c on fs.accept_num = c.acceptance_no and c.delete_flag = '0'
left JOIN claim_ods.bill b on c.id = b.claim_id and b.delete_flag = '0'
left JOIN claim_ods.clm_app_info cai on cai.C_CUSTOM_APP_NO = c.claim_no and cai.C_DEL_FLAG = '0'
LEFT JOIN claim_ods.clm_visit_inv_info inv ON inv.C_CUSTOM_APP_NO = c.claim_no and inv.C_PLY_NO = cai.C_PLY_NO and inv.c_del_flag = '0'  and inv.C_IS_NEED_SHOW = '0' and inv.C_INV_NO=b.bill_no
where substr(fs.create_time,1,7)='{last_month}'
and fs.`state` in ('4')
    and (substring(fs.file_num,7,6) = '{last_month_formatted}' or
   (substring(fs.file_num,9,6) = '{last_month_formatted}' AND substring(fs.file_num,7,2) = 'WW'))
 and  cai.CHANNEL_TYPE = 'M'
),
ff as (select third_party_code,
       acceptNum,
       bill_no,
       billName,
       visitTime,
       sepcialNeed,
       outpatientSpecial,
       billType,
       create_time,
       update_time,
       rank() over(partition by acceptNum,bill_no order by update_time desc) as rn
from CLAIM_DWD.DWD_LR_ORIGINAL_INFO)
select
    t.insure_company_channel  渠道号,
    t.app_no 案件号,
    t.bill_no 账单号实际,
    ff.bill_no 账单号录入,
    t.bill_user_name 姓名实际,
    ff.billName 姓名录入,
     case when  t.bill_user_name<>ff.billName then '否' else '是' end    是否一致_1 ,
    t.treatment_date 就诊日期实际,
    ff.visitTime 就诊日期录入,
    case when  t.treatment_date<>ff.visitTime then '否' else '是' end    是否一致_2 ,
    t.vip_type 是否特需实际,
    ff.sepcialNeed 是否特需录入,
    case when  t.vip_type<>ff.sepcialNeed then '否' else '是' end    是否一致_3 ,
    t.is_outpatient_special 是否门特实际,
    ff.outpatientSpecial 是否门特录入,
    case when  t.is_outpatient_special<>ff.outpatientSpecial then '否' else '是' end    是否一致_4 ,
    t.bill_type 账单类型实际,
    ff.billType 账单类型录入,
    case when  t.bill_type<>ff.billType then '否' else '是' end    是否一致_5
from t
left join ff on t.accept_num = ff.acceptNum and t.bill_no=ff.bill_no
where ff.rn=1
and (   t.bill_user_name<>ff.billName or  t.treatment_date<>ff.visitTime or
        t.vip_type<>ff.sepcialNeed or   t.is_outpatient_special<>ff.outpatientSpecial or  t.bill_type<>ff.billType)
and ff.acceptNum is not null
and ff.bill_no is not null

"""

def send_weekly_report_email(last_month_formatted,excel_filename):
    # 配置邮件信息
    email_config = {
        'sender': 'messages-noreply@insuresmart.com.cn',
        'password': 'M&H@&2z9E65q',  # SMTP授权码
        #'recipients': ['cuisl@insuresmart.com.cn'],
        'recipients': ['cuisl@insuresmart.com.cn','chengpan@insuresmart.com.cn', 'liyl@insuresmart.com.cn'],
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
    subject = '录入商差错统计月报数据'
    message = f"各位老师好！\n\n 请查收 录入商差错统计月报数据 {last_month_formatted}   统计 ！\n\n"
    message += "具体信息见附件：\n"  # 这里您可以添加查询结果或更详细的信息


    # 发送带有附件的邮件
    sender.send_email_with_attachment(subject, message, excel_filename)
def execute_data(sql_query,excel_filename):
    with DatabaseConnection() as db:
        df = db.execute_query_to_dataframe(sql_query)
        #print(df.head(3))
        #excel_filename = "E:\\pycharm\\output\\" + f"平安机构进件量_{last_month_formatted}.xlsx"
        #print(excel_filename)
        df.to_excel(excel_filename, index=False)




if __name__ == "__main__":
    excel_filename = "E:\\pycharm\\output\\" + f"录入商差错{last_month_formatted}.xlsx"
    # excel_filename = "/opt/adb_report/report_claim/output/" + f"录入商差错_{last_month_formatted}.xlsx"
    start_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    print("程序开始时间：", start_time)
    execute_data(sql_query,excel_filename)
    send_weekly_report_email(last_month_formatted,excel_filename)
    end_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    print("程序结束时间：", end_time)

