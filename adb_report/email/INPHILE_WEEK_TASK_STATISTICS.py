# import sys
# sys.path.append(r"E:\pycharm\database")
import os
from database import DatabaseConnection
from datetime import datetime, date, timedelta
from EmailSender_New import EmailSender_New

# 获取当前时间
today = date.today()
# 计算7天前的日期
formatted_date = today.strftime("%Y%m%d")
seven_days_ago = today - timedelta(days=7)
# 计算昨天的日期
yesterday = today - timedelta(days=1)



sql_query_1 = f"""
  -- @description: 因朔桔录入系统--电票手动--月度任务统计数据
  -- @param:
  -- @return:
  -- @alter:
  -- @time: 2024-11-01 15:01:06
  -- @author: 01
  -- @version: 1.0.0
SELECT
    t.handle_user_id AS "处理人",
    t.case_id AS "案件号",
    COUNT(1) AS "发票数"
FROM
    inphile_ods.ele_invoice_qrcode_result t
WHERE
    substr(t.create_time,1,10) >= '{seven_days_ago}'
     AND substr(t.create_time,1,10) <= '{yesterday}'
    AND t.deal_type = '3'
    AND t.deleted = '0'
    AND t.handle_user_id IS NOT NULL
GROUP BY
    t.handle_user_id,
    t.case_id;
"""


sql_query_2 = f"""
  -- @description: 因朔桔录入系统---特殊件---月度任务统计数据
  -- @param:
  -- @return:
  -- @alter:
  -- @time: 2024-11-01 15:01:06
  -- @author: 01
  -- @version: 1.0.0
SELECT
    at.team_name AS "组别",
    au.id AS "工号",
    au.user_name AS "姓名",
    cl.case_id AS "案件号",
    '特殊件' AS "环节",
    cl.create_time AS "提交时间"
FROM
    inphile_ods.case_log cl
INNER JOIN inphile_ods.auth_user au ON au.id = cl.create_by
INNER JOIN inphile_ods.auth_team at ON at.id = au.team_id
WHERE
    cl.node = '特殊件'
    AND cl.handle = '特殊件录入'
    AND cl.remark = '特殊件录入提交成功'
    AND substr(cl.create_time,1,10) >= '{seven_days_ago}'
    AND substr(cl.create_time,1,10) <= '{yesterday}'
    AND cl.deleted = '0'
GROUP BY
    at.team_name, au.id, au.user_name, cl.case_id, cl.node, cl.create_time;
"""

sql_query_3 = f"""
  -- @description: 因朔桔录入系统----预处理---月度任务统计数据
  -- @param:
  -- @return:
  -- @alter:
  -- @time: 2024-11-01 15:01:06
  -- @author: 01
  -- @version: 1.0.0
SELECT
    at.team_name AS "组别",
    au.id AS "工号",
    au.user_name AS "姓名",
    t.case_id AS "案件号",
    COUNT(1) AS "影像张数",
    t.create_time AS "提交时间"
FROM
    inphile_ods.case_log t
LEFT JOIN inphile_ods.case_image ci ON ci.case_id = t.case_id
LEFT JOIN inphile_ods.auth_user au ON au.id = t.create_by
LEFT JOIN inphile_ods.auth_team at ON at.id = au.team_id
WHERE
    t.node = '预处理'
    AND t.handle = '预处理'
    AND substr(t.create_time,1,10) >= '{seven_days_ago}'
    AND substr(t.create_time,1,10) <= '{yesterday}'
    AND t.deleted = '0'
GROUP BY
    at.team_name, au.id, au.user_name, t.case_id, t.create_time;
"""

sql_query_4 = f"""
  -- @description: 因朔桔录入系统----录入---月度任务统计数据
  -- @param:
  -- @return:
  -- @alter:
  -- @time: 2024-11-01 15:01:06
  -- @author: 01
  -- @version: 1.0.0
SELECT
    b.user_id as 工号,
    c.user_name as 姓名,
    a.id AS 案件号,
    (SELECT COUNT( distinct e.id)
     FROM inphile_ods.case_apply d
     LEFT JOIN inphile_ods.case_bill e ON e.apply_id = d.id
     WHERE d.case_id = a.id) AS 发票总数,
     (SELECT COUNT(e.id)
     FROM inphile_ods.case_apply d
     LEFT JOIN inphile_ods.case_bill e ON e.apply_id = d.id
     WHERE d.case_id = a.id and e.medical_date IS NOT NULL) AS 门诊发票,
     (SELECT COUNT(e.id)
     FROM inphile_ods.case_apply d
     LEFT JOIN inphile_ods.case_bill e ON e.apply_id = d.id
     WHERE d.case_id = a.id and e.out_hospital_date IS NOT NULL) AS 住院发票,
        (select count(1) from  inphile_ods.case_detail cd
    left join inphile_ods.case_bill cb on cd.bill_id = cb.id
    left join inphile_ods.case_apply ca on cb.apply_id=ca.id
    where cd.deleted=0 and ca.case_id = a.id ) AS 住院明细数,
    b.node as 环节,
    b.handle_time as 提交时间
FROM
    inphile_ods.case_info a
LEFT JOIN
    inphile_ods.case_log b ON a.id = b.case_id
LEFT JOIN
    inphile_ods.account c ON b.user_id = c.account
WHERE
    substr(b.create_time,1,10) >= '{seven_days_ago}'
    and substr(b.create_time,1,10) <= '{yesterday}'
    and b.node = '录入'   AND b.handle = '账核诊录入'
and a.deleted = 0
"""


sql_query_5 = f"""
  -- @description: 因朔桔录入系统----复核---月度任务统计数据
  -- @param:
  -- @return:
  -- @alter:
  -- @time: 2024-11-01 15:01:06
  -- @author: 01
  -- @version: 1.0.0
SELECT
    b.user_id as 工号,
    c.user_name as 姓名,
    a.id AS 案件号,
    (SELECT COUNT( distinct e.id)
     FROM inphile_ods.case_apply d
     LEFT JOIN inphile_ods.case_bill e ON e.apply_id = d.id
     WHERE d.case_id = a.id) AS 发票总数,
     (SELECT COUNT(e.id)
     FROM inphile_ods.case_apply d
     LEFT JOIN inphile_ods.case_bill e ON e.apply_id = d.id
     WHERE d.case_id = a.id and e.medical_date IS NOT NULL) AS 门诊发票,
     (SELECT COUNT(e.id)
     FROM inphile_ods.case_apply d
     LEFT JOIN inphile_ods.case_bill e ON e.apply_id = d.id
     WHERE d.case_id = a.id and e.out_hospital_date IS NOT NULL) AS 住院发票,
         (SELECT COUNT(distinct f.id)
     FROM inphile_ods.case_apply d
     LEFT JOIN inphile_ods.case_bill e ON e.apply_id = d.id
     left join inphile_ods.case_detail f on e.image_id = f.image_id
     WHERE d.case_id = a.id and e.out_hospital_date IS NOT NULL) AS 住院明细数,
    b.node as 环节,
    b.handle_time as 提交时间
FROM
    inphile_ods.case_info a
LEFT JOIN
    inphile_ods.case_log b ON a.id = b.case_id
LEFT JOIN
    inphile_ods.account c ON b.user_id = c.account
WHERE
    substr(b.create_time,1,10) >= '{seven_days_ago}'
    and  substr(b.create_time,1,10) <= '{yesterday}'
    and  b.node IN ('复核', '质检')
    AND b.handle IN ('复核录入', '质检录入')
and a.deleted = 0
"""


sql_query_6 = f"""
  -- @description: 因朔桔录入系统---手动回传------月度任务统计数据
  -- @param:
  -- @return:
  -- @alter:
  -- @time: 2024-11-01 15:01:06
  -- @author: 01
  -- @version: 1.0.0
SELECT
    at.team_name AS "组别",
    au.id AS "工号",
    au.user_name AS "姓名",
    cl.case_id AS "案件号",
    '手动回传' AS "环节",
    cl.create_time AS "提交时间"
FROM
    inphile_ods.case_log cl
INNER JOIN inphile_ods.auth_user au ON au.id = cl.create_by
INNER JOIN inphile_ods.auth_team at ON at.id = au.team_id
WHERE
    cl.node = '回传'
    AND cl.handle = '回传'
    AND cl.remark = '手动回传:成功'
        AND substr(cl.create_time,1,10) >= '{seven_days_ago}'
    AND substr(cl.create_time,1,10) <= '{yesterday}'
    AND cl.deleted = '0'
GROUP BY
    at.team_name, au.id, au.user_name, cl.case_id, cl.node, cl.create_time;
"""








def send_weekly_report_email(formatted_date,excel_filename1, excel_filename2, excel_filename3, excel_filename4, excel_filename5, excel_filename6):
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
    subject = '因朔桔录入周报数据'
    message = f"各位老师好！\n\n 因朔桔录入任务数据 {formatted_date}数据统计！\n\n"
    message += "具体信息见附件：\n"

    # 获取文件名部分
    if excel_filename2 is None:
        sender.send_email_with_attachment(subject, message, excel_filename1)
    else:
        sender.send_email_with_attachments(subject, message, [excel_filename1, excel_filename2, excel_filename3, excel_filename4, excel_filename5, excel_filename6])


def execute_data(sql_query,excel_filename):
    with DatabaseConnection() as db:
        df = db.execute_query_to_dataframe(sql_query)
        df.to_excel(excel_filename, index=False)




if __name__ == "__main__":
    excel_filename1 = "/opt/adb_report/report_claim/output/" + f"因朔桔录入电票手动{formatted_date}任务量统计.xlsx"
    excel_filename2 = "/opt/adb_report/report_claim/output/" + f"因朔桔录入特殊件{formatted_date}任务量统计.xlsx"
    excel_filename3 = "/opt/adb_report/report_claim/output/" + f"因朔桔录入预处理{formatted_date}任务量统计.xlsx"
    excel_filename4 = "/opt/adb_report/report_claim/output/" + f"因朔桔录入{formatted_date}任务量统计.xlsx"
    excel_filename5 = "/opt/adb_report/report_claim/output/" + f"因朔桔复核{formatted_date}任务量统计.xlsx"
    excel_filename6 = "/opt/adb_report/report_claim/output/" + f"因朔桔手动回传{formatted_date}任务量统计.xlsx"
    start_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    print("程序开始时间：", start_time)
    execute_data(sql_query_1,excel_filename1)
    execute_data(sql_query_2, excel_filename2)
    execute_data(sql_query_3, excel_filename3)
    execute_data(sql_query_4, excel_filename4)
    execute_data(sql_query_5, excel_filename5)
    execute_data(sql_query_6, excel_filename6)
    send_weekly_report_email(formatted_date, excel_filename1, excel_filename2, excel_filename3, excel_filename4, excel_filename5, excel_filename6)


    end_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    print("程序结束时间：", end_time)


