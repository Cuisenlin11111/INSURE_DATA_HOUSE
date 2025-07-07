# import sys
#
# sys.path.append(r"E:\pycharm\database")
from database import DatabaseConnection
from datetime import datetime
from datetime import datetime, timedelta

# 获取当前日期
now = datetime.now()

# 获取当前月的第一天
first_day_of_current_month = now.replace(day=1)

# 从当前月的第一天减去一天，就会得到上个月的最后一天
last_day_of_previous_month = first_day_of_current_month - timedelta(days=1)

# 格式化日期为 YYYY-MM 格式
previous_month = last_day_of_previous_month.strftime('%Y-%m')

sql_query = f"""
  -- @description: 泰康广分结案月表
  -- @param:
  -- @return:
  -- @alter:
  -- @time: 2024-06-20 15:01:06
  -- @author: 01
  -- @version: 1.0.0
  
INSERT INTO `CLAIM_DWD`.`DWD_TK07_CLOSE_SUMMARY_M`
select pr.insurance_claim_no insurance_claim_no,
p.C_DEPT_NAME C_DEPT_NAME,
cp.channel_group_policy_no channel_group_policy_no,
case when alr.claim_source = '1' then '线下'
 WHEN alr.claim_source = '2' THEN '线上'
 WHEN alr.claim_source = '3' THEN '线上转线下'
 WHEN alr.claim_source = '4' THEN '线下转线上'
 END as claim_source,
pr.ACCEPT_NUM ACCEPT_NUM,
cpc.insured_name insured_name,
cpc.insured_id_no insured_id_no,
count(distinct inv.C_INV_NO) C_INV_NO,
case when pr.postback_way = 'H' and INSTR(pr.insurance_claim_no,',') > 0 then '半流程多案件'
when pr.postback_way = 'H' then '半流程单案件'
when pr.postback_way = 'W' then '全流程'
else '未知' end postback_way,
alr.accept_batch_no,
alr.ACCEPT_DATE 受理时间,
pr.back_time 回传时间
,
case
when qc.gmt_created IS NOT NUll then '是'
else null END as 是否问题件,
'' 
from claim_ods.postback_record pr
inner join claim_dim.temp_0715 t on pr.insurance_claim_no = t.report_no
LEFT JOIN claim_ods.apply_claim ac on pr.app_no = ac.apply_no and ac.delete_flag = '0'
LEFT JOIN claim_ods.claim_policy cp on cp.policy_no = ac.policy_part_no
LEFT JOIN claim_ods.claim_policy_customer cpc on cpc.customer_no = cp.customer_no
left join claim_ods.ply p on cp.group_policy_no = p.C_PLY_NO
left join claim_ods.`clm_visit_inv_info` inv on pr.app_no = inv.C_CUSTOM_APP_NO  and inv.C_DEL_FLAG = '0'   and inv.C_IS_NEED_SHOW='0'
left join claim_ods.`accept_list_record` alr on pr.accept_num = alr.accept_num
LEFT JOIN  claim_ods.question_claim qc on qc.claim_no = pr.app_no
WHERE pr.INSURE_COMPANY_CHANNEL = 'TK07'
  and cp.channel_group_policy_no not in ('2853017521816','2853017514284','2853017405982','2853017440422','2853017337887','2853017337885','2853017337886','2853017326791','2853017326792','2853017326788','2853018603519','2853018634607','2853018703876')

and pr.is_deleted = 'N'
group by pr.insurance_claim_no
union all


select distinct
pr.insurance_case_no 赔案号,
p.C_DEPT_NAME 投保单位,
cp.channel_group_policy_no 保单号,
case when alr.claim_source = '1' then '线下'
 WHEN alr.claim_source = '2' THEN '线上'
 WHEN alr.claim_source = '3' THEN '线上转线下'
 WHEN alr.claim_source = '4' THEN '线下转线上'
 END as '线上/线下',
pr.ACCEPT_NUM 受理编号,
cpc.insured_name 姓名,
cpc.insured_id_no 证件号码,
count(distinct inv.C_INV_NO) 账单数量,
case when pr1.postback_way = 'H' and INSTR(pr1.insurance_claim_no,',') > 0 then '半流程多案件'
when pr1.postback_way = 'H' then '半流程单案件'
when pr1.postback_way = 'W' then '全流程'
else '未知' end '回传方式',
alr.accept_batch_no 受理批次号,
alr.ACCEPT_DATE 受理时间,
# pr.back_time  回传时间,
case when pr.back_time is null then pr.sucess_time else pr.back_time end 回传时间,
case
when qc.gmt_created IS NOT NUll then '是'
else null END as 是否问题件,
''
FROM claim_ods.`insurance_company_case` pr
    inner join claim_dim.temp_0715 t on pr.insurance_case_no = t.report_no
-- LEFT JOIN apply_claim ac on pr.app_no = ac.apply_no and pr.policy_no = ac.policy_part_no and ac.delete_flag = '0'
LEFT JOIN claim_ods.claim_policy cp on cp.policy_no = pr.policy_no
LEFT JOIN claim_ods.claim_policy_customer cpc on cpc.customer_no = cp.customer_no
left join claim_ods.ply p on cp.group_policy_no = p.C_PLY_NO
left join claim_ods.`clm_visit_inv_info` inv on pr.app_no = inv.C_CUSTOM_APP_NO  and inv.C_DEL_FLAG = '0'  and inv.C_PLY_NO=pr.policy_no  and inv.C_IS_NEED_SHOW='0'
left join claim_ods.`accept_list_record` alr on pr.accept_num = alr.accept_num
left join claim_ods.postback_record pr1 on pr.app_no = pr1.app_no and pr1.is_deleted = 'N'
LEFT JOIN  claim_ods.question_claim qc on qc.claim_no = pr.app_no
WHERE pr.INSURE_COMPANY_CHANNEL = 'TK07'
and pr.is_deleted = 'N'
and cp.channel_group_policy_no  in ('2853017521816','2853017514284','2853017405982','2853017440422','2853017337887','2853017337885','2853017337886','2853017326791','2853017326792','2853017326788','2853018603519','2853018634607','2853018703876')
group by pr.insurance_case_no

"""


# def truncate_table(table_name='CLAIM_DWD.DWD_TK07_CLOSE_SUMMARY_M'):
#     with DatabaseConnection() as conn:
#         truncate_sql = f" TRUNCATE TABLE {table_name};"
#         with conn.cursor() as cursor:
#             cursor.execute(truncate_sql)
#             conn.commit()


def insert_data(sql_query):
    with DatabaseConnection() as conn:
        with conn.cursor() as cursor:
            cursor.execute(sql_query)
            conn.commit()


if __name__ == "__main__":
    start_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    print("程序开始时间：", start_time)
    # truncate_table()
    insert_data(sql_query)
    end_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    print("程序结束时间：", end_time)
