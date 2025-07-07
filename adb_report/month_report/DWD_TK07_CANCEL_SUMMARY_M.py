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
  -- @description: 泰康广分撤案月表
  -- @param:
  -- @return:
  -- @alter:
  -- @time: 2024-10-30 15:01:06
  -- @author: 01
  -- @version: 1.0.0

insert into  `CLAIM_DWD`.`DWD_TK07_CANCEL_SUMMARY_M`
SELECT
'泰康广分' 渠道,
case when alr.claim_source = '1' then '线下'
 WHEN alr.claim_source = '2' THEN '线上'
 WHEN alr.claim_source = '3' THEN '线上转线下'
 WHEN alr.claim_source = '4' THEN '线下转线上'
 END as 来源,
alr.ACCEPT_NUM 受理编号,
c.claim_no 案件号,
c.customer_name 出险人姓名,
c.id_no 证件号码,
#  count(distinct inv.C_INV_NO) 账单数量,
cast(count(distinct  case when inv.C_INV_NO is not null  then inv.C_INV_NO  else  b.bill_no end)  as int ) 发票数量,
case when c.cancle_time is null then cast(alr.T_UPD_TIME as date) else cast(c.cancle_time as date) end as 撤案时间,
alr.WITHDRAWAL_REASON 撤案原因,
case alr.business_mode when 'I' then '半流程'
 when 'A' then '全流程'
else '' end 案件形式,
    p.C_DEPT_NAME 投保单位,
    ''

 FROM claim_ods.accept_list_record alr
LEFT JOIN claim_ods.claim c on c.acceptance_no = alr.ACCEPT_NUM
LEFT JOIN claim_ods.apply_claim ac on c.claim_no = ac.apply_no
LEFT JOIN claim_ods.claim_policy cp on cp.policy_no = ac.policy_part_no
LEFT JOIN claim_ods.claim_policy_customer cpc on cpc.customer_no = cp.customer_no
left join claim_ods.ply p on cp.group_policy_no = p.C_PLY_NO
left join claim_ods.`clm_visit_inv_info` inv on c.claim_no = inv.C_CUSTOM_APP_NO  and inv.C_DEL_FLAG = '0'  and inv.C_IS_NEED_SHOW='0'
 left join claim_ods.bill b on c.id = b.claim_id and b.delete_flag = '0'

WHERE alr.INSURE_COMPANY_CHANNEL = 'TK07'
   and ( (substr(c.cancle_time,1,4) in ('2023','2024','2022')  ) or
        (c.cancle_time is null and (  substr(alr.T_UPD_TIME,1,4) in ('2022','2023','2024') )  ) )
and (substr(c.cancle_time,1,7) <> '2024-10' or (c.cancle_time is null and  substr(alr.T_UPD_TIME,1,7) <> '2024-10') )

and alr.DEL_FLAG = '0'
 and (alr.ACCEPT_STATUS = '5' or c.clm_process_status = '11')
GROUP BY alr.ACCEPT_NUM

"""


# def truncate_table(table_name='CLAIM_DWD.DWD_TK07_CANCEL_SUMMARY_M'):
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
