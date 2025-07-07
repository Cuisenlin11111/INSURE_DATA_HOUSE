# import sys
#
# sys.path.append(r"E:\pycharm\database")
from database import DatabaseConnection
from datetime import datetime
from datetime import datetime, timedelta





sql_query = f"""
  -- @description: 泰康全渠道撤案
  -- @param:
  -- @return:
  -- @alter:
  -- @time: 2024-06-20 15:01:06
  -- @author: 01
  -- @version: 1.0.0
INSERT INTO `CLAIM_DWD`.`DWD_TK_CANCEL_SUMMARY_M`
SELECT
    case when alr.INSURE_COMPANY_CHANNEL = 'TK01' then '泰康养老上海分公司'
         when alr.INSURE_COMPANY_CHANNEL = 'TK02' then '泰康养老山东分公司'
        when alr.INSURE_COMPANY_CHANNEL = 'TK04' then '泰康养老北京分公司'
        when alr.INSURE_COMPANY_CHANNEL = 'TK06' then '泰康养老河南分公司'
        when alr.INSURE_COMPANY_CHANNEL = 'TK07' then '泰康养老广东分公司'
        when alr.INSURE_COMPANY_CHANNEL = 'TK09' then '泰康养老江苏分公司'
        when alr.INSURE_COMPANY_CHANNEL = 'TK10' then '泰康养老辽宁分公司'
        else '' end as INSURE_COMPANY_CHANNEL,
    case when alr.claim_source = '1' then '线下'
         WHEN alr.claim_source = '2' THEN '线上'
         WHEN alr.claim_source = '3' THEN '线上转线下'
         WHEN alr.claim_source = '4' THEN '线下转线上'
         END as 来源,
    alr.accept_batch_no 受理批次号,
    iat.num 批次案件数,
    alr.ACCEPT_NUM 受理编号,
    c.claim_no 因朔桔案件号,
    cp.channel_group_policy_no 团单号,
    cpc.insured_name 出险人姓名,
    cpc.insured_id_no 证件号码,
    group_concat(distinct inv.c_response_desc) 赔付责任,
    cast(count(distinct  case when inv.C_INV_NO is not null  then inv.C_INV_NO  else  b.bill_no end)  as int )  发票数量,
    cast(cai.N_FINAL_COMPENSATE_AMT as decimal(10, 2)) 赔付金额,
    -- 修改撤案时间的取值逻辑
    case when c.cancle_time is null then cast(alr.T_UPD_TIME as date) else cast(c.cancle_time as date) end as 撤案时间,
    alr.WITHDRAWAL_REASON 撤案原因,
    case alr.business_mode when 'I' then '半流程'
    when 'A' then '全流程'
    else '' end 案件形式,
    p.C_DEPT_NAME 投保单位,
    '2024-12'
FROM claim_ods.accept_list_record alr
LEFT JOIN claim_ods.claim c on c.acceptance_no = alr.ACCEPT_NUM
LEFT JOIN claim_ods.apply_claim ac on c.claim_no = ac.apply_no
LEFT JOIN claim_ods.claim_policy cp on cp.policy_no = ac.policy_part_no
LEFT JOIN claim_ods.claim_policy_customer cpc on cpc.customer_no = cp.customer_no
left join claim_ods.clm_app_info cai on ac.apply_no = cai.C_CUSTOM_APP_NO and ac.policy_part_no = cai.c_ply_no and cai.C_DEL_FLAG = '0' and cai.INSURANCE_COMPANY like 'TK%'
left join claim_ods.`clm_visit_inv_info` inv on cai.C_CUSTOM_APP_NO = inv.C_CUSTOM_APP_NO and cai.c_ply_no = inv.c_ply_no and inv.C_DEL_FLAG = '0' AND inv.INSURANCE_COMPANY like 'TK%'  and inv.C_IS_NEED_SHOW = '0'
left join claim_ods.`image_assign_task` iat on alr.accept_batch_no = iat.accept_batch_no
left join claim_ods.ply p on cp.group_policy_no = p.C_PLY_NO
left join claim_ods.bill b on c.id = b.claim_id and b.delete_flag = '0'
WHERE alr.INSURE_COMPANY_CHANNEL like '%TK%'
AND  (  substr(c.cancle_time, 1, 7)='2024-12'   or ( c.cancle_time is null and substr(alr.T_UPD_TIME, 1, 7)='2024-12' ))
  and alr.DEL_FLAG = '0'
  and (alr.ACCEPT_STATUS = '5' or c.clm_process_status = '11')
GROUP BY alr.ACCEPT_NUM
"""


def truncate_table(table_name='CLAIM_DWD.DWD_TK_CANCEL_SUMMARY_M'):
    with DatabaseConnection() as conn:
        truncate_sql = f"delete  from {table_name} where DATE_MON='2024-12'"
        with conn.cursor() as cursor:
            cursor.execute(truncate_sql)
            conn.commit()


def insert_data(sql_query):
    with DatabaseConnection() as conn:
        with conn.cursor() as cursor:
            cursor.execute(sql_query)
            conn.commit()


if __name__ == "__main__":
     # print(sql_query)
    #print(truncate_sql)
    start_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    print("程序开始时间：", start_time)
    truncate_table()
    insert_data(sql_query)
    end_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    print("程序结束时间：", end_time)
