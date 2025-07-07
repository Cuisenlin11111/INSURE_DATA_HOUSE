import pymysql
# import sys
#
# sys.path.append(r"E:\pycharm\database")
from database import DatabaseConnection
from datetime import datetime
from datetime import datetime, timedelta


# 获取当前日期时间
current_date = datetime.now()
# 计算6个月前的日期
six_months_ago_date = current_date - timedelta(days=15)
six_months = six_months_ago_date.strftime('%Y-%m')


sql_query = f"""
  -- @description: 泰康对账明细表-去掉一拆多
  -- @param:
  -- @return:
  -- @alter:
  -- @time: 2024-06-20 15:01:06
  -- @author: 01
  -- @version: 1.0.0

    with   TK_BILL_DETIL_DISTINCT_TMP as (
    SELECT alr.insure_company_channel,
           pr.sucess_time,
           alr.policy_no,
           alr.ACCEPT_STATUS,
           case
             when alr.claim_source in ('1', '3') then
              '线下'
             WHEN alr.claim_source in ('2', '4') THEN
              '线上'
             else
              ''
           END as claim_type,
           group_concat(distinct inv.C_RESPONSE_DESC) as MEDICAL_TYPE,
           case
             when c.claim_no like '%BL%' then
              '补偿案件'
             else
              '否'
           end as if_bcaj,
           '' as claim_duty,
           alr.ACCEPT_NUM accept_num,
           c.claim_no ysj_claim_no,
           case
             when alr.case_no is null then
              pr.insurance_claim_no
             else
              alr.case_no
           end case_no,
           case
             when cpc.insured_name is not null then
              cpc.insured_name
             else
              c.customer_name
           end insured_name,
           case
             when cpc.insured_id_no is not null then
              cpc.insured_id_no
             else
              c.id_no
           end insured_id_no,
           CASE
             WHEN alr.expense_type = '1' THEN
              '普通'
             WHEN alr.expense_type = '2' THEN
              '特殊'
           END AS claim_lb,
           count(distinct inv.C_INV_NO) as c_inv_no,
           sum(inv.N_COMPENSATE_AMT) as compensate_amt,
           case
             when alr.ACCEPT_STATUS = '5' or c.clm_process_status = '11' then
              '撤案'
             when pr.back_status = '2' then
              '结案'
             else
              ''
           end as is_back,
           case when   alr.ACCEPT_STATUS = '5' or c.clm_process_status = '11'  and  c.cancle_time is not null then c.cancle_time
               when   alr.ACCEPT_STATUS = '5' or c.clm_process_status = '11'  and  c.cancle_time is  null then alr.T_UPD_TIME
                   else '' end as cancle_time,
           alr.withdrawal_reason cancle_reason,
           c.clm_process_status,
           pr.back_status
      FROM claim_ods.accept_list_record alr
      LEFT JOIN claim_ods.claim c
        on c.acceptance_no = alr.ACCEPT_NUM and c.insure_company_channel in ('TK01','TK02','TK04','TK06','TK07')
      LEFT JOIN claim_ods.clm_app_info cai
        on cai.C_CUSTOM_APP_NO = c.claim_no
       and cai.C_DEL_FLAG = '0' and cai.INSURANCE_COMPANY in  ('TK01','TK02','TK04','TK06','TK07')
      LEFT JOIN claim_ods.claim_policy cp
        on cp.policy_no = cai.C_PLY_NO
      LEFT JOIN claim_ods.claim_policy_customer cpc
        on cpc.customer_no = cp.customer_no
      LEFT JOIN claim_ods.postback_record pr
        on pr.accept_num = alr.ACCEPT_NUM
       and pr.is_deleted = 'N'
       and pr.receiver = 'I' and pr.insure_company_channel in ('TK01','TK02','TK04','TK06','TK07')
      LEFT JOIN claim_ods.clm_visit_inv_info inv
        on inv.C_CUSTOM_APP_NO = c.claim_no
         and      inv.C_PLY_NO = cai.C_PLY_NO 
       and inv.C_BILL_TYP <> '3'
       and inv.C_DEL_FLAG = '0'  AND inv.INSURANCE_COMPANY in  ('TK01','TK02','TK04','TK06','TK07')
     where alr.DEL_FLAG = '0'  and alr.insure_company_channel in ('TK01','TK02','TK04','TK06','TK07')
     and  (   (pr.back_status='2' and substr(pr.back_time,1,7)>='{six_months}'  )  or
              ( (alr.ACCEPT_STATUS = '5' or c.clm_process_status = '11')  and (substr(c.cancle_time,1,7)>='{six_months}' or substr(alr.T_UPD_TIME,1,7)>='{six_months}' )  )  )
     GROUP BY alr.insure_company_channel,
              alr.ACCEPT_NUM ),
        t2 as
     (select insure_company_channel,
             sucess_time as comm_date,
             policy_no,
             claim_type,
                          case
                WHEN MEDICAL_TYPE like '%住院%' THEN '住院' ELSE '门诊' END AS  MEDICAL_TYPE,
             if_bcaj,
             claim_duty,
             accept_num,
             ysj_claim_no,
             case_no,
             insured_name,
             insured_id_no,
             claim_lb,
             c_inv_no,
             compensate_amt,
             is_back,
             cancle_time,
             cancle_reason
        from TK_BILL_DETIL_DISTINCT_TMP
       WHERE back_status in (2, 21)
      union all
      select insure_company_channel,
             cancle_time as comm_date,
             policy_no,
             claim_type,
                          case
                WHEN MEDICAL_TYPE like '%住院%' THEN '住院' ELSE '门诊' END AS  MEDICAL_TYPE,
             if_bcaj,
             claim_duty,
             accept_num,
             ysj_claim_no,
             case_no,
             insured_name,
             insured_id_no,
             claim_lb,
             c_inv_no,
             compensate_amt,
             is_back,
             cancle_time,
             cancle_reason
        from TK_BILL_DETIL_DISTINCT_TMP
       WHERE clm_process_status ='11' or ACCEPT_STATUS = '5' )
    select insure_company_channel,
           cast(comm_date as date) as comm_date,
           policy_no,
           claim_type,
           MEDICAL_TYPE,
           if_bcaj,
           claim_duty,
           accept_num,
           ysj_claim_no,
           case_no,
           insured_name,
           insured_id_no,
           claim_lb,
           c_inv_no,
           cast(compensate_amt as decimal(10, 2)) as compensate_amt,
           is_back,
           cancle_time as cancle_time,
           cancle_reason,
           chao8,
           cast(case
             when insure_company_channel = '泰康养老山东分公司' and claim_type = '线上' then
              (7 + chao8 * 8 / 7)
             when insure_company_channel = '泰康养老山东分公司' and claim_type = '线下' then
              (12 + chao8 * 8 / 12)
             else
              0
           end as decimal(10, 2)) as fee,
           substr(replace(comm_date,'-',''),1,8)
      from (select dim.channel_value insure_company_channel,
                   comm_date,
                   policy_no,
                   claim_type,
                   MEDICAL_TYPE,
                   if_bcaj,
                   claim_duty,
                   accept_num,
                   ysj_claim_no,
                   case_no,
                   insured_name,
                   insured_id_no,
                   claim_lb,
                   c_inv_no,
                   compensate_amt,
                   is_back,
                   cancle_time,
                   cancle_reason,
                   case
                     when  cast(c_inv_no as int) > 8 then  cast(c_inv_no as int) - 8
                     when  cast(c_inv_no as int) <= 8 then  0
                     else 0
                   end as chao8
              from t2
             inner join claim_ods.dim_insure_company_channel dim
                on insure_company_channel = dim.channel_key
                where dim.channel_key  like 'TK%'
                and  substr(comm_date,1,7)>='{six_months}'
           );
"""


def truncate_table(table_name='CLAIM_DWD.DWD_TK_BILL_DETAIL'):
    with DatabaseConnection() as conn:
        truncate_sql = f"delete from  {table_name}  where  substr(comm_date,1,7)>='{six_months}' "
        with conn.cursor() as cursor:
            cursor.execute(truncate_sql)
            conn.commit()


def insert_data(sql_query):
    insert_sql = """
        insert into CLAIM_DWD.DWD_TK_BILL_DETAIL
        (insure_company_channel,
         comm_date,
         policy_no,
         claim_type,
         MEDICAL_TYPE,
         if_bcaj,
         claim_duty,
         accept_num,
         ysj_claim_no,
         case_no,
         insured_name,
         insured_id_no,
         claim_lb,
         c_inv_no,
         compensate_amt,
         is_back,
         cancle_time,
         cancle_reason,
         chao8,
         fee,
         data_dt)
        values (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
    """
    batch_size = 1000  # 进一步减小分批插入的批次大小
    with DatabaseConnection() as conn:
        with conn.cursor() as cursor:
            cursor.execute(sql_query)
            while True:
                row = cursor.fetchone()
                if row is None:
                    break
                data = []
                for i in range(batch_size):
                    if row:
                        # 确保 row 是元组或列表，并且元素数量与占位符数量匹配
                        if isinstance(row, (tuple, list)) and len(row) == 18:
                            data.append(row)
                        row = cursor.fetchone()
                    else:
                        break
                if data:
                    cursor.executemany(insert_sql, data)
                    conn.commit()


if __name__ == "__main__":
    start_time = datetime.now().strftime("%Y-%m-d %H:%M:%S")
    print("程序开始时间：", start_time)
    truncate_table()
    insert_data(sql_query)
    end_time = datetime.now().strftime("%Y-%m-d %H:%M:%S")
    print("程序结束时间：", end_time)