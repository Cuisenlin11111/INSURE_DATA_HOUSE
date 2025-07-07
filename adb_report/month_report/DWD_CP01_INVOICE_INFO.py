# import sys
#
# sys.path.append(r"E:\pycharm\database")
from database import DatabaseConnection
from datetime import datetime
from datetime import datetime, timedelta



sql_query = f"""
  -- @description: 中智账单层数据
  -- @param:
  -- @return:
  -- @alter:
  -- @time: 2024-09-26 15:01:06
  -- @author: 01
  -- @version: 1.0.0


insert into  `CLAIM_DWD`.`DWD_CP01_INVOICE_INFO`
SELECT
  DISTINCT
  fsr.file_num 中智案卷号,
  c.claim_no 申请号,
  c.acceptance_no 受理编号,
  cpc2.cust_company_code 公司编号,
  cpc2.cust_group_code 集团编号,
  cpc2.channel_customer_no 雇员编号,
  cpc2.insured_name 雇员姓名,
  case
  when cpc2.insured_gender = 'M' then '男'
  when cpc2.insured_gender = 'F' then '女'
  end as 雇员性别,
  cpc.channel_customer_no 出险人雇员编号,
  cpc.insured_name 出险人姓名,
  case
  when cpc.insured_gender = 'M' then '男'
  when cpc.insured_gender = 'F' then '女'
  end as 出险人性别,
  cpc.insured_birthday 出险人出生日期,
  case
  when cpc.main_insured_relation = '1' then '本人'
  when cpc.main_insured_relation = '2' then '子女'
  when cpc.main_insured_relation = '3' then '配偶'
  end as 出险人与雇员关系,
  cai.APPLY_TM 申请时间,
  cai.T_ACCIDENT_TM 出险时间,
  c.bill_num 账单张数,
  c.claim_amount 索赔金额,
  cai.N_FINAL_COMPENSATE_AMT 中智赔付金额,
  case when cai.C_COMPENSATE_RESULT = '4'
  then '拒赔'
  when cai.C_COMPENSATE_RESULT = '1'
  then '正常赔付'
   end as 赔付结论,
  -- REPLACE(REPLACE(REPLACE(cai.C_INTERNAL_CONCLUSION,CHAR(10),''),CHAR(13),''),CHAR(9),'') 赔付结论描述,
  DATE_FORMAT(replace(replace(alr.ACCEPT_DATE,'AM',''),'PM',''), '%Y-%m-%d') 受理日期,
  cp.insure_effective_date 保单生效日期,
  cp.insure_expiry_date 保单截止日期,
  case
  when alr.claim_source = '1' then '线下'
 WHEN alr.claim_source = '2' THEN '线上'
 WHEN alr.claim_source = '3' THEN '线上转线下'
 WHEN alr.claim_source = '4' THEN '线下转线上'
  end as 申请来源,
  inv.c_ply_no 保单号,
  inv.C_INV_NO 发票号,
  inv.T_VISIT_BGN_TM 就诊开始时间,
  inv.T_VISIT_END_TM 就诊结束时间,
  inv.C_RESPONSE_DESC 责任,
  inv.N_SUM_AMT 总金额,
  inv.N_SOCIAL_GIVE_AMOUNT 医保支付金额,
  inv.N_CATEG_SELFPAY 分类自负金额,
  inv.N_SELF_EXPENSE 自费金额,
  inv.N_THIRD_PAY_AMT 第三方支付金额,
  inv.N_EXTRA_PAY_AMOUNT 附加支付,
  inv.N_FINAL_PAY 最终赔付金额,
  case when inv.C_COMPENSATE_RESULT = '1' then '正常赔付'
  when inv.C_COMPENSATE_RESULT = '4' then '拒赔'
  end as 赔付结果,
  -- REPLACE(REPLACE(REPLACE(inv.C_INTERNAL_CONCLUSION,CHAR(10),''),CHAR(13),''),CHAR(9),'') 结论描述,
  inv.N_DEDUCT_AMT 扣减金额,
  inv.N_DEDUCTLE_AMT 免赔额,
  inv.C_HOSPITAL_NME 医院名称,
  concat(bhl.province_name,bhl.city_name) 医院所属地,
  inv.C_DIAG_CDE 疾病码,
  REPLACE(REPLACE(REPLACE(inv.C_DIAG_NME,CHAR(10),''),CHAR(13),''),CHAR(9),'') 疾病名称,
  cpc.insured_id_no 证件号码,
  fsr.end_date  结案日期,
  DATE_FORMAT(cp.insure_effective_date,'%Y') 年份,
  '202502'
FROM
  claim_ods.claim c
  INNER JOIN claim_ods.accept_list_record alr on alr.ACCEPT_NUM = c.acceptance_no  and   alr.DEL_FLAG ='0'
  INNER JOIN claim_ods.clm_app_info cai on cai.C_CUSTOM_APP_NO = c.claim_no and cai.c_del_flag = '0'
  inner join claim_ods.clm_visit_inv_info inv on inv.C_CUSTOM_APP_NO = cai.C_CUSTOM_APP_NO and inv.C_PLY_NO = cai.C_PLY_NO and inv.c_del_flag = '0'
  INNER JOIN claim_ods.claim_policy cp on cp.policy_no = cai.C_PLY_NO
  INNER JOIN claim_ods.claim_policy_customer cpc on cpc.customer_no = cp.customer_no
  INNER JOIN claim_ods.claim_policy_customer cpc2 on cpc.main_channel_customer_no = cpc2.channel_customer_no
  inner join claim_ods.front_seq_record fsr on TRIM(fsr.app_no) = c.claim_no and fsr.is_deleted = 'N'
  left join claim_ods.base_hospital_label_v3 bhl on inv.C_HOSPITAL_NO = bhl.hospital_code
WHERE
    c.delete_flag = '0'   and
  cai.CHANNEL_TYPE = 'M'
  and c.insure_company_channel = 'CP01'
  and substring(cp.insure_effective_date,1,4) = '2025'
  and (substring(fsr.file_num,7,6) = '202502' or
   (substring(fsr.file_num,9,6) = '202502' AND substring(fsr.file_num,7,2) = 'WW'))
  and fsr.`state` in ('4')
ORDER BY c.claim_no desc

"""




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
