# import sys
# sys.path.append(r"E:\pycharm\database")
from database import DatabaseConnection
from datetime import datetime, date, timedelta

today = date.today()
# 计算7天前的日期
formatted_date = today.strftime("%Y%m%d")
seven_days_ago = today - timedelta(days=7)
# 计算昨天的日期
yesterday = today - timedelta(days=1)



sql_query = f"""
  -- @description: 因朔桔录入差错表
  -- @param:
  -- @return:
  -- @alter:
  -- @time: 2024-10-28 15:01:06
  -- @author: 01
  -- @version: 1.0.0
INSERT INTO CLAIM_DWD.DWD_YSJ_LR_ERROR_INFO (
    claim_no,
    back_time,
    bill_no,
      lr_name ,
      fh_name ,
    bill_user_name_actual,
    bill_payer_entered,
    treatment_date_actual,
    treatment_date_entered,
    hospital_name_actual,
    hospital_name_entered,
    bill_amount_actual,
    total_amount_entered,
    in_hospital_date_actual,
    in_hospital_date_entered,
    out_hospital_date_actual,
    out_hospital_date_entered,
    bill_type_actual,
    bill_type_entered,
    bill_shape_actual,
    bill_form_entered,
    vip_type_actual,
    special_entered,
    mergency_type_actual,
    emergency_treatment_entered,
    health_type_actual,
    recovery_entered,
    social_pay_amount_actual,
    social_security_payment_entered,
    personal_pay_amount_actual,
    personal_account_payment_entered,
    deductible_actual,
    start_pay_line_entered,
    class_pay_amount_actual,
    classified_self_payment_entered,
    own_pay_amount_actual,
    own_payment_entered,
    extra_pay_amount_actual,
    additional_payment_entered,
    third_party_pay_amount_actual,
    third_pay_amount_entered,
    remark_actual,
    remark_entered,
    hospital_dept_actual,
    department_code_entered,
    diagnosis1_actual,
    diagnosis1_entered,
    diagnosis2_actual,
    diagnosis2_entered,
    diagnosis3_actual,
    diagnosis3_entered,
    data_dt
)
SELECT
        t1.claim_no,
       t1.back_time,
       t1.bill_no,
       t2.录入姓名,
       t2.复核姓名,
       t1.bill_user_name 发票姓名实际,
       t2.bill_payer 发票姓名录入,
       t1.treatment_date 就诊日期实际,
       substr(t2.medical_date,1,10) 就诊日期录入,
       t1.hospital_name 医院名称实际,
       t2.hospital_name 医院名称录入,
       cast(t1.bill_amount as decimal(10,2))  发票金额实际,
       cast(t2.total_amount  as decimal(10,2))  发票金额录入,
       t1.in_hospital_date 入院日期实际,
       substr(t2.in_hospital_date,1,10) 入院日期录入,
       t1.out_hospital_date 出院日期实际,
       substr(t2.out_hospital_date,1,10) 出院日期录入,
       t1.bill_type 账单类型实际,
       t2.bill_type 账单类型录入,
       t1.bill_shape 账单形式实际,
       t2.bill_form 账单形式录入,
       t1.vip_type 是否特需实际,
       t2.special 是否特需录入,
       t1.mergency_type 是否急诊实际,
       t2.emergency_treatment 是否急诊录入,
       t1.health_type 是否康复实际,
       t2.recovery 是否康复录入,
       cast(t1.social_pay_amount as decimal(10,2)) 社保支付金额实际,
       cast(t2.social_security_payment  as decimal(10,2))  社保支付金额录入,
       cast(t1.personal_pay_amount as decimal(10,2)) 个人账户支付实际,
       cast(t2.personal_account_payment  as decimal(10,2))  个人账户支付录入,
       cast(t1.deductible as decimal(10,2)) 起付线实际,
       cast(t2.start_pay_line  as decimal(10,2))  起付线录入,
       cast(t1.class_pay_amount  as decimal(10,2)) 分类自负金额实际,
       cast(t2.classified_self_payment  as decimal(10,2))  分类自负金额录入,
       cast(t1.own_pay_amount as decimal(10,2)) 自费金额实际,
       cast(t2.own_payment  as decimal(10,2)) 自费金额录入,
       cast(t1.extra_pay_amount   as decimal(10,2))  附加支付金额实际,
       cast(t2.additional_payment   as decimal(10,2))  附加支付金额录入,
       cast(t1.third_party_pay_amount   as decimal(10,2))  第三方支付金额实际,
       cast(t2.third_pay_amount   as decimal(10,2))  第三方支付金额录入,
       t1.remark 账单备注实际,
       t2.remark 账单备注录入,
       t1.hospital_dept 科室实际,
       t2.department_code 科室录入,
       t1.诊断1 as 诊断1实际,
       t2.diagnosis_name_one 诊断1录入,
       t1.诊断2 as 诊断2实际,
       t2.diagnosis_name_two 诊断2录入,
       t1.诊断3 as 诊断3实际,
       t2.diagnosis_name_three 诊断3录入,
       substr(t1.back_time,1,10) 
FROM (
    -- t1子查询部分
    select
       -- c.insure_company_channel 保险公司渠道,
       c.claim_no,
       pr.back_time,
       b.bill_no,
       b.bill_user_name,
       b.treatment_date,
       b.hospital_name,
       b.bill_amount,
       b.in_hospital_date,
       b.out_hospital_date,
       b.bill_type,
       b.bill_shape,
       b.vip_type,
       b.health_type,
       b.mergency_type,
       -- b.patient_special 是否门特,
       b.social_pay_amount,
       b.personal_pay_amount,
       -- b.personal_payment 个人支付金额,
       b.deductible,
       b.class_pay_amount,
       b.own_pay_amount,
       b.extra_pay_amount,
       b.third_party_pay_amount,
       -- b.third_pay 第三方支付,
       b.remark,
       b.hospital_dept,
       SUBSTRING_INDEX(b.diagnose_name, '|', 1) AS 诊断1,
            CASE
        WHEN LENGTH(b.diagnose_name) - LENGTH(REPLACE(b.diagnose_name, '|', '')) >= 1 THEN SUBSTRING_INDEX(SUBSTRING_INDEX(b.diagnose_name, '|', 2), '|', -1)
        ELSE NULL
    END AS 诊断2,
    CASE
        WHEN LENGTH(b.diagnose_name) - LENGTH(REPLACE(b.diagnose_name, '|', '')) >= 2 THEN SUBSTRING_INDEX(SUBSTRING_INDEX(b.diagnose_name, '|', 3), '|', -1)
        ELSE NULL
    END AS 诊断3
    from claim_ods.bill  b
    left JOIN claim_ods.claim c on c.id = b.claim_id
    left join claim_ods.postback_record pr on c.claim_no = pr.app_no and pr.is_deleted='N'
    where  b.delete_flag = '0'
    and pr.receiver='I'
    AND SUBSTR(PR.back_time,1,10)>='{seven_days_ago}'
    AND SUBSTR(PR.back_time,1,10)<='{yesterday}'
    AND  pr.back_status  in ('2','21')
) t1
    -- 连接t2子查询
    left join (
        -- t2子查询部分
        select
          cl.case_id,
           cb.bill_number,
           au.user_name AS 录入姓名,
           au22.user_name AS 复核姓名,
           cb.bill_payer,
           cb.medical_date,
           cb.hospital_name,
           cb.total_amount,
           cb.in_hospital_date,
           cb.out_hospital_date,
           cb.bill_type,
           cb.bill_form,
           cb.special,
           cb.emergency_treatment,
           cb.recovery,
           cb.patient_special,
           cb.social_security_payment,
           cb.personal_account_payment,
           cb.personal_payment,
           cb.start_pay_line,
           cb.classified_self_payment,
           cb.own_payment,
           cb.additional_payment,
           cb.third_pay_amount,
           cb.third_pay,
           cb.remark,
           cb.department_code,
           cb.diagnosis_name_one,
           cb.diagnosis_name_two,
           cb.diagnosis_name_three
        from inphile_ods.case_bill  cb
        INNER JOIN inphile_ods.case_image ci ON   cb.image_id = ci.id
        INNER JOIN inphile_ods.case_log cl ON  ci.case_id = cl.case_id
        left join inphile_ods.case_log cl11 on  ci.case_id = cl11.case_id and  cl11.node = '录入' AND cl11.handle = '账核诊录入'
        LEFT JOIN inphile_ods.auth_user au ON au.id = cl11.create_by
        left join inphile_ods.case_log cl22 on  ci.case_id = cl22.case_id and      (cl22.node = '复核' OR cl22.node = '质检')
    AND (cl22.handle = '复核录入' OR cl22.handle = '质检录入')
        LEFT JOIN inphile_ods.auth_user au22 ON au22.id = cl22.create_by
        where  cb.deleted = '0'
        group by cl.case_id, cb.bill_number
    ) t2 on t1.claim_no = t2.case_id and t1.bill_no = t2.bill_number
WHERE
    (case when t1.bill_user_name <> t2.bill_payer then 1 else 0 end) +
    (case when t1.treatment_date <> substr(t2.medical_date,1,10) then 1 else 0 end) +
    (case when t1.hospital_name <> t2.hospital_name then 1 else 0 end) +
    (case when cast(t1.bill_amount as decimal(10,2)) <> t2.total_amount then 1 else 0 end) +
    (case when t1.in_hospital_date <> substr(t2.in_hospital_date,1,10) then 1 else 0 end) +
    (case when t1.out_hospital_date <> substr(t2.out_hospital_date,1,10) then 1 else 0 end) +
    (case when t1.bill_type <> t2.bill_type then 1 else 0 end) +
    (case when t1.bill_shape <> t2.bill_form then 1 else 0 end) +
    (case when t1.vip_type <> t2.special then 1 else 0 end) +
    (case when t1.mergency_type <> t2.emergency_treatment then 1 else 0 end) +
    (case when t1.health_type <> t2.recovery then 1 else 0 end) +
    (case when cast(t1.social_pay_amount as decimal(10,2)) <> t2.social_security_payment then 1 else 0 end) +
    (case when cast(t1.personal_pay_amount as decimal(10,2)) <> t2.personal_account_payment then 1 else 0 end) +
    (case when cast(t1.deductible as decimal(10,2)) <> t2.start_pay_line then 1 else 0 end) +
    (case when cast(t1.class_pay_amount as decimal(10,2)) <> t2.classified_self_payment then 1 else 0 end) +
    (case when cast(t1.own_pay_amount as decimal(10,2)) <> t2.own_payment then 1 else 0 end) +
    (case when cast(t1.extra_pay_amount  as decimal(10,2))   <> t2.additional_payment then 1 else 0 end) +
    (case when cast(t1.third_party_pay_amount  as decimal(10,2))  <> t2.third_pay_amount then 1 else 0 end) +
    (case when t1.remark <> t2.remark then 1 else 0 end) +
    (case when t1.hospital_dept <> t2.department_code then 1 else 0 end) +
    (case when t1.诊断1 <> t2.diagnosis_name_one then 1 else 0 end) +
    (case when t1.诊断2 <> t2.diagnosis_name_two then 1 else 0 end) +
    (case when t1.诊断3 <> t2.diagnosis_name_three then 1 else 0 end) > 0;
"""
def truncate_table(table_name='CLAIM_DWD.DWD_YSJ_LR_ERROR_INFO'):
    with DatabaseConnection() as conn:
        truncate_sql = f"delete from {table_name}  where  data_dt>='{seven_days_ago}'    and data_dt<='{yesterday}'"
        with conn.cursor() as cursor:
            cursor.execute(truncate_sql)
            conn.commit()

def insert_data(sql_query):
    with DatabaseConnection() as conn:
        with conn.cursor() as cursor:
            cursor.execute(sql_query)
            conn.commit()



if __name__ == "__main__":
    start_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    print("程序开始时间：", start_time)
    truncate_table()
    insert_data(sql_query)
    end_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    print("程序结束时间：", end_time)
