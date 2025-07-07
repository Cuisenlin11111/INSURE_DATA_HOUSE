
# import sys
# sys.path.append(r"E:\pycharm\database")
from database import DatabaseConnection
from datetime import datetime



sql_query = f"""
  -- @description: 渤海人寿汇总数据
  -- @param:
  -- @return:
  -- @alter:
  -- @time: 2024-06-20 15:01:06
  -- @author: 01
  -- @version: 1.0.0
insert  into claim_dws.dws_medical_record_summary_df
WITH
    app_no_data as (
    SELECT C_APP_NO,
           DATA_DT,
       GROUP_CONCAT(url ORDER BY url SEPARATOR ';') AS url,
       GROUP_CONCAT(text ORDER BY text SEPARATOR ';') AS text
FROM CLAIM_DWD.dwd_medical_record_df
GROUP BY C_APP_NO,DATA_DT
    )
 ,final_table_data as (
select a.C_APP_NO ,                ##申请号
       a.url ,                     ##图片地址
       a.text ,                    ##识别文本信息
       b.treatment_date,           ## 就诊日期
       b.hospital_name,         ## 医院名称
       b.diagnose_code_auto,       ##匹配疾病CODE 系统
       b.diagnose_name_auto,       ##匹配疾病名称系统
       b.INSURE_COMPANY_CHANNEL,    ## 大渠道
       a.DATA_DT from
    app_no_data a
left join ( SELECT
       t.claim_no,
       t.treatment_date,           ## 就诊日期
       t.hospital_name,         ## 入院日期
       t.diagnose_code_auto,       ##匹配疾病CODE 系统
       t.diagnose_name_auto,       ##匹配疾病名称 系统
       t.INSURE_COMPANY_CHANNEL,    ## 大渠道
        rank() over (partition by t.INSURE_COMPANY_CHANNEL,t.claim_no order by t.treatment_date  desc ) as rn
FROM
    claim_ods.bill t ) b
 on a.C_APP_NO = b.claim_no and b.rn = 1)
select C_APP_NO,
       min(url) ,
       min(text) ,
       min(treatment_date),           ## 就诊日期
       min(hospital_name),            ## 医院名称
       min(diagnose_code_auto),       ##匹配疾病CODE 系统
       group_concat(distinct  diagnose_name_auto),       ##匹配疾病名称系统
       min(INSURE_COMPANY_CHANNEL),    ## 大渠道
       min(DATA_DT)
from final_table_data  group by C_APP_NO;

"""
def truncate_table(table_name='claim_dws.dws_medical_record_summary_df'):
    with DatabaseConnection() as conn:
        truncate_sql = f"TRUNCATE TABLE {table_name}"
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
