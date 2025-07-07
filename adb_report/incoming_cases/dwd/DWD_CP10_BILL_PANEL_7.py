# import sys
# sys.path.append(r"E:\pycharm\database")
from database import DatabaseConnection
from datetime import datetime



sql_query = f"""
  -- @description: 总发票数
  -- @param:
  -- @return:
  -- @alter:
  -- @time: 2024-08-29 15:01:06
  -- @author: 01
  -- @version: 1.0.0



# 
INSERT INTO CLAIM_DWD.DWD_BILL_PANEL_7
    (insure_company_channel,
     create_time,
     elec_bill_claim_vol,
     all_elec_bill_claim_vol,
     data_dt)
-- 创建一个中间表 filtered_bills 来存储符合条件的 claim_id 和 bill_shape
WITH filtered_bills AS (
    SELECT
        claim_id,
        bill_shape
    FROM
        claim_ods.bill
    WHERE
        delete_flag = '0' AND bill_shape = '6'
),

-- 统计每个 claim_id 的电子发票数量（只包含 shape=6 的账单）
elec_bill_claims AS (
        SELECT
        claim_id,
        sum(case when bill_shape = '6' then 1 else 0 end) as elec_bill
    FROM
        claim_ods.bill
    WHERE
        delete_flag = '0'
    group by claim_id
having count(*) - elec_bill=0
),
-- 计算每个 claim_id 的总电子发票案件数（包含 shape=6 的账单）
all_elec_bill_claims AS (
    SELECT
        alr.department_code insure_company_channel,
        SUBSTR(c.create_time, 1, 10) AS create_time,
        count(DISTINCT fb.claim_id) AS electronic_bills,
        COUNT(DISTINCT eb.claim_id) AS all_electronic_bills
    FROM
        claim_ods.claim c
    LEFT JOIN
            claim_ods.accept_list_record alr on c.acceptance_no=alr.ACCEPT_NUM
    LEFT JOIN
        filtered_bills fb ON c.id = fb.claim_id
    LEFT JOIN  elec_bill_claims eb ON c.id = eb.claim_id
    where  c.insure_company_channel='CP10' AND alr.department_code  in ('大连分公司','苏州分公司')
    GROUP BY
        insure_company_channel,
        SUBSTR(c.create_time, 1, 10)
)

-- 最终插入语句
SELECT
    dim.channel_value AS insure_company_channel,
    aec.create_time,
    COALESCE(aec.electronic_bills, 0) AS elec_bill_claim_vol,
    COALESCE(aec.all_electronic_bills, 0) AS all_elec_bill_claim_vol,
    REPLACE(aec.create_time, '-', '') AS data_dt
FROM
    all_elec_bill_claims aec
LEFT JOIN
    claim_ods.dim_insure_company_channel dim
ON
    aec.insure_company_channel = dim.channel_key;

"""
def truncate_table(table_name='CLAIM_DWD.DWD_BILL_PANEL_7'):
    with DatabaseConnection() as conn:
        truncate_sql = f"delete from  {table_name}   where INSURE_COMPANY_CHANNEL in ('太保产险大连分公司','太保产险苏州分公司')  "
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
