# import sys
# sys.path.append(r"E:\pycharm\database")
from database import DatabaseConnection
from datetime import datetime



sql_query = f"""
  -- @description: 案件数量统计周表
  -- @param:
  -- @return:
  -- @alter:
  -- @time: 2024-09-18 15:01:06
  -- @author: 01
  -- @version: 1.0.0
INSERT INTO CLAIM_DWS.DWS_CLAIM_COUNT_WEEK
SELECT
    insure_company_channel,
    gmt_created AS comm_date,
    dt_dim.week_qj AS week_region,
    SUM(jjl) AS jjl,
    SUM(cancel_vol) AS cancel_vol,
    SUM(xxqd) AS xxqd,
    SUM(xsqd) AS xsqd,
    SUM(lrl) AS lrl,
    SUM(drhcal) AS drhcal,
    SUM(blchcal) AS blchcal,
    SUM(qlchcal) AS qlchcal,
    SUM(hcsbal) AS hcsbal,
    SUM(zdshal) AS zdshal,
    sum(ZDSHAL_RW) as  ZDSHAL_RW,
    SUM(fh_auto_claim_count) AS fh_auto_claim_count,
    SUM(question_claim_vol) AS question_claim_vol,
    SUM(hospatal_claim_vol) AS hospatal_claim_vol,
    cast(fhzdhl as  decimal(10,4))   fh_auto_claim_rate,
    cast(case when  sum(drhcal) =0 then 0 else sum(qlchcal)/sum(drhcal) end as  decimal(10,4))  qlchcal_rate,
    cast(COALESCE(SHZDHL,0) as  decimal(10,4))   ZDSHAL_rate,
    cast(COALESCE(SHZDHL_RW,0) as decimal(10,4)),
    current_timestamp,
    replace(substring(gmt_created,1,10),'-','') AS data_dt
FROM claim_dwd.DWD_CLAIM_COUNT_DAY
INNER JOIN claim_ods.DIM_DATE_WEEK dt_dim
ON gmt_created = dt_dim.date_dtd
WHERE       nvl(insure_company_channel,'')<>''
GROUP BY
    insure_company_channel,
    gmt_created,
    dt_dim.week_qj;
"""
def truncate_table(table_name='CLAIM_DWS.DWS_CLAIM_COUNT_WEEK'):
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
