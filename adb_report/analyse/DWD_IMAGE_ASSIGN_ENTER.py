# import sys
# sys.path.append(r"E:\pycharm\database")
from database import DatabaseConnection
from datetime import datetime



sql_query = f"""
  -- @description: 录入商日报
  -- @param:
  -- @return:
  -- @alter:
  -- @time: 2024-10-12 15:01:06
  -- @author: 01
  -- @version: 1.0.0
INSERT INTO CLAIM_DWD.DWD_IMAGE_ASSIGN_ENTER (
    insure_company_channel,
    enter_merchant,
    comm_date,
    division_nu,
    lrl,
    data_dt
)
SELECT
    dim.CHANNEL_VALUE AS insure_company_channel,
    t1.enter_merchant AS enter_merchant,
    t1.comm_date AS comm_date,
    SUM(NVL(t1.division_nu, 0)) AS division_nu,
    SUM(NVL(t2.lrl, 0)) AS lrl,
    REPLACE(t1.comm_date, '-', '')
FROM (
    SELECT
        CASE
            WHEN i.input_company_code IS NOT NULL AND i.input_company_code = 'KNVS' THEN '成都视觉'
            WHEN i.input_company_code IS NOT NULL AND i.input_company_code = 'GTRS' THEN '广纳'
            WHEN i.input_company_code IS NOT NULL AND i.input_company_code = 'ZZTN' THEN '智在'
            WHEN i.input_company_code IS NOT NULL AND i.input_company_code = 'YSJU' THEN '因朔桔'
            WHEN i.input_company_code IS NOT NULL AND i.input_company_code = 'SPIC' THEN '施博'
            ELSE '其他'
        END AS enter_merchant,
        i.INSURE_COMPANY_CHANNEL,
        SUBSTR(i.ALLOT_TIME, 1, 10) AS comm_date,
        COUNT(*) AS division_nu
    FROM claim_ods.image_assign_task i
    GROUP BY i.input_company_code, i.INSURE_COMPANY_CHANNEL, SUBSTR(i.ALLOT_TIME, 1, 10)
    ORDER BY comm_date DESC
) t1
LEFT JOIN (
    SELECT
        CASE
            WHEN i.input_company_code IS NOT NULL AND i.input_company_code = 'KNVS' THEN '成都视觉'
            WHEN i.input_company_code IS NOT NULL AND i.input_company_code = 'GTRS' THEN '广纳'
            WHEN i.input_company_code IS NOT NULL AND i.input_company_code = 'ZZTN' THEN '智在'
            WHEN i.input_company_code IS NOT NULL AND i.input_company_code = 'YSJU' THEN '因朔桔'
            WHEN i.input_company_code IS NOT NULL AND i.input_company_code = 'SPIC' THEN '施博'
            ELSE '其他'
        END AS enter_merchant,
        i.INSURE_COMPANY_CHANNEL,
        SUBSTR(i.ALLOT_TIME, 1, 10) AS comm_date,
        COUNT(*) AS lrl
    FROM claim_ods.image_assign_task i
    WHERE i.STATUS = '03'
    GROUP BY i.input_company_code, i.INSURE_COMPANY_CHANNEL, SUBSTR(i.ALLOT_TIME, 1, 10)
    ORDER BY comm_date DESC
) t2
ON t1.enter_merchant = t2.enter_merchant
    AND t1.comm_date = t2.comm_date
    AND t1.INSURE_COMPANY_CHANNEL = t2.INSURE_COMPANY_CHANNEL
INNER JOIN claim_ods.dim_insure_company_channel dim
ON t1.INSURE_COMPANY_CHANNEL = dim.channel_key
GROUP BY dim.CHANNEL_VALUE, t1.enter_merchant, t1.comm_date
ORDER BY t1.comm_date DESC;


"""
def truncate_table(table_name='CLAIM_DWD.DWD_IMAGE_ASSIGN_ENTER'):
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
