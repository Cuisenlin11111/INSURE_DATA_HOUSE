# import sys
# sys.path.append(r"E:\pycharm\database")
from database import DatabaseConnection
from datetime import datetime



sql_query = f"""
  -- @description: 录入量日分布统计
  -- @param:
  -- @return:
  -- @alter:
  -- @time: 2024-10-12 15:01:06
  -- @author: 01
  -- @version: 1.0.0
INSERT INTO CLAIM_DWD.DWD_LRL_DAY_COUNT (
    enter_merchant,
    t_crt_time,
    jj_hour,
    division_nu,
    lrl,
    DATA_DT
)
SELECT
    t1.enter_merchant,
    t1.t_crt_time,
    t1.jj_hour,
    COALESCE(t1.division_nu, 0) AS division_nu,
    COALESCE(t2.lrl, 0) AS lrl,
    REPLACE(t1.t_crt_time, '-', '')
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
        i.UPLOAD_COMPANY_CODE,
        SUBSTR(i.GMT_CREATED, 1, 10) AS t_crt_time,
        substr(i.GMT_CREATED,12,2) AS jj_hour,
        COUNT(*) AS division_nu
    FROM claim_ods.image_assign_task i
    WHERE i.STATUS = '02'
    GROUP BY i.input_company_code, i.UPLOAD_COMPANY_CODE, substr(i.GMT_CREATED,12,2), SUBSTR(i.GMT_CREATED, 1, 10)
    ORDER BY t_crt_time DESC
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
        i.UPLOAD_COMPANY_CODE,
        SUBSTR(i.GMT_CREATED, 1, 10) AS t_crt_time,
        substr(i.GMT_CREATED,12,2) AS jj_hour,
        COUNT(*) AS lrl
    FROM claim_ods.image_assign_task i
    WHERE i.STATUS = '03'
    GROUP BY i.input_company_code, i.UPLOAD_COMPANY_CODE, substr(i.GMT_CREATED,12,2), SUBSTR(i.GMT_CREATED, 1, 10)
    ORDER BY t_crt_time DESC
) t2
ON t1.enter_merchant = t2.enter_merchant
    AND t1.t_crt_time = t2.t_crt_time
    AND t1.jj_hour = t2.jj_hour
ORDER BY t1.t_crt_time DESC;

"""
def truncate_table(table_name='CLAIM_DWD.DWD_LRL_DAY_COUNT'):
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
