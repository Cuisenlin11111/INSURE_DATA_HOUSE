# import sys
# sys.path.append(r"E:\pycharm\database")
from database import DatabaseConnection
from datetime import datetime



sql_query = f"""
  -- @description: 进件量日分布统计
  -- @param:
  -- @return:
  -- @alter:
  -- @time: 2024-08-29 15:01:06
  -- @author: 01
  -- @version: 1.0.0
INSERT INTO CLAIM_DWD.DWD_COMPANY_CHANNEL_HOUR (
    insure_company_channel,         -- 渠道
    jjl,                            -- 进件量
    t_crt_time,                     -- 进件日期
    jj_hour,                        -- 进件小时
    JJ_DAY_HOUR ,                   -- 进件日期小时
    data_dt                         -- 调度日期
)
SELECT
    dim.channel_value AS insure_company_channel,
    COUNT(*) AS jjl,
    SUBSTR(a1.T_CRT_TIME, 1, 10) AS t_crt_time,
    HOUR(a1.T_CRT_TIME) AS jj_hour,
    concat(replace(substr(a1.T_CRT_TIME,6,5) ,'-',''),'-',replace(substr(T_CRT_TIME,12,2) ,'-','')),
    REPLACE(SUBSTR(a1.T_CRT_TIME, 1, 10), '-', '') AS data_dt
FROM claim_ods.accept_list_record a1
LEFT JOIN claim_ods.dim_insure_company_channel dim
    ON a1.insure_company_channel = dim.channel_key
WHERE dim.channel_value IS NOT NULL
    AND a1.DEL_FLAG = '0'
    AND a1.insure_company_channel NOT IN ('common')
GROUP BY dim.channel_value,
         SUBSTR(a1.T_CRT_TIME, 1, 10),
         HOUR(a1.T_CRT_TIME);
"""
def truncate_table(table_name='CLAIM_DWD.DWD_COMPANY_CHANNEL_HOUR'):
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
