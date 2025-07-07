# import sys
# sys.path.append(r"E:\pycharm\database")
from database import DatabaseConnection
from datetime import datetime



sql_query = f"""
  -- @description: 案件数量环比统计表
  -- @param:
  -- @return:
  -- @alter:
  -- @time: 2024-06-20 15:01:06
  -- @author: 01
  -- @version: 1.0.0
insert into `CLAIM_DWS`.`DWS_CLAIM_JJL_SEQ`
WITH all_channel AS (
    SELECT
        '全渠道'  INSURE_COMPANY_CHANNEL,
        GMT_CREATED,
        sum(JJL) JJL
    FROM claim_dws.DWS_CLAIM_COUNT_DAY where
     INSURE_COMPANY_CHANNEL not in ('平安产险_团','平安产险_个','众安暖哇_团','众安暖哇_个','泰康养老全渠道','太保苏州_雇主','太保苏州_医疗','平安产险_车险','平安产险_雇主','湖南医慧保')
        and coalesce(INSURE_COMPANY_CHANNEL,'')<>''
group by  GMT_CREATED  ),
    all_channel_fan as (
SELECT
        INSURE_COMPANY_CHANNEL,
        GMT_CREATED,
        JJL,
        LAG(JJL, 7) OVER (PARTITION BY INSURE_COMPANY_CHANNEL ORDER BY GMT_CREATED) AS JJL_1WEEK_AGO,
        LAG(JJL, 28) OVER (PARTITION BY INSURE_COMPANY_CHANNEL ORDER BY GMT_CREATED) AS JJL_4WEEKS_AGO
FROM all_channel),
    CTE AS (
    SELECT
        INSURE_COMPANY_CHANNEL,
        GMT_CREATED,
        JJL,
        LAG(JJL, 7) OVER (PARTITION BY INSURE_COMPANY_CHANNEL ORDER BY GMT_CREATED) AS JJL_1WEEK_AGO,
        LAG(JJL, 28) OVER (PARTITION BY INSURE_COMPANY_CHANNEL ORDER BY GMT_CREATED) AS JJL_4WEEKS_AGO
    FROM claim_dws.DWS_CLAIM_COUNT_DAY where INSURE_COMPANY_CHANNEL not in ('平安产险_团','平安产险_个','众安暖哇_团','众安暖哇_个','平安产险_车险','平安产险_雇主','泰康养老全渠道','太保苏州_雇主','太保苏州_医疗','湖南医慧保')

)

SELECT
    INSURE_COMPANY_CHANNEL,
    GMT_CREATED,
    JJL,
    coalesce(JJL-JJL_1WEEK_AGO,0) 一周前环比,
#     JJL_1WEEK_AGO,
    cast(case when JJL_1WEEK_AGO=0 then JJL else  coalesce((JJL-JJL_1WEEK_AGO)/JJL_1WEEK_AGO,0) end as decimal(10,4) ) 一周前环比增长率,
    coalesce(JJL-JJL_4WEEKS_AGO,0)  四周前环比,
#     JJL_4WEEKS_AGO,
    cast(case when JJL_4WEEKS_AGO=0 then JJL else coalesce((JJL-JJL_4WEEKS_AGO)/JJL_4WEEKS_AGO ,0) end as decimal(10,4) )  四周前环比增长率,
        replace(curdate(),'-','')
FROM all_channel_fan
union all
    SELECT
    case when INSURE_COMPANY_CHANNEL='泰康养老甘肃分公司' then '泰康养老甘肃-半流程' else INSURE_COMPANY_CHANNEL end  INSURE_COMPANY_CHANNEL,
    GMT_CREATED,
    JJL,
    coalesce(JJL-JJL_1WEEK_AGO,0) 一周前环比,
#     JJL_1WEEK_AGO,
    cast(case when JJL_1WEEK_AGO=0 then JJL else  coalesce((JJL-JJL_1WEEK_AGO)/JJL_1WEEK_AGO,0) end as decimal(10,4) )  一周前环比增长率,
    coalesce(JJL-JJL_4WEEKS_AGO,0)  四周前环比,
#     JJL_4WEEKS_AGO,
    cast(case when JJL_4WEEKS_AGO=0 then JJL else coalesce((JJL-JJL_4WEEKS_AGO)/JJL_4WEEKS_AGO ,0) end as decimal(10,4) ) 四周前环比增长率,
    replace(curdate(),'-','')
FROM CTE
;

"""
def truncate_table(table_name='CLAIM_DWS.DWS_CLAIM_JJL_SEQ'):
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
