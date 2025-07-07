# import sys
# sys.path.append(r"E:\pycharm\database")
from database import DatabaseConnection
from datetime import datetime, date, timedelta

from datetime import datetime, timedelta

# 获取今天的日期
today = datetime.now()
# 计算昨天的日期，通过减去一天的时间间隔（timedelta(days=1)）
yesterday = today - timedelta(days=1)
# 将日期格式化为指定的字符串格式（例如 '2024-11-20'）
formatted_yesterday = yesterday.strftime('%Y-%m-%d')



sql_query = f"""
  -- @description: 日案件量小时报表
  -- @param:
  -- @return:
  -- @alter:
  -- @time: 2024-11-21 15:01:06
  -- @author: 01
  -- @version: 1.0.0
insert into  CLAIM_DWD.DWD_CLAIM_COUNT_DAY_HOUR
WITH tmp_11 AS (
    SELECT
        CASE
            WHEN insure_company_channel = 'CP10' THEN DEPARTMENT_CODE
            ELSE insure_company_channel
        END AS insure_company_channel,
        T_CRT_TIME,
        accept_num,
        ACCEPT_STATUS,
        claim_source
    FROM claim_ods.accept_list_record
    WHERE DEL_FLAG = '0'
      AND insure_company_channel!= 'common'
    and substr(T_CRT_TIME, 1, 10) = '{formatted_yesterday}'
),
a1 AS (
    -- 第一个子查询部分
    SELECT
        insure_company_channel,
        count(*) AS jjl,
        substr(T_CRT_TIME, 1, 10) AS comm_date,
        substr(T_CRT_TIME, 12, 2) as hour
    FROM tmp_11
    WHERE
       ACCEPT_STATUS <> '1'
    and insure_company_channel is not null
    GROUP BY insure_company_channel, substr(T_CRT_TIME, 1, 10),substr(T_CRT_TIME, 12, 2)

),
a4 AS (
    SELECT
        substr(c.create_time, 1, 10) AS comm_date,
        substr(c.create_time, 12, 2) AS hour,
        a.insure_company_channel,
        count(*) AS lrl
    FROM tmp_11 a
    JOIN claim_ods.claim c ON a.accept_num = c.acceptance_no
    WHERE c.delete_flag = '0'
      AND substr(c.create_time, 1, 10) = '{formatted_yesterday}'
    GROUP BY substr(c.create_time, 1, 10),substr(c.create_time, 12, 2),
             a.insure_company_channel
),
a5 AS (
    SELECT
        substr(back_time, 1, 10) AS comm_date,
        substr(back_time, 12, 2) as hour,
        pr.insure_company_channel,
        count(distinct pr.app_no) AS drhcal
    FROM claim_ods.postback_record  pr
    WHERE pr.insure_company_channel NOT IN ('CP01', 'YX01', 'common')
      AND pr.back_status IN ('2', '21')
      AND receiver = 'I'
      AND substr(back_time, 1, 10) = '{formatted_yesterday}'
    GROUP BY substr(back_time, 1, 10),substr(back_time, 12, 2),
             pr.insure_company_channel
    UNION ALL
    -- 中智相关部分
    SELECT
        substr(fr.CREATE_TIME, 1, 10) AS comm_date,
        substr(fr.CREATE_TIME, 12, 2) as hour,
        c.insure_company_channel,
        count(DISTINCT c.id) AS drhcal
    FROM claim_ods.accept_list_record a
    JOIN claim_ods.claim c ON a.accept_num = c.acceptance_no
    AND c.delete_flag = '0'
    LEFT JOIN claim_ods.front_seq_record fr ON fr.app_no = c.claim_no
    AND fr.is_deleted = 'N'
    WHERE
        c.insure_company_channel = 'CP01'
        AND fr.CREATE_TIME IS NOT NULL
        AND substr(fr.CREATE_TIME, 1, 10) = '{formatted_yesterday}'
    GROUP BY substr(fr.CREATE_TIME, 1, 10), substr(fr.CREATE_TIME, 12, 2),
             c.insure_company_channel
),
a11 AS (
     	SELECT
        	insure_company_channel,
        	comm_date,
     	    hour,
        	SUM(问题件案件数) 问题件案件数
        FROM
        (
    		SELECT
    			 CASE
					WHEN c.insure_company_channel = 'CP10' THEN a.DEPARTMENT_CODE
					ELSE c.insure_company_channel
				 END insure_company_channel,
	             substr(q.gmt_created,1,10) comm_date,
            substr(q.gmt_created,12,2) as hour,
	             count(DISTINCT c.id) 问题件案件数
	        from claim_ods.accept_list_record a
	        join claim_ods.claim c
	          on a.accept_num = c.acceptance_no
	         and c.delete_flag = '0'
	        join claim_ods.question_claim q
	          on c.claim_no = q.claim_no
	        where substr(q.gmt_created,1,10) = '{formatted_yesterday}'
	       group by c.insure_company_channel,
	       substr(q.gmt_created,1,10),substr(q.gmt_created,12,2),
	       a.DEPARTMENT_CODE
        )
        GROUP BY insure_company_channel, comm_date

)
SELECT DISTINCT
    insure_company_channel,
    gmt_created,
    hour,
    jjl,
    lrl,
    drhcal,
    question_claim_vol,
    replace(substring(gmt_created, 1, 10), '-', '')
FROM (
    SELECT DISTINCT
        dc_dim.channel_value AS insure_company_channel,
        '{formatted_yesterday}' AS gmt_created,
        dc_dim.hour AS hour,
        coalesce(a1.jjl, 0) AS jjl,
        coalesce(a4.lrl, 0) AS lrl,
        coalesce(a5.drhcal, 0) AS drhcal,
        coalesce(a11.问题件案件数, 0) AS question_claim_vol
    FROM   `CLAIM_DIM`.`DIM_CHANNEL_HOUR`   dc_dim
    LEFT JOIN a1 ON  dc_dim.channel_key = a1.insure_company_channel  and  dc_dim.hour = a1.hour
    LEFT JOIN a4 ON  dc_dim.channel_key = a4.insure_company_channel  and dc_dim.hour=a4.hour
    LEFT JOIN a5 ON  dc_dim.channel_key = a5.insure_company_channel and dc_dim.hour=a5.hour
    LEFT JOIN a11 ON  dc_dim.channel_key = a11.insure_company_channel and dc_dim.hour=a11.hour
) order by  insure_company_channel   desc ,hour   asc

"""


def truncate_table(table_name='CLAIM_DWD.DWD_CLAIM_COUNT_DAY_HOUR'):
    with DatabaseConnection() as conn:
        truncate_sql = f"delete  from  {table_name}  where GMT_CREATED = '{formatted_yesterday}'  "
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
