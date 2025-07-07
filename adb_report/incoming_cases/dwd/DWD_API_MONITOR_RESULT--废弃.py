# import sys
# sys.path.append(r"E:\pycharm\database")
from database import DatabaseConnection
from datetime import datetime, date, timedelta

today = date.today()
yesterday = today - timedelta(days=1)

now = datetime.now()
# 获取小时数
hour = now.hour
# print(hour)
if hour>8:
    fromdate = today
else:
    fromdate = yesterday

sql_query = f"""
  -- @description: api接口监控结果
  -- @param:
  -- @return:
  -- @alter:
  -- @time: 2024-08-29 15:01:06
  -- @author: 01
  -- @version: 1.0.0



insert into CLAIM_DWD.DWD_API_MONITOR_RESULT

with t as (  select   app_name,
      request_uri,
         substr(request_time,1,10)  request_time,
         count(1) as num,
         sum(case when response_status <> 200 then 1 else 0 end) as error_num,
         sum(case when response_status <> 200 then 1 else 0 end)/count(1) as error_rate,
         sum(time_taken)/count(1) as avg_time
      from
claim_ods.api_monitor
      where  substr(request_time,1,10) = '{fromdate}'
      group by app_name,request_uri,substr(request_time,1,10)),
      t1 as (SELECT request_uri,
       request_time_date,
        case when max(row_num) = CEIL(total_count*0.99) then max(time_taken) else '' end  99_time_taken,
       case when min(row_num) = CEIL(total_count*0.95) then min(time_taken) else '' end  95_time_taken
FROM
(
    SELECT request_uri,
           substr(request_time,1,10) AS request_time_date,
           time_taken,
           COUNT(*) OVER (PARTITION BY request_uri, substr(request_time,1,10)) AS total_count,
           ROW_NUMBER() OVER (PARTITION BY request_uri, substr(request_time,1,10) ORDER BY time_taken ASC) AS row_num  -- 计算每个分组内的行号
    FROM claim_ods.api_monitor
)
WHERE row_num = CEIL(total_count*0.99) or row_num = CEIL(total_count*0.95)
and request_time_date = '{fromdate}'
group by request_uri,
       request_time_date)
      select
         t.app_name,
         t.request_uri,
         t.request_time,
         cast(t.num as int),
         cast(t.error_num as int),
         cast(t.error_rate as DECIMAL(10,4)),
         cast(t.avg_time as DECIMAL(10,0)),
         t2.99_time_taken,
         t2.95_time_taken
      from t
        left join t1 as
 t2 on t.request_uri = t2.request_uri and  t.request_time = t2.request_time_date

"""
def truncate_table(table_name='CLAIM_DWD.DWD_API_MONITOR_RESULT'):
    with DatabaseConnection() as conn:
        truncate_sql = f"""  delete from   CLAIM_DWD.DWD_API_MONITOR_RESULT  where request_time= '{fromdate}' """
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
