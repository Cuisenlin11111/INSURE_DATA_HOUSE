import sys
sys.path.append(r"D:\因朔桔智能科技有限公司\pycharm\pycharm")
from database import DatabaseConnection
from datetime import datetime
from datetime import datetime, timedelta

# 获取当前日期
today = datetime.today()
# 计算昨天的日期
yesterday = today - timedelta(days=1)

# 提取昨天的年份和月份，并格式化为 "YYYY-MM" 格式
formatted_date = yesterday.strftime('%Y-%m')


sql_query = f"""
  -- @description: 项目管理公摊月统计表
  -- @param:
  -- @return:
  -- @alter:
  -- @time: 2024-06-20 15:01:06
  -- @author: 01
  -- @version: 1.0.0
 INSERT INTO CLAIM_DWS.DWS_CLAIM_MANAGE_SHARE_M
with aa as (select DT_MONTH,
       sum(CLAIM_NUM) 回传案件量
from  CLAIM_DWS.DWS_CLAIM_INCOME_M
where DT_MONTH =  '{formatted_date}'
group by DT_MONTH order by DT_MONTH  desc),
    bb as (select DT_MONTH ,
       sum(CLAIM_OPERATE_VOL + CONFIG_VOL + CLAIM_STANDARD_VOL + CLAIM_AUDIT_VOL + CUSTOMER_VOL)   as 项目作业人数
from CLAIM_DIM.DIM_MANPOWER_CONFIG
where DT_MONTH =  '{formatted_date}'
group by DT_MONTH),
  cc as (
select aa.DT_MONTH,
       aa.回传案件量,
       bb.项目作业人数,
       aa.回传案件量/bb.项目作业人数 as 总人均产能
from aa
left join bb
on aa.DT_MONTH=bb.DT_MONTH)
      SELECT
       t2.DT_MONTH AS 年月,
       t2.回传案件量 案件量,
       cast(t2.总人均产能 as decimal(10,2)) as 总人均产能,
       cast(200000  as decimal(10,2))  意健TPA慧理赔管理公摊月,
       cast(200000 /回传案件量 as decimal(10,2)) 意健TPA慧理赔管理公摊案件,
       cast(333750 as decimal(10,2))   意健TPA公司管理公摊月,
       cast(333750 /回传案件量 as decimal(10,2)) 意健TPA公司管理公摊案件,
       REPLACE(CURDATE(),'-','')
    FROM cc t2

"""
def truncate_table(table_name='CLAIM_DWS.DWS_CLAIM_MANAGE_SHARE_M'):
    with DatabaseConnection() as conn:
        truncate_sql = f"delete  from   {table_name} where DT_MONTH= '{formatted_date}' "
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
