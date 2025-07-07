# import sys
# sys.path.append(r"E:\pycharm\database")
from database import DatabaseConnection
from datetime import datetime, date, timedelta

# 获取当前日期时间
now = datetime.now()
# 计算 60 天前的日期时间
ago_60_days = now - timedelta(days=20)
# 格式化为指定的字符串格式
formatted_date = ago_60_days.strftime('%Y-%m-%d')



sql_query = f"""
  -- @description: 审核风控分析--保单信息
  -- @param:
  -- @return:
  -- @alter:
  -- @time: 2024-06-27 15:01:06
  -- @author: 01
  -- @version: 1.0.0

  insert into CLAIM_DWS.DWS_EXAM_RISK_ANALYSIS_POLICY
    select *    from   CLAIM_DWD.DWD_EXAM_RISK_ANALYSIS_POLICY where  COMM_DATE >= '{formatted_date}'
    and INSURE_COMPANY_CHANNEL   is  not  null
"""
def truncate_table(table_name='CLAIM_DWS.DWS_EXAM_RISK_ANALYSIS_POLICY'):
    with DatabaseConnection() as conn:
        truncate_sql = f"delete  from  {table_name}  where  COMM_DATE>='{formatted_date}'"
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
