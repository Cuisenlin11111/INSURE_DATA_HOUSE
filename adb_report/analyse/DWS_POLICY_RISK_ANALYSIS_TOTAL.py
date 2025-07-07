# import sys
# sys.path.append(r"E:\pycharm\database")
from database import DatabaseConnection
from datetime import datetime



sql_query = f"""
  -- @description: 审核风控分析--保单汇总
  -- @param:
  -- @return:
  -- @alter:
  -- @time: 2024-06-27 15:01:06
  -- @author: 01
  -- @version: 1.0.0

  insert into CLAIM_DWS.DWS_POLICY_RISK_ANALYSIS_TOTAL
    select GROUP_POLICY_NO,
           COMM_DATE,
           C_RISK_REASON,
            count(CLAIM_NO)  cnt,
           replace(current_date,'-','') 
from CLAIM_DWS.DWS_EXAM_RISK_ANALYSIS_POLICY
where substr(COMM_DATE,1,4) >'2023'
 group by  GROUP_POLICY_NO,COMM_DATE,C_RISK_REASON
"""
def truncate_table(table_name='CLAIM_DWS.DWS_POLICY_RISK_ANALYSIS_TOTAL'):
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
