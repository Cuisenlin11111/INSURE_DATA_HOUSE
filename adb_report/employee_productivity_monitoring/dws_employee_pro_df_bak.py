# import sys
# sys.path.append(r"E:\pycharm\database")
from database import DatabaseConnection
from datetime import datetime



sql_query = f"""
  -- @description: 业务员工时汇总备份数据
  -- @param:
  -- @return:
  -- @alter:
  -- @time: 2024-08-16 15:01:06
  -- @author: 01
  -- @version: 1.0.0
insert into claim_dws.dws_employee_pro_df_bak
select  *   from  claim_dws.dws_employee_pro_df;
"""


def insert_data(sql_query):
    with DatabaseConnection() as conn:
        with conn.cursor() as cursor:
            cursor.execute(sql_query)
            conn.commit()



if __name__ == "__main__":
    start_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    print("程序开始时间：", start_time)
    insert_data(sql_query)
    end_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    print("程序结束时间：", end_time)
