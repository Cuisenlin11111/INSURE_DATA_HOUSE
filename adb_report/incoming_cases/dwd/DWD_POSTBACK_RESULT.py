# import sys
# sys.path.append(r"E:\pycharm\database")
from database import DatabaseConnection
from datetime import datetime, timedelta

# 获取当前日期时间
today = datetime.now()
# 计算昨天的日期时间，通过减去一天的时间间隔（timedelta(days=1)）来实现
yesterday = today - timedelta(days=1)
# 将昨天的日期格式化为指定的字符串格式（例如：20241118）
result_date = yesterday.strftime('%Y%m%d')
yesterday_date = yesterday.strftime('%Y-%m-%d')


sql_query = f"""
  -- @description: 回传案件汇总表
  -- @param:
  -- @return:
  -- @alter:
  -- @time: 2024-11-19 15:01:06
  -- @author: 01
  -- @version: 1.0.0
insert into CLAIM_DWD.DWD_POSTBACK_RESULT
select pr.insure_company_channel,
       pr.accept_num,
       pr.app_no,
       pr.insurance_claim_no,
       pr.postback_way,
       pr.back_time,
       pr.back_status,
       cat.C_HANDLE_STAFF,
       cat.C_REVIEWER_STAFF,
       replace(substr(pr.back_time,1,10),'-','')
from  claim_ods.postback_record pr
left join  claim_ods.`case_audit_task` cat on pr.app_no = cat.C_CLAIM_CASE_NO and cat.C_DEL_FLAG = '0'
where pr.is_deleted='N'
and pr.back_status='2'
and pr.receiver='I'
and substr(pr.back_time,1,10)='{yesterday_date}';
"""
def truncate_table(table_name='CLAIM_DWD.DWD_POSTBACK_RESULT'):
    with DatabaseConnection() as conn:
        truncate_sql = f"delete from  {table_name} where data_dt = '{result_date}';"
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
