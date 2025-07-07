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
  -- @description: 撤销案件汇总表
  -- @param:
  -- @return:
  -- @alter:
  -- @time: 2024-11-19 15:01:06
  -- @author: 01
  -- @version: 1.0.0
insert into CLAIM_DWD.DWD_CANCEL_RESULT
SELECT
    alr.INSURE_COMPANY_CHANNEL,
    alr.ACCEPT_NUM 受理编号,
    c.CLAIM_NO 案件号,
    case when c.cancle_time is null then cast(alr.T_UPD_TIME as date) else cast(c.cancle_time as date) end as 撤案时间,
    alr.WITHDRAWAL_REASON 撤案原因
FROM claim_ods.accept_list_record alr
LEFT JOIN claim_ods.claim c on c.acceptance_no = alr.ACCEPT_NUM
WHERE
   alr.DEL_FLAG = '0'
  and (alr.ACCEPT_STATUS = '5' or c.clm_process_status = '11')
 and  (  substr(c.cancle_time, 1, 10)='{yesterday_date}'   or ( c.cancle_time is null and substr(alr.T_UPD_TIME, 1, 10)='{yesterday_date}' ))
"""
def truncate_table(table_name='CLAIM_DWD.DWD_CANCEL_RESULT'):
    with DatabaseConnection() as conn:
        truncate_sql = f"delete from  {table_name} where cancle_time = '{yesterday_date}';"
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
