# import sys
# sys.path.append(r"E:\pycharm\database")
from database import DatabaseConnection
from datetime import datetime



sql_query = f"""
  -- @description: 案件退回差错率统计表
  -- @param:
  -- @return:
  -- @alter:
  -- @time: 2024-09-02 
  -- @author: 01
  -- @version: 1.0.0
insert into  `CLAIM_DWS`.`DWS_CLAIM_RETURN_CT`
select    aa.INSURE_COMPANY_CHANNEL,
          substr(aa.GMT_CREATED,1,10),
          count(case when OPER_TYPE='退回录入' then 1 end) as 退回录入,
          count(case when OPER_TYPE='退回扣费' then 1 end) as 退回扣费,
           count(case when OPER_TYPE='退回审核' then 1 end) as 退回审核,
          count(case when OPER_TYPE='保司复核退回审核' then 1 end) as 保司复核退回审核,
           count(case when OPER_TYPE='回传保司退回审核' then 1 end) as 回传保司退回审核,
           bb.DRHCAL,
          cast(case when bb.DRHCAL=0 then 0 else
               (coalesce(count(case when OPER_TYPE='回传保司退回审核' then 1 end),0)+coalesce(count(case when OPER_TYPE='保司复核退回审核' then 1 end),0) )/bb.DRHCAL end as decimal(10,4)) as CCL,
            replace(curdate(),'-','')
from  CLAIM_DWD.DWD_CLAIM_RETURN_REASON  aa
left join claim_dwd.DWD_CLAIM_COUNT_DAY bb on aa.INSURE_COMPANY_CHANNEL = bb.INSURE_COMPANY_CHANNEL  and substr(aa.GMT_CREATED,1,10)=bb.GMT_CREATED
group by aa.INSURE_COMPANY_CHANNEL,substr(aa.GMT_CREATED,1,10)
 ;

"""
def truncate_table(table_name='CLAIM_DWS.DWS_CLAIM_RETURN_CT'):
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
