# import sys
# sys.path.append(r"E:\pycharm\database")
from database import DatabaseConnection
from datetime import datetime

start_time = datetime.now().strftime("%H%M%S")

sql_query = f"""
create TABLE claim_ods.bill_diagnose_rule_match_task_0816_{start_time} as
select *
    from claim_ods.bill_diagnose_rule_match_task where substr(gmt_created,1,10)=current_date() 
"""

sql_query_a = f"""
create TABLE claim_ods.bill_0816_{start_time} as
select *
from claim_ods.bill where substr(create_time,1,10)=current_date()
"""


sql_query_b = f"""
create TABLE claim_ods.deduct_task_0816_{start_time} as
select *
from claim_ods.deduct_task  where  substr(update_time, 1,10)=current_date() ;
"""


sql_query_c = f"""
create TABLE claim_ods.manual_match_task_0816_{start_time} as
select *
from
claim_ods.manual_match_task   where  substr(deal_time,1,10)=current_date();
"""



sql_query_d = f"""
create TABLE claim_dws.dws_employee_comp_num_0816_{start_time} as
select  *   from  claim_dws.dws_employee_comp_num;
"""

sql_query_e = f"""
create TABLE claim_dws.dws_employee_pro_df_0816_{start_time} as
select  *   from  claim_dws.dws_employee_pro_df;
"""

def insert_data(sql_query):
    with DatabaseConnection() as conn:
        with conn.cursor() as cursor:
            cursor.execute(sql_query)
            conn.commit()



if __name__ == "__main__":
    start_time = datetime.now().strftime("%H%M%S")
    print("程序开始时间：", start_time)

    # insert_data(sql_query)
    # insert_data(sql_query_a)
    # insert_data(sql_query_b)
    insert_data(sql_query_c)
    insert_data(sql_query_d)
    # insert_data(sql_query_e)


    end_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    print("程序结束时间：", end_time)
