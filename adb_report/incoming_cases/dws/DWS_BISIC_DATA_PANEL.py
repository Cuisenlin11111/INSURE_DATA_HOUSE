# import sys
# sys.path.append(r"E:\pycharm\database")
from database import DatabaseConnection
from datetime import datetime



sql_query = f"""
  --@description: 基础数据看板  
  --@param:
  --@return:
  --@alter:
  --@time: 2024-09-12 15:01:06
  --@author: 01
  --@version: 1.0.0
insert into CLAIM_DWS.DWS_BISIC_DATA_PANEL
    (    
    INSURE_COMPANY_CHANNEL,          -- 渠道
    CREATE_TIME,                     -- 时间
    TOTAL_CLAIM_COUNT,              -- 受理案件量
    TOTAL_MZ_CLAIM_COUNT,           -- 门诊案件量
    HOSPITAL_CLAIM_COUNT,           -- 住院案件数
    MZ_ONLINE_CLAIM_COUNT,          -- 门诊线上案件数
    MZ_OFFLINE_CLAIM_COUNT,         -- 门诊线下案件数
    ALL_FLOW_CLAIM_COUNT,           -- 全流程案件量
    HALF_FLOW_CLAIM_COUNT,          -- 半流程案件量
    TOTAL_HALF_ALL_FLOW,            -- 全流程和半流程案件量之和
    TOTAL_BILL_COUNT,               -- 总发票数
    MZ_BILL_COUNT,                  -- 门诊发票数
    ELEC_BILL_VOL,                  -- 电子发票数量
    ZY_BILL_COUNT,                  -- 住院发票数
    TOTAL_DETAIL_COUNT,             -- 总明细条数
    MZ_DETAIL_COUNT,                -- 门诊明细条数
    ZY_BILL_DETAIL_COUNT,           -- 住院发票明细数
    ELEC_BILL_CLAIM_VOL,            -- 电子发票案件数
    ALL_ELEC_BILL_CLAIM_VOL,        -- 全案电子发票案件数
    DATA_DT                         -- 调度日期
    )
    select d1.insure_company_channel,
           d1.create_time,
           d1.total_claim_count,
           d1.total_mz_claim_count,
           d1.hospital_claim_count,
           d1.mz_online_claim_count,
           d1.mz_offline_claim_count,
           d1.all_flow_claim_count,
           d1.half_flow_claim_count,
           d1.total_half_all_flow,
           coalesce(t1.total_bill_count, 0) total_bill_count,
           coalesce(t2.mz_bill_count, 0) mz_bill_count,
           coalesce(t2.elec_bill_vol, 0) elec_bill_vol,
           coalesce(t3.zy_bill_count, 0) zy_bill_count,
           coalesce(t4.total_detail_count, 0) total_detail_count,
           coalesce(t5.mz_detail_count, 0) mz_detail_count,
           coalesce(t6.zy_bill_detail_count, 0) zy_bill_detail_count,
           coalesce(t7.elec_bill_claim_vol, 0) elec_bill_claim_vol,
           coalesce(t7.all_elec_bill_claim_vol, 0) all_elec_bill_claim_vol,
           d1.DATA_DT
      from claim_dwd.DWD_BISIC_DATA_PANEL d1
      left join claim_dwd.DWD_BILL_PANEL_1 t1
        on d1.insure_company_channel = t1.insure_company_channel
       and d1.create_time = t1.create_time
      left join claim_dwd.DWD_BILL_PANEL_2 t2
        on d1.insure_company_channel = t2.insure_company_channel
       and d1.create_time = t2.create_time
      left join claim_dwd.DWD_BILL_PANEL_3 t3
        on d1.insure_company_channel = t3.insure_company_channel
       and d1.create_time = t3.create_time
      left join claim_dwd.DWD_BILL_PANEL_4 t4
        on d1.insure_company_channel = t4.insure_company_channel
       and d1.create_time = t4.create_time
      left join claim_dwd.DWD_BILL_PANEL_5 t5
        on d1.insure_company_channel = t5.insure_company_channel
       and d1.create_time = t5.create_time
      left join claim_dwd.DWD_BILL_PANEL_6 t6
        on d1.insure_company_channel = t6.insure_company_channel
       and d1.create_time = t6.create_time
      left join claim_dwd.DWD_BILL_PANEL_7 t7
        on d1.insure_company_channel = t7.insure_company_channel
       and d1.create_time = t7.create_time;
"""
def truncate_table(table_name='CLAIM_DWS.DWS_BISIC_DATA_PANEL'):
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
