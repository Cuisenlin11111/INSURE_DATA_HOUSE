# import sys
# sys.path.append(r"E:\pycharm\database")
from database import DatabaseConnection
from datetime import datetime, date, timedelta


today = date.today()
# 计算7天前的日期
formatted_date = today.strftime("%Y%m%d")
seven_days_ago = today - timedelta(days=7)
# 计算昨天的日期
yesterday = today - timedelta(days=1)



sql_query = f"""
  -- @description: 湖南医惠保报表
  -- @param:
  -- @return:
  -- @alter:
  -- @time: 2024-10-28 15:01:06
  -- @author: 01
  -- @version: 1.0.0
  insert  into  CLAIM_DWD.DWD_YH_CLAIM_DAY
    SELECT
        -- 第一个查询的结果列
        substr(alr.ACCEPT_DATE, 1, 10) AS 日期,
        CAST(count(distinct ACCEPT_NUM) AS INTEGER) AS 进件量,
        CAST(count(distinct case when INV.N_FINAL_PAY > 0 then   alr.ACCEPT_NUM  else '' end ) AS INTEGER) AS 理赔数量,
        CAST(sum(INV.N_FINAL_PAY) AS DECIMAL(10,2)) AS 赔付金额,
        CAST(sum(case when inv.C_RESPONSE_DESC='社保内医疗'  then inv.N_FINAL_PAY else 0 end )  AS DECIMAL(10,2))  AS 政策内赔付金额,
        CAST(sum(case when inv.C_RESPONSE_DESC='社保外医疗'  then inv.N_FINAL_PAY else 0 end )   AS DECIMAL(10,2))   AS 政策外赔付金额
    FROM
        claim_ods.accept_list_record alr
            left join  claim_ods.claim c on alr.ACCEPT_NUM=c.acceptance_no and c.delete_flag='0'
        LEFT JOIN claim_ods.clm_app_info cai
            ON c.claim_no = cai.C_CUSTOM_APP_NO AND cai.C_DEL_FLAG='0' AND cai.INSURANCE_COMPANY='YH01'
        LEFT JOIN claim_ods.clm_visit_inv_info inv
            ON inv.C_CUSTOM_APP_NO = cai.C_CUSTOM_APP_NO AND inv.C_PLY_NO = cai.C_PLY_NO AND inv.c_del_flag = '0' AND inv.C_BILL_TYP <> '3' AND inv.INSURANCE_COMPANY='YH01'
    WHERE
        alr.DEL_FLAG='0'
        AND alr.insure_company_channel='YH01'
    GROUP BY
        substr(alr.ACCEPT_DATE, 1, 10)
    ORDER BY
        substr(alr.ACCEPT_DATE, 1, 10) DESC
"""
def truncate_table(table_name='CLAIM_DWD.DWD_YH_CLAIM_DAY'):
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