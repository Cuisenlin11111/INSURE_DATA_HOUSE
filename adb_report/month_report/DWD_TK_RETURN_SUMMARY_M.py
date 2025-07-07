# import sys
#
# sys.path.append(r"E:\pycharm\database")
from database import DatabaseConnection
from datetime import datetime
from datetime import datetime, timedelta





sql_query = f"""
  -- @description: 泰康全渠道退回
  -- @param:
  -- @return:
  -- @alter:
  -- @time: 2024-06-20 15:01:06
  -- @author: 01
  -- @version: 1.0.0
INSERT INTO `CLAIM_DWD`.`DWD_TK_RETURN_SUMMARY_M`

SELECT
    CASE alr.INSURE_COMPANY_CHANNEL
        WHEN 'TK01' THEN '泰康上分'
        WHEN 'TK02' THEN '泰康电力'
        WHEN 'TK03' THEN '泰康浙分'
        WHEN 'TK04' THEN '泰康北分'
        WHEN 'TK05' THEN '泰康重分'
        WHEN 'TK06' THEN '泰康河南'
        WHEN 'TK07' THEN '泰康广分'
        WHEN 'TK08' THEN '泰康厦门'
        WHEN 'TK09' THEN '泰康江苏'
        WHEN 'TK10' THEN '泰康辽宁'
    END AS 渠道,
    t.gmt_created AS 退回时间,
    icrr.app_no AS 案件号,
    alr.accept_num AS 受理编号,
    REPLACE(JSON_EXTRACT(t.extra, '$."backCause"'),'"','') AS 退回原因,
    t.creator AS 退回人,'2024-12'
FROM claim_ods.insurance_company_review_record icrr
INNER JOIN claim_ods.accept_list_record alr ON icrr.accept_num = alr.accept_num AND alr.INSURE_COMPANY_CHANNEL LIKE 'TK%'
RIGHT JOIN claim_ods.operation_log t ON t.busi_no = icrr.app_no AND t.INSURE_COMPANY_CHANNEL LIKE 'TK%' AND t.oper_type = '16' AND t.sub_busi_type IN ('2')
LEFT JOIN claim_ods.postback_record pr ON pr.app_no = t.busi_no
WHERE icrr.INSURE_COMPANY_CHANNEL LIKE 'TK%'
    AND SUBSTR(pr.back_time, 1, 7) = '2024-12'
GROUP BY alr.accept_num
"""


def truncate_table(table_name='CLAIM_DWD.DWD_TK_RETURN_SUMMARY_M'):
    with DatabaseConnection() as conn:
        truncate_sql = f"delete  from {table_name} where date_month='2024-12'"
        with conn.cursor() as cursor:
            cursor.execute(truncate_sql)
            conn.commit()


def insert_data(sql_query):
    with DatabaseConnection() as conn:
        with conn.cursor() as cursor:
            cursor.execute(sql_query)
            conn.commit()


if __name__ == "__main__":
     # print(sql_query)
    #print(truncate_sql)
    start_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    print("程序开始时间：", start_time)
    truncate_table()
    insert_data(sql_query)
    end_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    print("程序结束时间：", end_time)
