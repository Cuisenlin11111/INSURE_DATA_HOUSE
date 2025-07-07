# import sys
# sys.path.append(r"E:\pycharm\database")
from database import DatabaseConnection
from datetime import datetime, date, timedelta


today = date.today()
# 计算7天前的日期
formatted_date = today.strftime("%Y%m%d")

# 计算昨天的日期
yesterday = today - timedelta(days=1)


sql_query = f"""
  -- @description: 案件数量统计月表
  -- @param:
  -- @return:
  -- @alter:
  -- @time: 2024-09-18 15:01:06
  -- @author: 01
  -- @version: 1.0.0
INSERT INTO CLAIM_DWS.DWS_CLAIM_COUNT_MONTH
SELECT
    t1.insure_company_channel,
    year_month,
    jjl,
    cancel_vol,
    xxqd,
    xsqd,
    lrl,
    drhcal,
    blchcal,
    qlchcal,
    hcsbal,
    zdshal,
    fh_auto_claim_count,
    question_claim_vol,
    hospatal_claim_vol,
    CAST(当月实际发生天数 AS INT),
    cast(avg_jjl as decimal(10,2)),
    cast(fh_auto_claim_rate as decimal(10,4)),
    cast(qlchcal_rate as decimal(10,4)),
    cast(ZDSHAL_rate as decimal(10,4)),
    cast(COALESCE(t3.SHZDHL_RW,0) as decimal(10,4)),
    CURRENT_TIMESTAMP,
    replace('{yesterday}','-','')
FROM (
SELECT
    insure_company_channel,
    substring(gmt_created,1,7) year_month,
    SUM(jjl) jjl,
    SUM(cancel_vol) cancel_vol,
    SUM(xxqd) xxqd,
    SUM(xsqd) xsqd,
    SUM(lrl) lrl,
    SUM(drhcal) drhcal,
    SUM(blchcal) blchcal,
    SUM(qlchcal) qlchcal,
    SUM(hcsbal) hcsbal,
    SUM(zdshal) zdshal,
    SUM(fh_auto_claim_count) fh_auto_claim_count,
    SUM(question_claim_vol) question_claim_vol,
    SUM(hospatal_claim_vol) hospatal_claim_vol,
    AVG(jjl) avg_jjl,
    CASE
        WHEN SUM(qlchcal) = 0 THEN 0
        ELSE CASE
            WHEN insure_company_channel = '泰康养老广东分公司' THEN
                SUM(fh_auto_claim_count) / (SUM(qlchcal) + SUM(blchcal))
            ELSE
                SUM(fh_auto_claim_count) / SUM(qlchcal)
        END
    END fh_auto_claim_rate,
    CASE
        WHEN SUM(drhcal) = 0 THEN 0
        ELSE SUM(qlchcal) / SUM(drhcal)
    END qlchcal_rate,
    CASE
        WHEN SUM(qlchcal) = 0 THEN 0
        ELSE CASE
            WHEN insure_company_channel = '泰康养老广东分公司' THEN
                SUM(zdshal) / (SUM(qlchcal) + SUM(blchcal))
            ELSE
                SUM(zdshal) / SUM(qlchcal)
        END
    END ZDSHAL_rate
FROM CLAIM_DWD.DWD_CLAIM_COUNT_DAY
GROUP BY insure_company_channel, substring(gmt_created,1,7)
) t1
JOIN (
    SELECT
        COUNT(*) 当月实际发生天数,
        dt_month
    FROM claim_ods.DIM_DATE_WEEK
    WHERE date_dt <= '{yesterday}'
      AND dt_week IN ('星期一', '星期二', '星期三', '星期四', '星期五')
    GROUP BY dt_month
) t2 ON year_month = dt_month
left join  (       SELECT
			a3.channel_value as insure_company_channel,
			a2.comm_date,
			CASE
				WHEN COALESCE(a2.totalNum, 0) = 0 THEN 0
				ELSE COALESCE(a1.num, 0) / COALESCE(a2.totalNum, 0)
			END AS SHZDHL_RW
		FROM
			(
				SELECT
					insure_company_channel,
					comm_date,
					sum(num) num
				FROM
				(
					SELECT
						CASE
							WHEN c.insure_company_channel = 'CP10' THEN alro.DEPARTMENT_CODE
							ELSE c.insure_company_channel
						END insure_company_channel,
						substr(cat.T_CRT_TM,1,7) comm_date,
						count(DISTINCT cat.C_CLAIM_CASE_NO) num
					FROM
						claim_ods.claim c
					LEFT JOIN claim_ods.case_audit_task cat
					ON cat.c_claim_case_no = c.claim_no AND cat.C_DEL_FLAG = '0' AND cat.C_HANDLE_CDE = '1'
					LEFT JOIN claim_ods.ACCEPT_LIST_RECORD alro
					ON c.ACCEPTANCE_NO = alro.accept_num AND alro.DEL_FLAG = '0'
					WHERE cat.T_CRT_TM IS NOT NULL
					GROUP BY c.insure_company_channel, substr(cat.T_CRT_TM,1,7), alro.DEPARTMENT_CODE
				)
				GROUP BY insure_company_channel, comm_date
		    ) a1
			RIGHT JOIN (
				SELECT
					insure_company_channel,
					comm_date,
					sum(totalNum) totalNum
				FROM
					(
						SELECT
							CASE
								WHEN c.insure_company_channel = 'CP10' THEN alro.DEPARTMENT_CODE
								ELSE c.insure_company_channel
							END insure_company_channel,
							substr(cat.T_CRT_TM,1,7) comm_date,
							count(DISTINCT cat.C_CLAIM_CASE_NO) totalNum
						FROM
							claim_ods.claim c
						LEFT JOIN claim_ods.case_audit_task cat
						ON cat.c_claim_case_no = c.claim_no AND cat.C_DEL_FLAG = '0'
						LEFT JOIN claim_ods.ACCEPT_LIST_RECORD alro
				        ON c.ACCEPTANCE_NO = alro.accept_num AND alro.DEL_FLAG = '0'
						WHERE cat.T_CRT_TM IS NOT NULL
						GROUP BY c.insure_company_channel, substr(cat.T_CRT_TM,1,7), alro.DEPARTMENT_CODE
			       )
				GROUP BY insure_company_channel, comm_date
			) a2
			 ON a1.comm_date = a2.comm_date AND a1.insure_company_channel = a2.insure_company_channel
		left  join   claim_ods.dim_insure_company_channel a3
		on  a2.insure_company_channel = a3.channel_key
			 WHERE nvl(a2.insure_company_channel,'')<>''  ) t3
     on t1.insure_company_channel = t3.insure_company_channel   and t1.year_month = t3.comm_date
     where t1.insure_company_channel <> ''

"""
def truncate_table(table_name='CLAIM_DWS.DWS_CLAIM_COUNT_MONTH'):
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
