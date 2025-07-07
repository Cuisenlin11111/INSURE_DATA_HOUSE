# import sys
# sys.path.append(r"E:\pycharm\database")
from database import DatabaseConnection
from datetime import datetime



sql_query = f"""
  -- @description: 案件数量统计日表
  -- @param:
  -- @return:
  -- @alter:
  -- @time: 2024-09-18 15:01:06
  -- @author: 01
  -- @version: 1.0.0
insert into CLAIM_DWS.DWS_CLAIM_COUNT_DAY
SELECT
    insure_company_channel,
    gmt_created,
    jjl,
    cancel_vol,
    xxqd,
    xsqd,
    lrl,
    drhcal,
    blchcal,
    qlchcal,
    perc_all_flow,
    hcsbal,
    zdshal,
    ZDSHAL_RW,
    shzdhl,
    SHZDHL_RW,
    fh_auto_claim_count,
    cast(case when fhzdhl>1 then 1 else fhzdhl end as decimal(10,4)) as fhzdhl,
    question_claim_vol,
    cast(case  when jjl=0 then 0  when question_claim_vol/jjl>=1 then 1 else question_claim_vol/jjl end as decimal(10,4)) as QUESTION_CLAIM_perct,
    hospatal_claim_vol,
    cast(case when jjl=0 then 0  when hospatal_claim_vol/jjl>= 1 then 1 else hospatal_claim_vol/jjl end as decimal(10,4))  as  HOSPATAL_CLAIM_perct,
    dim.week_qj,
    current_timestamp,
    replace(substring(gmt_created,1,10),'-',''),
    sh_claim_count
FROM CLAIM_DWD.DWD_CLAIM_COUNT_DAY
INNER JOIN CLAIM_ODS.DIM_DATE_WEEK  dim
ON GMT_CREATED = dim.date_dtd
where insure_company_channel<>''
union all
SELECT '泰康养老全渠道' INSURE_COMPANY_CHANNEL,
       GMT_CREATED,
       sum(JJL) JJL,
       sum(CANCEL_VOL),
       sum(XXQD),
       sum(XSQD),
       sum(LRL),
       sum(DRHCAL),
       sum(BLCHCAL),
       sum(QLCHCAL),
        cast(case when coalesce(sum(drhcal), 0) = 0 THEN 0
            when coalesce(sum(qlchcal), 0) = 0 THEN 0
        ELSE coalesce(sum(qlchcal), 0) / coalesce(sum(drhcal), 0) END as DECIMAL(10,4))  PERC_ALL_FLOW,
       sum(HCSBAL),
       sum(ZDSHAL),
       sum(ZDSHAL_RW),
       			cast(case  when coalesce(sum(qlchcal), 0) = 0  THEN 0
	             	ELSE coalesce(sum(zdshal), 0) / coalesce(sum(qlchcal), 0)  END    as DECIMAL(10,4)) shzdhl,
       cast(case when  sum(sh_claim_count) =0 then 0 else sum(ZDSHAL_RW)/sum(sh_claim_count)  end as DECIMAL(10,4)) SHZDHL_RW,
       sum(FH_AUTO_CLAIM_COUNT),
       	cast(case
		 		when coalesce(sum(qlchcal), 0) = 0 THEN 0
       	        when coalesce(sum(fh_auto_claim_count), 0) / coalesce(sum(qlchcal), 0) >= 1 THEN 1
	             	ELSE coalesce(sum(fh_auto_claim_count), 0) / coalesce(sum(qlchcal), 0)  END
		     as DECIMAL(10,4)) as fhzdhl,
       sum(QUESTION_CLAIM_VOL),
       cast(case  when sum(jjl)=0 then 0  when sum(question_claim_vol)/sum(jjl)>=1 then 1 else sum(question_claim_vol)/sum(jjl) end as decimal(10,4)) as QUESTION_CLAIM_perct,
       sum(HOSPATAL_CLAIM_VOL),
       cast(case when sum(jjl)=0 then 0  when sum(hospatal_claim_vol)/sum(jjl)>= 1 then 1 else sum(hospatal_claim_vol)/sum(jjl) end as decimal(10,4))  as  HOSPATAL_CLAIM_perct,
       dim.WEEK_QJ,
       current_timestamp,
       DATA_DT,
       sum(sh_claim_count)
FROM CLAIM_DWD.DWD_CLAIM_COUNT_DAY
INNER JOIN CLAIM_ODS.DIM_DATE_WEEK  dim
ON GMT_CREATED = dim.date_dtd
where
INSURE_COMPANY_CHANNEL like '%泰康%'
group by GMT_CREATED

"""
def truncate_table(table_name='CLAIM_DWS.DWS_CLAIM_COUNT_DAY'):
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
