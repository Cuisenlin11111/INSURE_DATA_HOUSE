# import sys
# sys.path.append(r"E:\pycharm\database")
from database import DatabaseConnection
from datetime import datetime



sql_query = f"""
  -- @description: 控制台汇总数据
  -- @param:
  -- @return:
  -- @alter:
  -- @time: 2024-06-20 15:01:06
  -- @author: 01
  -- @version: 1.0.0
insert  into claim_dwd.`dwd_claim_risk_analysis_total`
with  tmp_drhcal as (
		    select
	              pr.insure_company_channel,
	              count(distinct pr.app_no) as drhcal
	        from claim_ods.postback_record pr
	        LEFT JOIN claim_ods.ACCEPT_LIST_RECORD alro
				ON pr.accept_num = alro.accept_num AND alro.DEL_FLAG = '0'
	       where pr.back_status in ('2', '21')
	         and pr.is_deleted = 'N'
	         and pr.insure_company_channel not in
	             ('TK02', 'CP01', 'YX01', 'common')
	       and substr(back_time,1,10) = CURDATE()
	       group by pr.insure_company_channel
      union all
      ##泰康山东的案件比较特殊，回传了两次，一次回传电力，一次回传泰康  注意：  泰康山东的回传案件 加receiver ='I' 只保留回传泰康的量
      select
             insure_company_channel,
             count(distinct pr.app_no) as drhcal
        from claim_ods.postback_record pr
       where pr.back_status in ('2', '21')
         and pr.is_deleted = 'N'
         and insure_company_channel = 'TK02'
	   and  substr(back_time,1,10) = CURDATE()
       group by insure_company_channel
      union all
      ##宁波普惠
      select
             c.insure_company_channel,
             count(DISTINCT c.id) drhcal
        FROM claim_ods.accept_list_record a
        join claim_ods.claim c
          on a.accept_num = c.acceptance_no
         and c.delete_flag = '0'
        left join claim_ods.case_audit_task cat
          on cat.C_CLAIM_CASE_NO = c.claim_no
         and cat.C_DEL_FLAG = '0'
         and cat.C_SUB_STATUS = '79'
       where c.clm_process_status = '9'
         and c.insure_company_channel = 'YX01'
	and substr(cat.T_UPD_TM,1,10) = CURDATE()
       group by  c.insure_company_channel
      union all
      ##中智
      select
             c.insure_company_channel,
             count(DISTINCT c.id) as drhcal
        FROM claim_ods.accept_list_record a
        join claim_ods.claim c
          on a.accept_num = c.acceptance_no
         and c.delete_flag = '0'
        LEFT JOIN claim_ods.front_seq_record fr
          on fr.app_no = c.claim_no
         and fr.is_deleted = 'N'
       where
         c.insure_company_channel = 'CP01' AND fr.CREATE_TIME =CURDATE()
       group by  c.insure_company_channel),
 tmp_11 as (
		select  insure_company_channel,
	             substr(T_CRT_TIME,1,10)   T_CRT_TIME,
	             substr(ACCEPT_DATE,1,10)  ACCEPT_DATE,
		         accept_num,
	             ACCEPT_STATUS,
	             claim_source
  from claim_ods.accept_list_record
      where DEL_FLAG = '0'
    and insure_company_channel != 'common'),
    tmp_drjjl as (
	select
	insure_company_channel,
	count(*) jjl
	from tmp_11
 where  insure_company_channel = 'PA02'  and ACCEPT_STATUS<>'1'
 and T_CRT_TIME=current_date()
	group by insure_company_channel
	union all
    select
         insure_company_channel,
         COUNT(*) jjl
    from tmp_11
    where  insure_company_channel in ('DJ01','ZA01','CP08')
    and  ACCEPT_DATE=current_date()
    group by insure_company_channel
		union all
		 select
         insure_company_channel,
         COUNT(*) jjl
    from tmp_11
    where  insure_company_channel not in ('DJ01','ZA01','CP08','PA02')
		and ACCEPT_STATUS<>'1'  and   ACCEPT_DATE=current_date()
    group by insure_company_channel),

    tmp_drhcal_tj as (
select     t2.insure_company_channel,
    SUM(CASE WHEN is_end = 0 and node <> 6 and nvl(node,'')<>''  and is_deleted = 'N' THEN 1 ELSE 0 END) AS 待处理总,
    SUM(CASE WHEN is_end = 0 AND  node<>6 and  claim_dead_time <= DATE_ADD(NOW(), INTERVAL 33 HOUR) AND node IS NOT NULL  and is_deleted = 'N' THEN 1 ELSE 0 END) AS 今日待处理,
    SUM(CASE WHEN is_end = 0 AND time_effect_status = 2   and is_deleted = 'N' THEN 1 ELSE 0 END) AS 预警,
    SUM(CASE WHEN is_end = 0 AND time_effect_status = 3  and is_deleted = 'N' THEN 1 ELSE 0 END) AS 超时
        from   claim_ods.claim_console_record  t2
        GROUP BY t2.insure_company_channel)
select
    t1.CHANNEL_VALUE,
    t2.待处理总,
    t2.今日待处理,
    t2.预警,
    t2.超时,
    nvl(t4.jjl,0) as 当天进件量,
    nvl(t3.drhcal,0) as 当天完成量
from CLAIM_DIM.DIM_EFFECT_INSURE_CHANNEL   t1
left join  tmp_drhcal_tj  t2  on t1.CHANNEL_KEY=t2.insure_company_channel
left join  tmp_drhcal t3 on t1.CHANNEL_KEY=t3.insure_company_channel
left join  tmp_drjjl t4 on t1.CHANNEL_KEY=t4.insure_company_channel

"""
def truncate_table(table_name='CLAIM_DWD.dwd_claim_risk_analysis_total'):
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
