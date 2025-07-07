# import sys
# sys.path.append(r"E:\pycharm\database")
from database import DatabaseConnection
from datetime import datetime



sql_query = f"""
  -- @description: 业务员工时统计
  -- @param:
  -- @return:
  -- @alter:
  -- @time: 2024-08-29 15:01:06
  -- @author: 01
  -- @version: 1.0.0
 

    insert into CLAIM_DWD.DWD_PERSON_WORK_VOL
    (insure_company_channel,
     comm_time,
     full_name,
     pre_audit_num,
     hostipal_match_num,
     diag_match_num,
     detail_match_num,
     charg_claim_num,
     charg_bill_vol,
     config_claim_num,
     audit_claim_num,
     fh_claim_num,
	 zdgz_num,
     HOS_NUM ,
     MED_NUM,
     TREAT_NUM ,
     MATE_NUM ,
     MED_INS_NUM,cancel_NUM,
     data_dt)
     select dim.channel_value as insure_company_channel,
           abc.comm_time,
           abc.full_name,
           sum(预审案件数) pre_audit_num,
           sum(医院匹配条数) hostipal_match_num,
           sum(诊断匹配条数) diag_match_num,
           sum(明细匹配条数) detail_match_num,
           sum(扣费案件数) charg_claim_num,
           sum(扣费发票数) charg_bill_vol,
           sum(配置案件数) config_claim_num,
           sum(审核案件数) audit_claim_num,
           sum(复核案件数) fh_claim_num,
		   sum(诊断规则数) zdgz_num,
		     sum(医院条)  HOS_NUM ,
         sum(药品条) MED_NUM,
         sum(诊疗条)  TREAT_NUM ,
         sum(材料条)  MATE_NUM ,
         sum(医保比例条)  MED_INS_NUM,
         sum(撤案量) cancel_NUM ,
           replace(comm_time,'-','')
      from ( select alr.insure_company_channel,
                   substr(e.update_time,1,10) comm_time,
                   u.full_name,
                   count(DISTINCT c.id) 预审案件数,
                   0 医院匹配条数,
                   0 诊断匹配条数,
                   0 明细匹配条数,
                   0 扣费案件数,
                   0 扣费发票数,
                   0 配置案件数,
                   0 审核案件数,
                   0 复核案件数,
				   0 诊断规则数,
				   0  医院条,
                   0  药品条,
                   0  诊疗条,
                   0  材料条,
                   0 医保比例条,0  撤案量
              from claim_ods.accept_list_record alr
              join claim_ods.claim c
                on alr.accept_num = c.acceptance_no
              join claim_ods.clm_pretrial_examine e
                on c.claim_no = e.claim_app_no
              left join claim_ods.sys_users u
                on e.allot_operator_no = u.id
             where e.app_state != '0'
             group by alr.insure_company_channel,
                      substr(e.update_time,1,10),
                      u.full_name,
                      e.allot_operator_no

            union all

					 select
								 mmt.insure_company_channel,
								 substr(mmt.deal_time, 1,10) comm_time,
								 u.full_name,
								0 预审案件数,
								 count(*) 医院匹配条数,
								 0 诊断匹配条数,
								 0 明细匹配条数,
								 0 扣费案件数,
								 0 扣费发票数,
								 0 配置案件数,
								 0 审核案件数,
								 0 复核案件数,
								 0 诊断规则数,
                                0  医院条,
                                0  药品条,
                                0  诊疗条,
                                0  材料条,
                                0 医保比例条,0  撤案量
					from claim_ods.manual_match_task mmt
					left join  claim_ods.sys_users u ON mmt.match_ower = u.id
						where   mmt.match_type = '1'  and mmt.match_status = '2'
group by u.full_name, mmt.insure_company_channel,substr(mmt.deal_time, 1,10)

            union all

						select
									 mmt.insure_company_channel,
									 substr(mmt.deal_time, 1,10) comm_time,
									 u.full_name,
										0 预审案件数,
									 0 医院匹配条数,
									 count(*) 诊断匹配条数,
									 0 明细匹配条数,
									 0 扣费案件数,
									 0 扣费发票数,
									 0 配置案件数,
									 0 审核案件数,
									 0 复核案件数,
									 0 诊断规则数,
                                    0  医院条,
                                    0  药品条,
                                    0  诊疗条,
                                    0  材料条,
                                    0 医保比例条,0  撤案量
						from claim_ods.manual_match_task mmt
						left join  claim_ods.sys_users u ON mmt.match_ower = u.id
							where   mmt.match_type = '2'  and mmt.match_status = '2'
group by u.full_name, mmt.insure_company_channel,substr(mmt.deal_time, 1,10)

            union all

						select
									 mmt.insure_company_channel,
									 substr(mmt.deal_time, 1,10) comm_time,
									 u.full_name,
										0 预审案件数,
									 0 医院匹配条数,
									 0 诊断匹配条数,
									 count(*) 明细匹配条数,
									 0 扣费案件数,
									 0 扣费发票数,
									 0 配置案件数,
									 0 审核案件数,
									 0 复核案件数,
									 0 诊断规则数,
                                    0  医院条,
                                    0  药品条,
                                    0  诊疗条,
                                    0  材料条,
                                    0 医保比例条,0  撤案量
						from claim_ods.manual_match_task mmt
						left join  claim_ods.sys_users u ON mmt.match_ower = u.id
							where   mmt.match_type = '3'  and mmt.match_status = '2'
group by u.full_name, mmt.insure_company_channel,substr(mmt.deal_time, 1,10)

            union all

            select alr.insure_company_channel,
                   substr(d.update_time,1,10) comm_time,
                   u.full_name,
                   0 预审案件数,
                   0 医院匹配条数,
                   0 诊断匹配条数,
                   0 明细匹配条数,
                   count(DISTINCT c.id) 扣费案件数,
                   0 扣费发票数,
                   0 配置案件数,
                   0 审核案件数,
                   0 复核案件数,
				    0 诊断规则数,
                    0  医院条,
                    0  药品条,
                    0  诊疗条,
                    0  材料条,
                    0 医保比例条,0  撤案量
              from claim_ods.accept_list_record alr
              join claim_ods.claim c
                on alr.accept_num = c.acceptance_no
              join claim_ods.deduct_task d
                on c.id = d.claim_id
              left join claim_ods.sys_users u
                on d.deduct_owner = u.id
             where d.deduct_status = 2
             group by d.deduct_owner,
                      alr.insure_company_channel,
                      substr(d.update_time,1,10),
                      u.full_name

            union all

            select alr.insure_company_channel,
                   substr(d.update_time,1,10) comm_time,
                   u.full_name,
                   0 预审案件数,
                   0 医院匹配条数,
                   0 诊断匹配条数,
                   0 明细匹配条数,
                   0 扣费案件数,
                   count(b.id) 扣费发票数,
                   0 配置案件数,
                   0 审核案件数,
                   0 复核案件数,
				    0 诊断规则数,
                    0  医院条,
                    0  药品条,
                    0  诊疗条,
                    0  材料条,
                    0 医保比例条,0  撤案量
              from claim_ods.accept_list_record alr
              join claim_ods.claim c
                on alr.accept_num = c.acceptance_no
              join claim_ods.deduct_task d
                on c.id = d.claim_id
              left join claim_ods.bill b
                on b.claim_id = c.id
              left join claim_ods.sys_users u
                on d.deduct_owner = u.id
             where d.deduct_status = 2
               and ( b.person_flag = 'Y' or b.reason_notes is not null )  and b.delete_flag = '0'
             group by d.deduct_owner,
                      alr.insure_company_channel,
                      substr(d.update_time,1,10),
                      u.full_name

            union all

            select t.insure_company_channel,
                   substr(t.gmt_modified,1,10) comm_time,
                   t.modifier as full_name,
                   0 预审案件数,
                   0 医院匹配条数,
                   0 诊断匹配条数,
                   0 明细匹配条数,
                   0 扣费案件数,
                   0 扣费发票数,
                   count(DISTINCT t.accept_num) 配置案件数,
                   0 审核案件数,
                   0 复核案件数,
				    0 诊断规则数,
                    0  医院条,
                    0  药品条,
                    0  诊疗条,
                    0  材料条,
                    0 医保比例条,0  撤案量
              from claim_ods.special_config_task t
             where t.config_flag = 'config'
               and t.modifier != 'system'
               and t.task_state != '0'
             group by t.modifier,
                      t.insure_company_channel,
                      substr(t.gmt_modified, 1,10)

            union all

            select c.insure_company_channel,
                   substr(cat.T_AUDIT_END_TM, 1,10) comm_time,
                   cat.C_HANDLE_STAFF as full_name,
                   0 预审案件数,
                   0 医院匹配条数,
                   0 诊断匹配条数,
                   0 明细匹配条数,
                   0 扣费案件数,
                   0 扣费发票数,
                   0 配置案件数,
                   count(DISTINCT c.id) 审核案件数,
                   0 复核案件数,
				    0 诊断规则数,
                    0  医院条,
                    0  药品条,
                    0  诊疗条,
                    0  材料条,
                    0 医保比例条,0  撤案量
              FROM claim_ods.claim c
              LEFT JOIN claim_ods.case_audit_task cat
                on cat.C_CLAIM_CASE_NO = c.claim_no
               and cat.C_DEL_FLAG = '0'
               and cat.C_HANDLE_CDE != '1'
             WHERE cat.C_ALLOT_TM is not null
               and cat.T_AUDIT_END_TM is not null
             group by cat.C_HANDLE_STAFF,
                      c.insure_company_channel,
                      substr(cat.T_AUDIT_END_TM, 1,10)

            union all

            SELECT c.insure_company_channel,
                   substr(cat.T_RECHECK_TM,1,10) comm_time,
                   cat.C_REVIEWER_STAFF as full_name,
                   0 预审案件数,
                   0 医院匹配条数,
                   0 诊断匹配条数,
                   0 明细匹配条数,
                   0 扣费案件数,
                   0 扣费发票数,
                   0 配置案件数,
                   0 审核案件数,
                   count(DISTINCT c.id) 复核案件数,
				    0 诊断规则数,
                    0  医院条,
                    0  药品条,
                    0  诊疗条,
                    0  材料条,
                    0 医保比例条,0  撤案量
              from claim_ods.claim c
              JOIN claim_ods.case_audit_task cat
                on cat.C_CLAIM_CASE_NO = c.claim_no
               and cat.C_DEL_FLAG = '0'
             WHERE cat.C_REVIEWER_STAFF != '系统自动'
             group by cat.C_REVIEWER_STAFF,
                      c.insure_company_channel,
                      substr(cat.T_RECHECK_TM,1,10)
			union all
			    			 SELECT ff.insure_company_channel,
           substr(ff.gmt_modified,1,10) comm_time,
           ff.modifier as full_name,
		   0 预审案件数,
		   0 医院匹配条数,
		   0 诊断匹配条数,
		   0 明细匹配条数,
		   0 扣费案件数,
		   0 扣费发票数,
		   0 配置案件数,
		   0 审核案件数,
		   0 复核案件数,
        COUNT(DISTINCT b.bill_no) AS 诊断规则数,
        0  医院条,
        0  药品条,
        0  诊疗条,
        0  材料条,
        0 医保比例条,0  撤案量
    FROM claim_ods.bill_diagnose_rule_match_task ff
LEFT JOIN claim_ods.bill b ON b.claim_id = ff.claim_id
    WHERE ff.is_created_task = 'Y'
        AND ff.modifier <> 'system'  AND b.delete_flag = '0'
    GROUP BY ff.insure_company_channel,substr(ff.gmt_modified,1,10), ff.modifier

    union all

                          SELECT
            substr(app_no,1,4)  INSURE_COMPANY_CHANNEL,
             substr(recheck_time,1,10) comm_time,
        case when reviewer is null or reviewer ='' then 0 else reviewer end full_name,
                                0 预审案件数,
        						 0 医院匹配条数,
								 0 诊断匹配条数,
								 0 明细匹配条数,
								 0 扣费案件数,
								 0 扣费发票数,
								 0 配置案件数,
								 0 审核案件数,
								 0 复核案件数,
								 0 诊断规则数,
        sum(case when recheck_type = 1 then 1 else 0 end) 医院条,
        sum(case when recheck_type = 2 then 1 else 0 end) 药品条,
        sum(case when recheck_type = 3 then 1 else 0 end) 诊疗条,
        sum(case when recheck_type = 4 then 1 else 0 end) 材料条,
                                 0 医保比例条,0  撤案量
        FROM claim_ods.base_data_recheck
        where recheck_status = 2
            GROUP BY substr(app_no,1,4) , reviewer,substr(recheck_time,1,10)

        union all

                SELECT  rr.INSURE_COMPANY_CHANNEL,
                substr(review_time,1,10) comm_time,
                                u.full_name,
                                0 预审案件数,
                				0 医院匹配条数,
								 0 诊断匹配条数,
								 0 明细匹配条数,
								 0 扣费案件数,
								 0 扣费发票数,
								 0 配置案件数,
								 0 审核案件数,
								 0 复核案件数,
								 0 诊断规则数,
                                0 医院条,
                                0 药品条,
                                0 诊疗条,
                                0 材料条,
        count(rr.id) 医保比例条,0  撤案量
        FROM claim_ods.recheck_ratio rr
                      left join claim_ods.sys_users u
                on rr.reviewer_id = u.id
        where check_status in ('2','3')
        group by  rr.INSURE_COMPANY_CHANNEL,substr(review_time,1,10),reviewer_id

            union all
                        select
    alr.insure_company_channel,
    case when c.cancle_time is null then cast(alr.T_UPD_TIME as date) else cast(c.cancle_time as date) end as comm_time,
    if(su.full_name is null ,alr.C_UPD_CDE,su.full_name) full_name,
                                    0 预审案件数,
                				0 医院匹配条数,
								 0 诊断匹配条数,
								 0 明细匹配条数,
								 0 扣费案件数,
								 0 扣费发票数,
								 0 配置案件数,
								 0 审核案件数,
								 0 复核案件数,
								 0 诊断规则数,
                                0 医院条,
                                0 药品条,
                                0 诊疗条,
                                0 材料条,
                                0  医保比例条,
    count(distinct alr.ACCEPT_NUM) 撤案量
FROM claim_ods.accept_list_record alr
LEFT JOIN claim_ods.claim c on c.acceptance_no = alr.ACCEPT_NUM
 left join claim_ods.sys_users su on  alr.C_UPD_CDE = su.id
WHERE  alr.DEL_FLAG = '0'
  and (alr.ACCEPT_STATUS = '5' or c.clm_process_status = '11')
and  if(su.full_name is null ,alr.C_UPD_CDE,su.full_name)  not  in  ('0','33','6','368','null','','标准化系统管理员','系统自动')
GROUP BY alr.insure_company_channel,
case when c.cancle_time is null then cast(alr.T_UPD_TIME as date) else cast(c.cancle_time as date) end,
if(su.full_name is null ,alr.C_UPD_CDE,su.full_name)
			) abc
     inner join claim_ods.dim_insure_company_channel dim
        on abc.insure_company_channel = dim.channel_key
              where  ((abc.`FULL_NAME` <> '标准化系统管理员') AND (COALESCE(abc.`FULL_NAME`, '') <> ''))
     group by dim.channel_value, abc.comm_time, abc.full_name;

"""
def truncate_table(table_name='CLAIM_DWD.DWD_PERSON_WORK_VOL'):
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
