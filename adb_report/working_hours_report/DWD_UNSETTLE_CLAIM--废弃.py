# import sys
# sys.path.append(r"E:\pycharm\database")
from database import DatabaseConnection
from datetime import datetime



sql_query = f"""
  -- @description: 未决案件统计
  -- @param:
  -- @return:
  -- @alter:
  -- @time: 2024-08-29 15:01:06
  -- @author: 01
  -- @version: 1.0.0
INSERT INTO CLAIM_DWD.DWD_UNSETTLE_CLAIM
                    (insure_company_channel,
                     fail_download,
                     lr_dfp,
                     dlr_vol,
                     dys_vol,
                     dyypp_vol,
                     dyypiaj_vol,
                     djbpi_vol,
                     djbppaj_vol,
                     dmxpi_vol,
                     dmxpiaj_vol,
                     dkf_vol,
                     dkfaj_vol,
                     dshaj_vol,
                     dfhaj_vol,
                     gqwtaj_vol,
                     thlraj_vol,
                     dpzrws,
                     dzlszajs,
                     hckzajs,
                     dhcajs,
                     hcsbajs,
                     hczajs,
                     data_dt)
WITH t1 AS
     (SELECT a.insure_company_channel, COUNT(*) fail_download
        FROM claim_ods.accept_list_record a
       WHERE ACCEPT_STATUS IN ('1')
         AND DATE(a.T_CRT_TIME) >= CURDATE() - INTERVAL 3 DAY
         AND a.T_CRT_TIME < NOW() - INTERVAL 10 MINUTE
       GROUP BY a.insure_company_channel),
    t2 AS
     (SELECT a.insure_company_channel, COUNT(*) lr_dfp
        FROM claim_ods.accept_list_record a
        JOIN claim_ods.image_assign_task d
          ON a.accept_batch_no = d.accept_batch_no
       WHERE d.status = '01'
         AND DATE(a.T_CRT_TIME) >= CURDATE() - INTERVAL 89 DAY
       GROUP BY a.insure_company_channel),
    t3 AS
     (SELECT COUNT(*) dlr_vol, a.insure_company_channel
        FROM claim_ods.accept_list_record a
        LEFT JOIN claim_ods.claim c
          ON a.accept_num = c.acceptance_no
         AND c.delete_flag = '0'
       WHERE a.accept_status NOT IN ('1', '5')
         AND DATE(a.T_CRT_TIME) >= CURDATE() - INTERVAL 89 DAY
         AND c.id IS NULL
         AND a.del_flag = '0'
       GROUP BY a.insure_company_channel),
    t4 AS
     (SELECT c.insure_company_channel, COUNT(DISTINCT c.id) dys_vol
        FROM claim_ods.accept_list_record a
        LEFT JOIN claim_ods.claim c
          ON a.accept_num = c.acceptance_no
         AND c.delete_flag = '0'
        JOIN claim_ods.clm_pretrial_examine e
          ON c.claim_no = e.claim_app_no
       WHERE e.app_state!= '3'
         AND DATE(a.T_CRT_TIME) >= CURDATE() - INTERVAL 89 DAY
       GROUP BY c.insure_company_channel),
    t5 AS
     (SELECT c.insure_company_channel, COUNT(DISTINCT b.id) dyypp_vol
        FROM claim_ods.manual_match_task t
        JOIN claim_ods.claim c
          ON t.claim_id = c.id
        JOIN claim_ods.bill b
          ON c.id = b.claim_id
       WHERE DATE(c.create_time) >= CURDATE() - INTERVAL 89 DAY
         AND match_type = 1
         AND match_status!= '2'
       GROUP BY c.insure_company_channel),
    t6 AS
     (SELECT c.insure_company_channel, COUNT(DISTINCT c.id) dyypiaj_vol
        FROM claim_ods.manual_match_task t
        JOIN claim_ods.claim c
          ON t.claim_id = c.id
        JOIN claim_ods.bill b
          ON c.id = b.claim_id
       WHERE DATE(c.create_time) >= CURDATE() - INTERVAL 89 DAY
         AND match_type = 1
         AND match_status!= '2'
       GROUP BY c.insure_company_channel),
    t7 AS
     (SELECT c.insure_company_channel, COUNT(DISTINCT b.id) djbpi_vol
        FROM claim_ods.manual_match_task t
        JOIN claim_ods.claim c
          ON t.claim_id = c.id
        JOIN claim_ods.bill b
          ON c.id = b.claim_id
       WHERE DATE(c.create_time) >= CURDATE() - INTERVAL 89 DAY
         AND match_type = 2
         AND match_status!= '2'
       GROUP BY c.insure_company_channel),
    t8 AS
     (SELECT c.insure_company_channel, COUNT(DISTINCT c.id) djbppaj_vol
        FROM claim_ods.manual_match_task t
        JOIN claim_ods.claim c
          ON t.claim_id = c.id
        JOIN claim_ods.bill b
          ON c.id = b.claim_id
       WHERE DATE(c.create_time) >= CURDATE() - INTERVAL 89 DAY
         AND match_type = 2
         AND match_status!= '2'
       GROUP BY c.insure_company_channel),
    t9 AS
     (SELECT c.insure_company_channel, COUNT(DISTINCT b.id) dmxpi_vol
        FROM claim_ods.manual_match_task t
        JOIN claim_ods.claim c
          ON t.claim_id = c.id
        JOIN claim_ods.bill b
          ON c.id = b.claim_id
       WHERE DATE(c.create_time) >= CURDATE() - INTERVAL 89 DAY
         AND match_type = 3
         AND match_status!= '2'
       GROUP BY c.insure_company_channel),
    t10 AS
     (SELECT c.insure_company_channel, COUNT(DISTINCT c.id) dmxpiaj_vol
        FROM claim_ods.manual_match_task t
        JOIN claim_ods.claim c
          ON t.claim_id = c.id
        JOIN claim_ods.bill b
          ON c.id = b.claim_id
       WHERE DATE(c.create_time) >= CURDATE() - INTERVAL 89 DAY
         AND match_type = 3
         AND match_status!= '2'
       GROUP BY c.insure_company_channel),
    t11 AS
     (SELECT c.insure_company_channel, COUNT(DISTINCT b.id) dkf_vol
        FROM claim_ods.claim c
        JOIN claim_ods.deduct_task d
          ON c.id = d.claim_id
         AND c.delete_flag = '0'
        JOIN claim_ods.bill b
          ON b.claim_id = c.id
         AND b.delete_flag = '0'
       WHERE d.deduct_status!= '2'
         AND DATE(c.create_time) >= CURDATE() - INTERVAL 89 DAY
       GROUP BY c.insure_company_channel),
    t12 AS
     (SELECT c.insure_company_channel, COUNT(DISTINCT c.id) dkfaj_vol
        FROM claim_ods.claim c
        JOIN claim_ods.deduct_task d
          ON c.id = d.claim_id
         AND c.delete_flag = '0'
        JOIN claim_ods.bill b
          ON b.claim_id = c.id
         AND b.delete_flag = '0'
       WHERE d.deduct_status!= '2'
         AND DATE(c.create_time) >= CURDATE() - INTERVAL 89 DAY
       GROUP BY c.insure_company_channel),
    t13 AS
     (SELECT c.insure_company_channel,
             COUNT(DISTINCT cat.C_CLAIM_CASE_NO) dshaj_vol
        FROM claim_ods.claim c
        JOIN claim_ods.case_audit_task cat
          ON c.claim_no = cat.C_CLAIM_CASE_NO
       WHERE cat.C_DEL_FLAG = '0'
         AND (cat.C_HANDLE_CDE!= '1' OR cat.C_HANDLE_CDE IS NULL)
         AND cat.C_MAIN_STATUS = '4'
         AND cat.C_SUB_STATUS!= '49'
         AND c.clm_process!= '60'
         AND c.is_hang_up NOT IN ('2', '4')
         AND c.clm_process_status = '6'
         AND DATE(cat.T_CRT_TM) >= CURDATE() - INTERVAL 89 DAY
       GROUP BY c.insure_company_channel),
    t14 AS
     (SELECT c.insure_company_channel,
             COUNT(DISTINCT cat.C_CLAIM_CASE_NO) dfhaj_vol
        FROM claim_ods.claim c
        JOIN claim_ods.case_audit_task cat
          ON c.claim_no = cat.C_CLAIM_CASE_NO
       WHERE cat.C_DEL_FLAG = '0'
         AND (cat.C_REVIEWER_STAFF!= '系统自动' OR cat.C_REVIEWER_STAFF IS NULL)
         AND cat.C_MAIN_STATUS = '5'
         AND cat.C_SUB_STATUS!= '59'
         AND cat.C_MAIN_STATUS = '5'
         AND cat.C_SUB_STATUS!= '59'
         AND c.clm_process_status = '7'
         AND c.clm_process!= '70'
         AND c.is_hang_up NOT IN ('2', '4')
         AND DATE(cat.T_CRT_TM) >= CURDATE() - INTERVAL 89 DAY
       GROUP BY c.insure_company_channel),
    t15 AS
     (SELECT c.insure_company_channel, COUNT(DISTINCT c.claim_no) gqwtaj_vol
        FROM claim_ods.claim c
       WHERE c.delete_flag = '0'
         AND c.is_hang_up IN ('2', '4')
         AND DATE(c.create_time) >= CURDATE() - INTERVAL 89 DAY
       GROUP BY c.insure_company_channel),
    t16 AS
     (SELECT c.insure_company_channel, COUNT(DISTINCT c.claim_no) thlraj_vol
        FROM claim_ods.claim c
       WHERE c.delete_flag = '0'
         AND c.is_hang_up = '4'
         AND DATE(c.create_time) >= CURDATE() - INTERVAL 89 DAY
       GROUP BY c.insure_company_channel),
    t17 AS
     (SELECT t.insure_company_channel, COUNT(DISTINCT t.id) dpzrws
        FROM claim_ods.special_config_task t
       WHERE t.config_flag = 'config'
         AND t.task_state NOT IN ('2', '3')
         AND is_deleted = 'N'
         AND DATE(t.gmt_created) >= CURDATE() - INTERVAL 89 DAY
       GROUP BY t.insure_company_channel),
    t18 AS
     (SELECT a.insure_company_channel, COUNT(DISTINCT a.accept_num) dzlszajs
        FROM claim_ods.accept_list_record a
        LEFT JOIN claim_ods.claim c
          ON a.accept_num = c.acceptance_no
         AND c.delete_flag = '0'
        LEFT JOIN claim_ods.clm_process cp0
          ON c.claim_no = cp0.C_CLAIM_APPLY_NO
         AND cp0.C_PROCESS_STATUS = '2'
       WHERE c.clm_process_status IN ('3', '4', '5')
         AND DATE(a.t_crt_time) >= CURDATE() - INTERVAL 89 DAY
       GROUP BY a.insure_company_channel),
    t19 AS
     (SELECT COUNT(*) hckzajs, c.insure_company_channel
        FROM claim_ods.claim c
        JOIN claim_ods.postback_record pr
          ON pr.app_no = c.claim_no
         AND pr.is_deleted = 'N'
       WHERE pr.id IS NULL
         AND c.clm_process_status > '7'
         AND c.insure_company_channel!= 'CP01'
         AND DATE(c.create_time) >= CURDATE() - INTERVAL 89 DAY
       GROUP BY c.insure_company_channel
      UNION ALL
      SELECT COUNT(DISTINCT c.id) hckzajs, c.insure_company_channel
        FROM claim_ods.claim c
        LEFT JOIN claim_ods.front_seq_record f
          ON c.claim_no = f.app_no
       WHERE c.insure_company_channel = 'CP01'
         AND c.clm_process IN ('82', '101')
         AND f.id IS NULL
         AND DATE(c.create_time) >= CURDATE() - INTERVAL 89 DAY
       GROUP BY c.insure_company_channel),
    t20 AS
     (SELECT COUNT(*) dhcajs, c.insure_company_channel
        FROM claim_ods.accept_list_record a
        JOIN claim_ods.claim c
          ON a.accept_num = c.acceptance_no
         AND c.delete_flag = '0'
        JOIN claim_ods.postback_record pr
          ON pr.app_no = c.claim_no
         AND pr.is_deleted = 'N'
       WHERE pr.back_status = '0'
         AND a.accept_status NOT IN ('1', '5')
         AND c.insure_company_channel!= 'CP01'
         AND DATE(c.create_time) >= CURDATE() - INTERVAL 89 DAY
       GROUP BY c.insure_company_channel
      UNION ALL
      SELECT COUNT(DISTINCT c.id) dhcajs, c.insure_company_channel
        FROM claim_ods.claim c
        LEFT JOIN claim_ods.front_seq_record f
          ON c.claim_no = f.app_no
       WHERE c.insure_company_channel = 'CP01'
         AND c.clm_process IN ('101')
         AND DATE(c.create_time) >= CURDATE() - INTERVAL 89 DAY
       GROUP BY c.insure_company_channel),
    t21 AS
     (SELECT COUNT(*) hcsbajs, c.insure_company_channel
        FROM claim_ods.claim c
        JOIN claim_ods.postback_record pr
          ON pr.app_no = c.claim_no
         AND pr.is_deleted = 'N'
       WHERE c.clm_process_status > '7'
         AND pr.back_status IN ('3')
         AND c.insure_company_channel!= 'CP01'
         AND DATE(c.create_time) >= CURDATE() - INTERVAL 89 DAY
       GROUP BY c.insure_company_channel
      UNION ALL
      SELECT COUNT(DISTINCT c.id) hcsbajs, c.insure_company_channel -- 中智
        FROM claim_ods.claim c
        LEFT JOIN claim_ods.front_seq_record f
          ON c.claim_no = f.app_no
       WHERE c.insure_company_channel = 'CP01'
         AND f.state IN ('5', '9')
         AND DATE(c.create_time) >= CURDATE() - INTERVAL 89 DAY
       GROUP BY c.insure_company_channel),
    t22 AS
     (
      -- 回传中案件数
      SELECT c.insure_company_channel, COUNT(DISTINCT c.claim_no) hczajs
        FROM claim_ods.claim c
        JOIN claim_ods.postback_record pr
          ON pr.app_no = c.claim_no
         AND pr.is_deleted = 'N'
       WHERE c.clm_process_status > '7'
         AND pr.back_status IN ('1')
         AND c.insure_company_channel!= 'CP01'
         AND DATE(c.create_time) >= CURDATE() - INTERVAL 89 DAY
       GROUP BY c.insure_company_channel
      UNION ALL
      SELECT c.insure_company_channel, COUNT(DISTINCT c.id) hczajs
        FROM claim_ods.claim c
        LEFT JOIN claim_ods.front_seq_record f
          ON c.claim_no = f.app_no
       WHERE c.insure_company_channel = 'CP01'
         AND f.state IN ('6')
         AND DATE(c.create_time) >= CURDATE() - INTERVAL 89 DAY
       GROUP BY c.insure_company_channel)
SELECT dim.channel_value insure_company_channel,
       COALESCE(t1.fail_download, 0) fail_download,
       COALESCE(t2.lr_dfp, 0) lr_dfp,
       COALESCE(t3.dlr_vol, 0) dlr_vol,
       COALESCE(t4.dys_vol, 0) dys_vol,
       COALESCE(t5.dyypp_vol, 0) dyypp_vol,
       COALESCE(t6.dyypiaj_vol, 0) dyypiaj_vol,
       COALESCE(t7.djbpi_vol, 0) djbpi_vol,
       COALESCE(t8.djbppaj_vol, 0) djbppaj_vol,
       COALESCE(t9.dmxpi_vol, 0) dmxpi_vol,
       COALESCE(t10.dmxpiaj_vol, 0) dmxpiaj_vol,
       COALESCE(t11.dkf_vol, 0) dkf_vol,
       COALESCE(t12.dkfaj_vol, 0) dkfaj_vol,
       COALESCE(t13.dshaj_vol, 0) dshaj_vol,
       COALESCE(t14.dfhaj_vol, 0) dfhaj_vol,
       COALESCE(t15.gqwtaj_vol, 0) gqwtaj_vol,
       COALESCE(t16.thlraj_vol, 0) thlraj_vol,
       COALESCE(t17.dpzrws, 0) dpzrws,
       COALESCE(t18.dzlszajs, 0) dzlszajs,
       COALESCE(t19.hckzajs, 0) hckzajs,
       coalesce(t20.dhcajs, 0) dhcajs,
           coalesce(t21.hcsbajs, 0) hcsbajs,
           coalesce(t22.hczajs, 0) hczajs,
           replace(curdate(),'-','')
      from claim_ods.dim_insure_company_channel  dim
      left join t1
        on t1.insure_company_channel = dim.channel_key
      left join t2
        on t2.insure_company_channel = dim.channel_key
      left join t3
        on t3.insure_company_channel = dim.channel_key
      left join t4
        on t4.insure_company_channel = dim.channel_key
      left join t5
        on t5.insure_company_channel = dim.channel_key
      left join t6
        on t6.insure_company_channel = dim.channel_key
      left join t7
        on t7.insure_company_channel = dim.channel_key
      left join t8
        on t8.insure_company_channel = dim.channel_key
      left join t9
        on t9.insure_company_channel = dim.channel_key
      left join t10
        on t10.insure_company_channel = dim.channel_key
      left join t11
        on t11.insure_company_channel = dim.channel_key
      left join t12
        on t12.insure_company_channel = dim.channel_key
      left join t13
        on t13.insure_company_channel = dim.channel_key
      left join t14
        on t14.insure_company_channel = dim.channel_key
      left join t15
        on t15.insure_company_channel = dim.channel_key
      left join t16
        on t16.insure_company_channel = dim.channel_key
      left join t17
        on t17.insure_company_channel = dim.channel_key
      left join t18
        on t18.insure_company_channel = dim.channel_key
      left join t19
        on t19.insure_company_channel = dim.channel_key
      left join t20
        on t20.insure_company_channel = dim.channel_key
      left join t21
        on t21.insure_company_channel = dim.channel_key
      left join t22
        on t22.insure_company_channel = dim.channel_key
     where dim.channel_key not in ('BH01', 'PA01', 'HI01', 'PI01','TK05','TK08','YT03','TK03','CP06','YT01','YX01','CA01','RB01');

"""
def truncate_table(table_name='CLAIM_DWD.DWD_UNSETTLE_CLAIM'):
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
