import sys
import pymysql
from datetime import datetime
from database import DatabaseConnection


# 数据库连接信息
host = 'rr-uf63**********************cs.com'
port = 3306
user = 'p***************nly'
password = '7M*******************sj=B'
db_name = 'c****************d'


def get_sql_query():
    """
    获取SQL查询语句，这里将具体的SQL文本逻辑封装在函数内，避免在主逻辑中展示过长文本
    """
    # 这里按原来的方式拼接SQL语句，具体内容省略不展示
    sql_query = f"""
SELECT t1.insure_company_channel insure_company_channel,
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
       COALESCE(t20.dhcajs, 0) dhcajs,
       COALESCE(t21.hcsbajs, 0) hcsbajs,
       COALESCE(t22.hczajs, 0) hczajs,
       REPLACE(CURDATE(),'-','')
FROM  (
    SELECT a.insure_company_channel, COUNT(*) fail_download
    FROM claim_prd.accept_list_record a
    WHERE ACCEPT_STATUS IN ('1')
      AND DATE(a.T_CRT_TIME) >= CURDATE() - INTERVAL 3 DAY
      AND a.T_CRT_TIME < NOW() - INTERVAL 10 MINUTE
    GROUP BY a.insure_company_channel
) t1
LEFT JOIN (
    SELECT a.insure_company_channel, COUNT(*) lr_dfp
    FROM claim_prd.accept_list_record a
    JOIN claim_prd.image_assign_task d ON a.accept_batch_no = d.accept_batch_no
    WHERE d.status = '01'
      AND DATE(a.T_CRT_TIME) >= CURDATE() - INTERVAL 89 DAY
    GROUP BY a.insure_company_channel
) t2 ON t2.insure_company_channel = t1.insure_company_channel
LEFT JOIN (
    SELECT COUNT(*) dlr_vol, a.insure_company_channel
    FROM claim_prd.accept_list_record a
    LEFT JOIN claim_prd.claim c ON a.accept_num = c.acceptance_no AND c.delete_flag = '0'
    WHERE a.accept_status NOT IN ('1', '5')
      AND DATE(a.T_CRT_TIME) >= CURDATE() - INTERVAL 89 DAY
      AND c.id IS NULL
      AND a.del_flag = '0'
    GROUP BY a.insure_company_channel
) t3 ON t3.insure_company_channel = t1.insure_company_channel
LEFT JOIN (
    SELECT c.insure_company_channel, COUNT(DISTINCT c.id) dys_vol
    FROM claim_prd.accept_list_record a
    LEFT JOIN claim_prd.claim c ON a.accept_num = c.acceptance_no AND c.delete_flag = '0'
    JOIN claim_prd.clm_pretrial_examine e ON c.claim_no = e.claim_app_no
    WHERE e.app_state != '3'
      AND DATE(a.T_CRT_TIME) >= CURDATE() - INTERVAL 89 DAY
    GROUP BY c.insure_company_channel
) t4 ON t4.insure_company_channel = t1.insure_company_channel
LEFT JOIN (
    SELECT c.insure_company_channel, COUNT(DISTINCT b.id) dyypp_vol
    FROM claim_prd.manual_match_task t
    JOIN claim_prd.claim c ON t.claim_id = c.id
    JOIN claim_prd.bill b ON c.id = b.claim_id
    WHERE DATE(c.create_time) >= CURDATE() - INTERVAL 89 DAY
      AND match_type = 1
      AND match_status != '2'
    GROUP BY c.insure_company_channel
) t5 ON t5.insure_company_channel = t1.insure_company_channel
LEFT JOIN (
    SELECT c.insure_company_channel, COUNT(DISTINCT c.id) dyypiaj_vol
    FROM claim_prd.manual_match_task t
    JOIN claim_prd.claim c ON t.claim_id = c.id
    JOIN claim_prd.bill b ON c.id = b.claim_id
    WHERE DATE(c.create_time) >= CURDATE() - INTERVAL 89 DAY
      AND match_type = 1
      AND match_status != '2'
    GROUP BY c.insure_company_channel
) t6 ON t6.insure_company_channel = t1.insure_company_channel
LEFT JOIN (
    SELECT c.insure_company_channel, COUNT(DISTINCT b.id) djbpi_vol
    FROM claim_prd.manual_match_task t
    JOIN claim_prd.claim c ON t.claim_id = c.id
    JOIN claim_prd.bill b ON c.id = b.claim_id
    WHERE DATE(c.create_time) >= CURDATE() - INTERVAL 89 DAY
      AND match_type = 2
      AND match_status != '2'
    GROUP BY c.insure_company_channel
) t7 ON t7.insure_company_channel = t1.insure_company_channel
LEFT JOIN (
    SELECT c.insure_company_channel, COUNT(DISTINCT c.id) djbppaj_vol
    FROM claim_prd.manual_match_task t
    JOIN claim_prd.claim c ON t.claim_id = c.id
    JOIN claim_prd.bill b ON c.id = b.claim_id
    WHERE DATE(c.create_time) >= CURDATE() - INTERVAL 89 DAY
      AND match_type = 2
      AND match_status != '2'
    GROUP BY c.insure_company_channel
) t8 ON t8.insure_company_channel = t1.insure_company_channel
LEFT JOIN (
    SELECT c.insure_company_channel, COUNT(DISTINCT b.id) dmxpi_vol
    FROM claim_prd.manual_match_task t
    JOIN claim_prd.claim c ON t.claim_id = c.id
    JOIN claim_prd.bill b ON c.id = b.claim_id
    WHERE DATE(c.create_time) >= CURDATE() - INTERVAL 89 DAY
      AND match_type = 3
      AND match_status != '2'
    GROUP BY c.insure_company_channel
) t9 ON t9.insure_company_channel = t1.insure_company_channel
LEFT JOIN (
    SELECT c.insure_company_channel, COUNT(DISTINCT c.id) dmxpiaj_vol
    FROM claim_prd.manual_match_task t
    JOIN claim_prd.claim c ON t.claim_id = c.id
    JOIN claim_prd.bill b ON c.id = b.claim_id
    WHERE DATE(c.create_time) >= CURDATE() - INTERVAL 89 DAY
      AND match_type = 3
      AND match_status != '2'
    GROUP BY c.insure_company_channel
) t10 ON t10.insure_company_channel = t1.insure_company_channel
LEFT JOIN (
    SELECT c.insure_company_channel, COUNT(DISTINCT b.id) dkf_vol
    FROM claim_prd.claim c
    JOIN claim_prd.deduct_task d ON c.id = d.claim_id AND c.delete_flag = '0'
    JOIN claim_prd.bill b ON b.claim_id = c.id AND b.delete_flag = '0'
    WHERE d.deduct_status != '2'
      AND DATE(c.create_time) >= CURDATE() - INTERVAL 89 DAY
    GROUP BY c.insure_company_channel
) t11 ON t11.insure_company_channel = t1.insure_company_channel
LEFT JOIN (
    SELECT c.insure_company_channel, COUNT(DISTINCT c.id) dkfaj_vol
    FROM claim_prd.claim c
    JOIN claim_prd.deduct_task d ON c.id = d.claim_id AND c.delete_flag = '0'
    JOIN claim_prd.bill b ON b.claim_id = c.id AND b.delete_flag = '0'
    WHERE d.deduct_status != '2'
      AND DATE(c.create_time) >= CURDATE() - INTERVAL 89 DAY
    GROUP BY c.insure_company_channel
) t12 ON t12.insure_company_channel = t1.insure_company_channel
LEFT JOIN (SELECT c.insure_company_channel,
             COUNT(DISTINCT cat.C_CLAIM_CASE_NO) dshaj_vol
        FROM claim_prd.claim c
        JOIN claim_prd.case_audit_task cat
          ON c.claim_no = cat.C_CLAIM_CASE_NO
       WHERE cat.C_DEL_FLAG = '0'
         AND (cat.C_HANDLE_CDE!= '1' OR cat.C_HANDLE_CDE IS NULL)
         AND cat.C_MAIN_STATUS = '4'
         AND cat.C_SUB_STATUS!= '49'
         AND c.clm_process!= '60'
         AND c.is_hang_up NOT IN ('2', '4')
         AND c.clm_process_status = '6'
         AND DATE(cat.T_CRT_TM) >= CURDATE() - INTERVAL 89 DAY
       GROUP BY c.insure_company_channel
) t13 ON t13.insure_company_channel = t1.insure_company_channel
LEFT JOIN  (SELECT c.insure_company_channel,
             COUNT(DISTINCT cat.C_CLAIM_CASE_NO) dfhaj_vol
        FROM claim_prd.claim c
        JOIN claim_prd.case_audit_task cat
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
       GROUP BY c.insure_company_channel

) t14 ON t14.insure_company_channel = t1.insure_company_channel
LEFT JOIN  (
SELECT c.insure_company_channel, COUNT(DISTINCT c.claim_no) gqwtaj_vol
        FROM claim_prd.claim c
       WHERE c.delete_flag = '0'
         AND c.is_hang_up IN ('2', '4')
         AND DATE(c.create_time) >= CURDATE() - INTERVAL 89 DAY
       GROUP BY c.insure_company_channel
) t15 ON t15.insure_company_channel = t1.insure_company_channel
LEFT JOIN (
SELECT c.insure_company_channel, COUNT(DISTINCT c.claim_no) thlraj_vol
        FROM claim_prd.claim c
       WHERE c.delete_flag = '0'
         AND c.is_hang_up = '4'
         AND DATE(c.create_time) >= CURDATE() - INTERVAL 89 DAY
       GROUP BY c.insure_company_channel
) t16 ON t16.insure_company_channel = t1.insure_company_channel
LEFT JOIN (
SELECT t.insure_company_channel, COUNT(DISTINCT t.id) dpzrws
        FROM claim_prd.special_config_task t
       WHERE t.config_flag = 'config'
         AND t.task_state NOT IN ('2', '3')
         AND is_deleted = 'N'
         AND DATE(t.gmt_created) >= CURDATE() - INTERVAL 89 DAY
       GROUP BY t.insure_company_channel
) t17 ON t17.insure_company_channel = t1.insure_company_channel
LEFT JOIN  (
SELECT a.insure_company_channel, COUNT(DISTINCT a.accept_num) dzlszajs
        FROM claim_prd.accept_list_record a
        LEFT JOIN claim_prd.claim c
          ON a.accept_num = c.acceptance_no
         AND c.delete_flag = '0'
        LEFT JOIN claim_prd.clm_process cp0
          ON c.claim_no = cp0.C_CLAIM_APPLY_NO
         AND cp0.C_PROCESS_STATUS = '2'
       WHERE c.clm_process_status IN ('3', '4', '5')
         AND DATE(a.t_crt_time) >= CURDATE() - INTERVAL 89 DAY
       GROUP BY a.insure_company_channel
) t18 ON t18.insure_company_channel = t1.insure_company_channel
LEFT JOIN  (
SELECT COUNT(*) hckzajs, c.insure_company_channel
        FROM claim_prd.claim c
        JOIN claim_prd.postback_record pr
          ON pr.app_no = c.claim_no
         AND pr.is_deleted = 'N'
       WHERE pr.id IS NULL
         AND c.clm_process_status > '7'
         AND c.insure_company_channel!= 'CP01'
         AND DATE(c.create_time) >= CURDATE() - INTERVAL 89 DAY
       GROUP BY c.insure_company_channel
      UNION ALL
      SELECT COUNT(DISTINCT c.id) hckzajs, c.insure_company_channel
        FROM claim_prd.claim c
        LEFT JOIN claim_prd.front_seq_record f
          ON c.claim_no = f.app_no
       WHERE c.insure_company_channel = 'CP01'
         AND c.clm_process IN ('82', '101')
         AND f.id IS NULL
         AND DATE(c.create_time) >= CURDATE() - INTERVAL 89 DAY
       GROUP BY c.insure_company_channel
) t19 ON t19.insure_company_channel = t1.insure_company_channel
LEFT JOIN   (
SELECT COUNT(*) dhcajs, c.insure_company_channel
        FROM claim_prd.accept_list_record a
        JOIN claim_prd.claim c
          ON a.accept_num = c.acceptance_no
         AND c.delete_flag = '0'
        JOIN claim_prd.postback_record pr
          ON pr.app_no = c.claim_no
         AND pr.is_deleted = 'N'
       WHERE pr.back_status = '0'
         AND a.accept_status NOT IN ('1', '5')
         AND c.insure_company_channel!= 'CP01'
         AND DATE(c.create_time) >= CURDATE() - INTERVAL 89 DAY
       GROUP BY c.insure_company_channel
      UNION ALL
      SELECT COUNT(DISTINCT c.id) dhcajs, c.insure_company_channel
        FROM claim_prd.claim c
        LEFT JOIN claim_prd.front_seq_record f
          ON c.claim_no = f.app_no
       WHERE c.insure_company_channel = 'CP01'
         AND c.clm_process IN ('101')
         AND DATE(c.create_time) >= CURDATE() - INTERVAL 89 DAY
       GROUP BY c.insure_company_channel
) t20 ON t20.insure_company_channel = t1.insure_company_channel
LEFT JOIN   (
SELECT COUNT(*) hcsbajs, c.insure_company_channel
        FROM claim_prd.claim c
        JOIN claim_prd.postback_record pr
          ON pr.app_no = c.claim_no
         AND pr.is_deleted = 'N'
       WHERE c.clm_process_status > '7'
         AND pr.back_status IN ('3')
         AND c.insure_company_channel!= 'CP01'
         AND DATE(c.create_time) >= CURDATE() - INTERVAL 89 DAY
       GROUP BY c.insure_company_channel
      UNION ALL
      SELECT COUNT(DISTINCT c.id) hcsbajs, c.insure_company_channel -- 中智
        FROM claim_prd.claim c
        LEFT JOIN claim_prd.front_seq_record f
          ON c.claim_no = f.app_no
       WHERE c.insure_company_channel = 'CP01'
         AND f.state IN ('5', '9')
         AND DATE(c.create_time) >= CURDATE() - INTERVAL 89 DAY
       GROUP BY c.insure_company_channel
) t21 ON t21.insure_company_channel = t1.insure_company_channel
LEFT JOIN  (SELECT c.insure_company_channel, COUNT(DISTINCT c.claim_no) hczajs
        FROM claim_prd.claim c
        JOIN claim_prd.postback_record pr
          ON pr.app_no = c.claim_no
         AND pr.is_deleted = 'N'
       WHERE c.clm_process_status > '7'
         AND pr.back_status IN ('1')
         AND c.insure_company_channel!= 'CP01'
         AND DATE(c.create_time) >= CURDATE() - INTERVAL 89 DAY
       GROUP BY c.insure_company_channel
      UNION ALL
      SELECT c.insure_company_channel, COUNT(DISTINCT c.id) hczajs
        FROM claim_prd.claim c
        LEFT JOIN claim_prd.front_seq_record f
          ON c.claim_no = f.app_no
       WHERE c.insure_company_channel = 'CP01'
         AND f.state IN ('6')
         AND DATE(c.create_time) >= CURDATE() - INTERVAL 89 DAY
       GROUP BY c.insure_company_channel
       )  t22 ON t22.insure_company_channel = t1.insure_company_channel
    """
    return sql_query


def truncate_table(table_name='CLAIM_DWD.DWD_UNSETTLE_CLAIM'):
    """
    清空指定表的数据
    """
    with DatabaseConnection() as conn:
        truncate_sql = f"TRUNCATE TABLE {table_name}"
        with conn.cursor() as cursor:
            cursor.execute(truncate_sql)
            conn.commit()


def insert_data(data):
    """
    执行插入数据的操作
    """
    # 构建插入语句
    insert_sql = f"""
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
    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
    """
    with DatabaseConnection() as conn:
        with conn.cursor() as cursor:
            # 执行插入语句，并将数据作为参数传递
            if isinstance(data, (list, tuple)) and len(data) > 0 and isinstance(data[0], (list, tuple)):
                # 检查每个子列表或元组的元素数量是否与占位符数量一致
                num_placeholders = insert_sql.count('%s')
                for sub_data in data:
                    if len(sub_data) == num_placeholders:
                        cursor.executemany(insert_sql, data)
                        conn.commit()
                    else:
                        print(f"数据元素长度错误，期望 {num_placeholders} 个元素，实际 {len(sub_data)} 个元素。")

            else:
                print("数据格式错误。")



update_sql = """
    UPDATE CLAIM_DWD.DWD_UNSETTLE_CLAIM duc
    JOIN claim_ods.dim_insure_company_channel dim ON duc.insure_company_channel = dim.channel_key
    SET duc.insure_company_channel = dim.channel_value
    """


def update():
    """
    清空指定表的数据
    """
    with DatabaseConnection() as conn:
        with conn.cursor() as cursor:
            cursor.execute(update_sql)
            conn.commit()

def fetch_data_from_database():
    """
    从数据库中获取数据
    """
    connection = pymysql.connect(host=host,
                                 port=port,
                                 user=user,
                                 password=password,
                                 db=db_name,
                                 charset='utf8mb4')
    try:
        with connection.cursor() as cursor:
            sql = get_sql_query()
            cursor.execute(sql)
            result = cursor.fetchall()
            return result
    finally:
        connection.close()


if __name__ == "__main__":
    start_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    print("程序开始时间：", start_time)

    # 清空表数据
    truncate_table()

    # 从数据库获取数据
    data = fetch_data_from_database()
    # 插入数据
    insert_data(data)
    update()


    end_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    print("程序结束时间：", end_time)