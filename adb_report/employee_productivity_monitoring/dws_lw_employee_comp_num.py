import pymysql
from datetime import datetime

import configparser

config = configparser.ConfigParser()

# 读取配置文件
config.read('/opt/adb_report/config.ini')
# 阿里云AnalyticDB for MySQL的相关参数
host = 'am-uf61afo16ust6f600167320.ads.aliyuncs.com'  # 替换为您的AnalyticDB实例的endpoint
port = 3306  # 或者使用实际提供的端口号
user = config.get('CREDENTIALS', 'username')
password = config.get('CREDENTIALS', 'password')
db_name = 'claim_ods'

# 创建连接
connection = pymysql.connect(host=host,
                             port=port,
                             user=user,
                             password=password,
                             db=db_name,
                             charset='utf8mb4')  # 可根据实际情况指定字符集

now = datetime.now()
start_time = now.strftime("%Y-%m-%d %H:%M:%S")
# 输出当前时间
print("程序开始时间：", start_time)

try:
    # 创建游标对象
    with connection.cursor() as cursor:
        # 清理临时表
        sql_0 = "DROP VIEW IF EXISTS adb.claim_ods.tmp_00;"
        cursor.execute(sql_0)
        # 执行SQL查询或其他操作
        # 1  环节  审核
        sql_11 = f""" CREATE VIEW  tmp_00    as
     SELECT
         "1" as business_segment,
        c.INSURE_COMPANY_CHANNEL,
        c.C_HANDLE_STAFF as STAFF_NAME,
         count(b.id)  as cnt,
        '众安暖哇_团'  company_name
        FROM claim_ods.case_audit_task c
             left  join claim_ods.bill b
                 on  b.claim_no = c.C_CLAIM_CASE_NO
                   LEFT JOIN claim bcc ON bcc.claim_no = c.C_CLAIM_CASE_NO
        left join accept_list_record acc on acc.ACCEPT_NUM = bcc.acceptance_no
          where
         b.delete_flag = '0'
        and c.c_del_flag = '0'
        AND DATE_FORMAT(c.T_AUDIT_END_TM, 'yyyyMMdd') = DATE_FORMAT(now(), '%Y%m%d')
        and  c.C_HANDLE_CDE <> ''
        and  c.C_HANDLE_STAFF <> '系统自动'
        and  c.C_HANDLE_CDE is  not null
          and c.INSURE_COMPANY_CHANNEL = 'ZA01'
          and SUBSTRING(SUBSTRING_INDEX(acc.extra,'policyType',-1),4,1) = '1'
      group  by  c.C_HANDLE_STAFF
union all
      SELECT
         "1" as business_segment,
        c.INSURE_COMPANY_CHANNEL,
        c.C_HANDLE_STAFF as STAFF_NAME,
         count(distinct c.C_CLAIM_CASE_NO )  as cnt,
         '众安暖哇_个'  company_name
        FROM claim_ods.case_audit_task c
                   LEFT JOIN claim bcc ON bcc.claim_no = c.C_CLAIM_CASE_NO
        left join accept_list_record acc on acc.ACCEPT_NUM = bcc.acceptance_no
          where
          c.c_del_flag = '0'
        AND DATE_FORMAT(c.T_AUDIT_END_TM, 'yyyyMMdd') = DATE_FORMAT(now(), '%Y%m%d')
        and  c.C_HANDLE_CDE <> ''
        and  c.C_HANDLE_STAFF <> '系统自动'
        and  c.C_HANDLE_CDE is  not null
          and c.INSURE_COMPANY_CHANNEL = 'ZA01'
          and SUBSTRING(SUBSTRING_INDEX(acc.extra,'policyType',-1),4,1) = '2'
      group  by  c.C_HANDLE_STAFF;    
        """
        cursor.execute(sql_11)

        # 2环节  复核
        sql_bb = "DROP VIEW IF EXISTS adb.claim_ods.tmp_11;"
        cursor.execute(sql_bb)
        sql_22 = f""" CREATE VIEW  tmp_11    as
        SELECT  "2" as business_segment,
         c.INSURE_COMPANY_CHANNEL,
         c.C_REVIEWER_STAFF as STAFF_NAME ,
        count(b.id) cnt,
          '众安暖哇_团' company_name
    FROM claim_ods.case_audit_task c
        left join claim_ods.bill b  on  b.claim_no = c.C_CLAIM_CASE_NO
        LEFT JOIN claim bcc ON bcc.claim_no = c.C_CLAIM_CASE_NO
        left join accept_list_record acc on acc.ACCEPT_NUM = bcc.acceptance_no
          where
         b.delete_flag = '0'
        and c.c_del_flag = '0'
        AND   DATE_FORMAT(c.T_CLOSING_CASE_TM, 'yyyyMMdd') = DATE_FORMAT(now(), '%Y%m%d')
          and c.INSURE_COMPANY_CHANNEL = 'ZA01'
          and SUBSTRING(SUBSTRING_INDEX(acc.extra,'policyType',-1),4,1) = '1'
     group by c.C_REVIEWER_STAFF
union all
    SELECT  "2" as business_segment,
         c.INSURE_COMPANY_CHANNEL,
         c.C_REVIEWER_STAFF as STAFF_NAME ,
        count(distinct c.C_CLAIM_CASE_NO) cnt,
          '众安暖哇_个' company_name
    FROM claim_ods.case_audit_task c
        LEFT JOIN claim bcc ON bcc.claim_no = c.C_CLAIM_CASE_NO
        left join accept_list_record acc on acc.ACCEPT_NUM = bcc.acceptance_no
          where c.c_del_flag = '0'
        AND   DATE_FORMAT(c.T_CLOSING_CASE_TM, 'yyyyMMdd') = DATE_FORMAT(now(), '%Y%m%d')
          and c.INSURE_COMPANY_CHANNEL = 'ZA01'
          and SUBSTRING(SUBSTRING_INDEX(acc.extra,'policyType',-1),4,1) = '2'
     group by c.C_REVIEWER_STAFF;
        """

        cursor.execute(sql_22)

        # 3环节   问题件发起
        sql_cc = "DROP VIEW IF EXISTS adb.claim_ods.tmp_22;"
        cursor.execute(sql_cc)
        sql_33 = f""" CREATE VIEW  tmp_22    as
                 SELECT
                 "3" as business_segment,
                 a.belong_company as insure_company_channel,
                a.creator as STAFF_NAME ,
                count(1) cnt,
                c.company_name
        FROM claim_ods.question_claim  a
         left join (select claim_no,acceptance_no
from claim_ods.claim
where  insure_company_channel = 'ZA01'
 group by claim_no,acceptance_no) b
 on    a.claim_no = b.claim_no
left join (SELECT accept_num,
        case when  SUBSTRING(SUBSTRING_INDEX(extra,'policyType',-1),4,1) = '1' then  '众安暖哇_团'
            when  SUBSTRING(SUBSTRING_INDEX(extra,'policyType',-1),4,1) = '2' then  '众安暖哇_个'
             else '' end  AS  company_name
  FROM claim_ods.accept_list_record
  WHERE insure_company_channel = 'ZA01' )   c
 on b.acceptance_no  = c.accept_num
        where  DATE_FORMAT(a.gmt_created, 'yyyyMMdd') = DATE_FORMAT(now(), '%Y%m%d')
        and    a.belong_company = 'ZA01'
            GROUP BY a.belong_company,a.creator
        """
        cursor.execute(sql_33)

        # 4环节 问题件回销
        sql_dd = "DROP VIEW IF EXISTS adb.claim_ods.tmp_33;"
        cursor.execute(sql_dd)
        sql_44 = f""" CREATE VIEW  tmp_33    as
                   SELECT
                  "4" as business_segment,
                 a.belong_company as insure_company_channel,
                 modifier as STAFF_NAME,
                 count(1) cnt,
                  c.company_name
        FROM claim_ods.question_claim a
        left join (select claim_no,acceptance_no
from claim_ods.claim
where  insure_company_channel = 'ZA01'
 group by claim_no,acceptance_no) b
 on    a.claim_no = b.claim_no
left join (SELECT accept_num,
        case when  SUBSTRING(SUBSTRING_INDEX(extra,'policyType',-1),4,1) = '1' then  '众安暖哇_团'
            when  SUBSTRING(SUBSTRING_INDEX(extra,'policyType',-1),4,1) = '2' then  '众安暖哇_个'
             else '' end  AS  company_name
  FROM claim_ods.accept_list_record
  WHERE insure_company_channel = 'ZA01' )   c
 on b.acceptance_no  = c.accept_num
        where a.status = 3
        AND    DATE_FORMAT(a.conclusion_time, 'yyyyMMdd') = DATE_FORMAT(now(), '%Y%m%d')
        and    a.belong_company = 'ZA01'
            group by belong_company, modifier;
        """
        cursor.execute(sql_44)

        # 第五步 环节 预审
        sql_ee = "DROP VIEW IF EXISTS adb.claim_ods.tmp_44;"
        cursor.execute(sql_ee)
        sql_55 = f""" CREATE VIEW  tmp_44    as
                select
        "5" as business_segment,
        a.INSURE_COMPANY_CHANNEL,
        a.allot_operator_name as STAFF_NAME,
        count(a.id) cnt,
          c.company_name
        FROM claim_ods.clm_pretrial_examine a
         left join (select claim_no,acceptance_no
from claim_ods.claim
where  insure_company_channel = 'ZA01'
 group by claim_no,acceptance_no) b
 on    a.claim_app_no = b.claim_no
left join (SELECT accept_num,
        case when  SUBSTRING(SUBSTRING_INDEX(extra,'policyType',-1),4,1) = '1' then  '众安暖哇_团'
            when  SUBSTRING(SUBSTRING_INDEX(extra,'policyType',-1),4,1) = '2' then  '众安暖哇_个'
             else '' end  AS  company_name
  FROM claim_ods.accept_list_record
  WHERE insure_company_channel = 'ZA01'  )   c
 on b.acceptance_no  = c.accept_num
        where a.app_state = 3
           and   DATE_FORMAT(a.update_time, 'yyyyMMdd') = DATE_FORMAT(now(), '%Y%m%d')
        and   a.INSURE_COMPANY_CHANNEL  = 'ZA01'
   group by  a.INSURE_COMPANY_CHANNEL,a.allot_operator_name;
        """
        cursor.execute(sql_55)

        # 第六步 环节 医院
        sql_ff = "DROP VIEW IF EXISTS adb.claim_ods.tmp_55;"
        cursor.execute(sql_ff)
        sql_66 = f""" CREATE VIEW  tmp_55    as
                   SELECT   "6" as business_segment,
                  a.INSURE_COMPANY_CHANNEL ,
                  d.full_name as STAFF_NAME,
                  count(*) cnt,
                   c.company_name
        FROM claim_ods.manual_match_task a
         left join (select id,acceptance_no
from claim_ods.claim
where  insure_company_channel = 'ZA01'
 group by id,acceptance_no) b
 on    a.claim_id = b.id
left join (SELECT accept_num,
        case when  SUBSTRING(SUBSTRING_INDEX(extra,'policyType',-1),4,1) = '1' then  '众安暖哇_团'
            when  SUBSTRING(SUBSTRING_INDEX(extra,'policyType',-1),4,1) = '2' then  '众安暖哇_个'
             else '' end  AS  company_name
  FROM claim_ods.accept_list_record
  WHERE insure_company_channel = 'ZA01'  )   c
 on b.acceptance_no  = c.accept_num
left join claim_ods.sys_users d
on a.update_operator =d.id
        where a.match_status = 2
          and  DATE_FORMAT(a.deal_time, 'yyyyMMdd') = DATE_FORMAT(now(), '%Y%m%d')
        and   a.match_type = 1
        and    a.INSURE_COMPANY_CHANNEL  = 'ZA01'
           group by  a.INSURE_COMPANY_CHANNEL,a.match_ower
        """
        cursor.execute(sql_66)

        # 第七步 环节 诊断
        sql_gg = "DROP VIEW IF EXISTS adb.claim_ods.tmp_66;"
        cursor.execute(sql_gg)
        sql_77 = f""" CREATE VIEW  tmp_66    as
                SELECT    "7" as business_segment,
                  a.INSURE_COMPANY_CHANNEL ,
                  d.full_name as STAFF_NAME,
                  count(*) cnt,
                   c.company_name
        FROM claim_ods.manual_match_task a
         left join (select id,acceptance_no
from claim_ods.claim
where  insure_company_channel = 'ZA01'
 group by id,acceptance_no) b
 on    a.claim_id = b.id 
left join ( SELECT accept_num,
        case when  SUBSTRING(SUBSTRING_INDEX(extra,'policyType',-1),4,1) = '1' then  '众安暖哇_团'
            when  SUBSTRING(SUBSTRING_INDEX(extra,'policyType',-1),4,1) = '2' then  '众安暖哇_个'
             else '' end  AS  company_name
  FROM claim_ods.accept_list_record
  WHERE insure_company_channel = 'ZA01' )   c
 on b.acceptance_no  = c.accept_num  
left join claim_ods.sys_users d
on a.update_operator =d.id
        where a.match_status = 2
          and  DATE_FORMAT(a.deal_time, 'yyyyMMdd') = DATE_FORMAT(now(), '%Y%m%d')
        and   a.match_type = 2
        and  a.INSURE_COMPANY_CHANNEL  = 'ZA01'
           group by  a.INSURE_COMPANY_CHANNEL,a.match_ower
        """
        cursor.execute(sql_77)

        # 第八步 环节 明细
        sql_hh = "DROP VIEW IF EXISTS adb.claim_ods.tmp_77;"
        cursor.execute(sql_hh)
        sql_88 = f"""CREATE VIEW  tmp_77   as
          SELECT    "8" as business_segment,
                  a.INSURE_COMPANY_CHANNEL ,
                  d.full_name as STAFF_NAME,
                  count(*) cnt,
                   c.company_name
        FROM claim_ods.manual_match_task a
         left join (select id,acceptance_no
from claim_ods.claim
where  insure_company_channel = 'ZA01'
 group by id,acceptance_no) b
 on    a.claim_id = b.id
left join (SELECT accept_num,
        case when  SUBSTRING(SUBSTRING_INDEX(extra,'policyType',-1),4,1) = '1' then  '众安暖哇_团'
            when  SUBSTRING(SUBSTRING_INDEX(extra,'policyType',-1),4,1) = '2' then  '众安暖哇_个'
             else '' end  AS  company_name
  FROM claim_ods.accept_list_record
  WHERE insure_company_channel = 'ZA01'  )   c
 on b.acceptance_no  = c.accept_num
left join claim_ods.sys_users d
on a.update_operator =d.id
        where a.match_status = 2
          and  DATE_FORMAT(a.deal_time, 'yyyyMMdd') = DATE_FORMAT(now(), '%Y%m%d')
        and   a.match_type = 3
        and   a.INSURE_COMPANY_CHANNEL  = 'ZA01'
           group by  a.INSURE_COMPANY_CHANNEL,a.match_ower
        """
        cursor.execute(sql_88)

        # 第九步 环节 扣费
        sql_jj = "DROP VIEW IF EXISTS adb.claim_ods.tmp_88;"
        cursor.execute(sql_jj)
        sql_99 = f"""  CREATE VIEW  tmp_88    as
        SELECT  "9" as business_segment,
                   dt.INSURE_COMPANY_CHANNEL,
                  d.full_name as STAFF_NAME,
                  count(bb.id) cnt,
                 c.company_name
        FROM claim_ods.deduct_task dt
        left  join     claim_ods.bill bb
        on dt.claim_id = bb.claim_id
         left join ( select id,acceptance_no
from claim_ods.claim
where  insure_company_channel = 'ZA01'
 group by id,acceptance_no) b
 on    dt.claim_id = b.id
left join ( SELECT accept_num,
        case when  SUBSTRING(SUBSTRING_INDEX(extra,'policyType',-1),4,1) = '1' then  '众安暖哇_团'
            when  SUBSTRING(SUBSTRING_INDEX(extra,'policyType',-1),4,1) = '2' then  '众安暖哇_个'
             else '' end  AS  company_name
  FROM claim_ods.accept_list_record
  WHERE insure_company_channel = 'ZA01' )   c
 on b.acceptance_no  = c.accept_num
left join claim_ods.sys_users d
on dt.update_operator =d.id
          where
         (bb.person_flag = 'Y' or bb.reason_notes is not null)  and   bb.delete_flag = '0' 
        and dt.deduct_status = '2' 
         AND   DATE_FORMAT(dt.update_time, 'yyyyMMdd') = DATE_FORMAT(now(), '%Y%m%d')
           and   dt.INSURE_COMPANY_CHANNEL  = 'ZA01'
            GROUP BY  dt.INSURE_COMPANY_CHANNEL, d.full_name
        """
        cursor.execute(sql_99)

        # 第十步 环节 诊断规则
        sql_kk = "DROP VIEW IF EXISTS adb.claim_ods.tmp_99;"
        cursor.execute(sql_kk)
        sql_10 = f"""CREATE VIEW  tmp_99    as

             select
                       "10" as business_segment,
                       dt.insure_company_channel,
                       t.operator  as STAFF_NAME,
                       count(distinct dt.bill_id) as cnt,
                     c.company_name
        from claim_ods.bill_diagnose_rule_match_task dt
        inner join claim_ods.case_match_task t on dt.claim_task_id = t.id
         left join (select id,acceptance_no
from claim_ods.claim
where  insure_company_channel = 'ZA01'
 group by id,acceptance_no) b
 on    dt.claim_id = b.id
left join (SELECT accept_num,
        case when  SUBSTRING(SUBSTRING_INDEX(extra,'policyType',-1),4,1) = '1' then  '众安暖哇_团'
            when  SUBSTRING(SUBSTRING_INDEX(extra,'policyType',-1),4,1) = '2' then  '众安暖哇_个'
             else '' end  AS  company_name
  FROM claim_ods.accept_list_record
  WHERE insure_company_channel = 'ZA01' )   c
 on b.acceptance_no  = c.accept_num
        where
        t.match_type = 5 and t.task_status = 2 and t.operator != 'system'
        and   DATE_FORMAT(t.match_finish_time, 'yyyyMMdd') = DATE_FORMAT(now(), '%Y%m%d')
        and   dt.INSURE_COMPANY_CHANNEL  = 'ZA01'
        group by   dt.insure_company_channel,t.operator
        """
        cursor.execute(sql_10)

        # 11  将10个环节的数据汇总到一起
        sql_ll = "delete from adb.claim_dws.dws_employee_comp_num where   INSURE_COMPANY_CHANNEL='ZA01' and  dt=DATE_FORMAT(now(), '%Y%m%d') "
        cursor.execute(sql_ll)
        sql_12 = f"""insert into  claim_dws.dws_employee_comp_num
    SELECT  aa.*,
             cast(b.work_hours_factor as decimal(10,6)),
             DATE_FORMAT(now(), '%Y%m%d')
       from  (select *  from   tmp_00  union all
select *  from   tmp_11  union all
select *  from   tmp_22  union all
select *  from   tmp_33  union all
select *  from   tmp_44  union all
select *  from   tmp_55  union all
select *  from   tmp_66  union all
select *  from   tmp_77  union all
select *  from   tmp_88  union all
select *  from   tmp_99)  aa
      left   join  dim_project_info  b
      on aa.business_segment =b.business_segment  and aa.company_name = b.project_name
"""
        cursor.execute(sql_12)

        # 获取结果（如果适用）
        # sql_abc_lll = f""" select *  from tmp_00"""
        # cursor.execute(sql_abc_lll)
        # result = cursor.fetchall()
        print("success")


finally:
    # 关闭数据库连接
    connection.close()
    end = datetime.now()
    end_time = end.strftime("%Y-%m-%d %H:%M:%S")
    # 输出当前时间
    print("程序结束时间：", end_time)
