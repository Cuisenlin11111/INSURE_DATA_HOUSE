#!/usr/bin/env python3
import pymysql
from datetime import datetime
import argparse
import configparser

config = configparser.ConfigParser()

# 读取配置文件
config.read('/opt/adb_report/config.ini')


# 创建命令行参数解析器
parser = argparse.ArgumentParser(description='Execute SQL with variable parameter')
parser.add_argument('value', type=str, help='The value to use in the SQL query')
args = parser.parse_args()

# 阿里云AnalyticDB for MySQL的相关参数
host = config.get('CREDENTIALS', 'host')
port_str = config.get('CREDENTIALS', 'port')  # 初始获取的是字符串
port = int(port_str)
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

        # 执行SQL查询或其他操作

        # 删除当天数据
        sql_fff = f""" delete  from  CLAIM_DWD.DWD_CLAIM_COUNT_DAY  where GMT_CREATED=%s   and insure_company_channel in ('众安暖哇_团','众安暖哇_个') """
        # 使用命令行参数作为变量值
        variable_value = args.value

        # 执行SQL语句，传入变量参数
        cursor.execute(sql_fff, (variable_value,))
        # cursor.execute(sql_fff)
        # 将结果数据写入结果表
        sql_13 = f"""
insert into CLAIM_DWD.DWD_CLAIM_COUNT_DAY
    (insure_company_channel,
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
     shzdhl_rw,
     fh_auto_claim_count,
     fhzdhl,
     question_claim_vol,
     hospatal_claim_vol,
     data_dt,
     sh_claim_count)
with b1 as
     (
      SELECT
        insure_company_channel,
        comm_date,
        count(*) jjl
      FROM
      (
        select
           CASE
          WHEN substring(substring_index(extra, 'policyType', -1),4,1) = '1'  THEN '众安暖哇_团'
          WHEN substring(substring_index(extra, 'policyType', -1),4,1) = '2'  THEN '众安暖哇_个'
          ELSE ''
         END insure_company_channel,
               substring(ACCEPT_DATE,1,10)  comm_date
          from claim_ods.accept_list_record
         where DEL_FLAG = '0'
           and insure_company_channel ='ZA01'
        )
        group by insure_company_channel,comm_date
     ),
    ba as
     (
      SELECT

        comm_date,
        insure_company_channel,
        count(*) cancel_vol
      FROM
      (
        select
         CASE
          WHEN substring(substring_index(extra, 'policyType', -1),4,1) = '1'  THEN '众安暖哇_团'
          WHEN substring(substring_index(extra, 'policyType', -1),4,1) = '2'  THEN '众安暖哇_个'
          ELSE ''
         END  insure_company_channel,
               substring(T_CRT_TIME,1,10) comm_date
          from claim_ods.accept_list_record
         where insure_company_channel ='ZA01'
           and DEL_FLAG = '0'
           and ACCEPT_STATUS = '5' 
        )
        group by insure_company_channel,comm_date
      ),
    b2 as
     (
      SELECT
        comm_date,
        insure_company_channel,
        count(*) xxqd
      FROM
      (
        select
               substring(ACCEPT_DATE,1,10) comm_date,
               CASE
          WHEN substring(substring_index(extra, 'policyType', -1),4,1) = '1'  THEN '众安暖哇_团'
          WHEN substring(substring_index(extra, 'policyType', -1),4,1) = '2'  THEN '众安暖哇_个'
          ELSE ''
         END insure_company_channel
          from claim_ods.accept_list_record
         where DEL_FLAG = '0'
           and claim_source in ( '1','3')
           and insure_company_channel ='ZA01'   

         )
         group by insure_company_channel,comm_date
     ),
    b3 as
     (
      SELECT
        comm_date,
        insure_company_channel,
        count(*) xsqd
      FROM
      (
        select
               substring(ACCEPT_DATE,1,10) comm_date,
              CASE
          WHEN substring(substring_index(extra, 'policyType', -1),4,1) = '1'  THEN '众安暖哇_团'
          WHEN substring(substring_index(extra, 'policyType', -1),4,1) = '2'  THEN '众安暖哇_个'
          ELSE ''
         END insure_company_channel
          from claim_ods.accept_list_record
         where DEL_FLAG = '0'
           and claim_source in ( '2','4')
           and insure_company_channel  in ('PA02','ZA01')  

        )
        group by insure_company_channel,comm_date
     ),
    b4 as
     (
      SELECT
        comm_date,
        insure_company_channel,
        count(*) lrl

      FROM
      (
        select
               substring(c.create_time,1,10) comm_date,
               CASE
          WHEN substring(substring_index(a.extra, 'policyType', -1),4,1) = '1'  THEN '众安暖哇_团'
          WHEN substring(substring_index(a.extra, 'policyType', -1),4,1) = '2'  THEN '众安暖哇_个'
          ELSE '' END insure_company_channel
          FROM claim_ods.accept_list_record a
          join claim_ods.claim c
            on a.accept_num = c.acceptance_no
           and c.delete_flag = '0'
         where c.delete_flag = '0'
           and a.insure_company_channel ='ZA01'  

      )
      group by insure_company_channel,comm_date
     ),
    b5 as
     (
      SELECT
        comm_date,
        insure_company_channel,
        count(distinct app_no) drhcal
      FROM
      (
        select  app_no,
               substring(back_time,1,10)  comm_date,
               CASE
       WHEN substring(substring_index(alro.extra, 'policyType', -1),4,1) = '1'  THEN '众安暖哇_团'
       WHEN substring(substring_index(alro.extra, 'policyType', -1),4,1) = '2'  THEN '众安暖哇_个'
          ELSE ''
         END  insure_company_channel
          from claim_ods.postback_record pr
          LEFT JOIN claim_ods.accept_list_record alro
        ON pr.accept_num = alro.accept_num AND alro.DEL_FLAG = '0'
         where pr.back_status in ('2', '21')
           and pr.is_deleted = 'N'
           and pr.insure_company_channel ='ZA01'  
        )
        group BY insure_company_channel,comm_date

     ),
    b6 as
     (
      SELECT
        comm_date,
        insure_company_channel,
        count(distinct app_no ) blchcal
      FROM
      (
        select  substring(back_time,1,10) comm_date,
                app_no,
                CASE
       WHEN substring(substring_index(alro.extra, 'policyType', -1),4,1) = '1'  THEN '众安暖哇_团'
       WHEN substring(substring_index(alro.extra, 'policyType', -1),4,1) = '2'  THEN '众安暖哇_个'
          ELSE ''
         END  insure_company_channel
          from claim_ods.postback_record pr
          LEFT JOIN claim_ods.accept_list_record alro
        ON pr.accept_num = alro.accept_num AND alro.DEL_FLAG = '0'
         WHERE pr.back_status in ('2', '21')
           and pr.is_deleted = 'N'
           and pr.postback_way = 'H'
           and pr.insure_company_channel ='ZA01' 
        )
        group by insure_company_channel,comm_date
     ),
    b7 as
     (
           SELECT
        comm_date,
        insure_company_channel,
        count(distinct app_no) qlchcal
      FROM
      (
        select
               substring(back_time,1,10) comm_date,
                 pr.app_no,
                CASE
        WHEN substring(substring_index(alro.extra, 'policyType', -1),4,1) = '1'  THEN '众安暖哇_团'
        WHEN substring(substring_index(alro.extra, 'policyType', -1),4,1) = '2'  THEN '众安暖哇_个'
          ELSE ''
         END  insure_company_channel
          from claim_ods.postback_record pr
          LEFT JOIN claim_ods.accept_list_record alro
      ON pr.accept_num = alro.accept_num AND alro.DEL_FLAG = '0'
         where pr.back_status in ('2', '21')
           and pr.is_deleted = 'N'
            and (pr.postback_way = 'W' or pr.postback_way is null)
           and pr.insure_company_channel ='ZA01'  

        )
        group by insure_company_channel,comm_date
    ),
    b8 as
     (
      SELECT
        comm_date,
        insure_company_channel,
        count(distinct app_no) hcsbal
      FROM
      (
    select    substring(gmt_created,1,10) comm_date,
               app_no,
               CASE
      WHEN substring(substring_index(alro.extra, 'policyType', -1),4,1) = '1'  THEN '众安暖哇_团'
      WHEN substring(substring_index(alro.extra, 'policyType', -1),4,1) = '2'  THEN '众安暖哇_个'
          ELSE ''
         END  insure_company_channel
          from claim_ods.postback_record pr
          LEFT JOIN claim_ods.accept_list_record alro
      ON pr.accept_num = alro.accept_num AND alro.DEL_FLAG = '0'
         WHERE pr.back_status in ('3')
           and pr.is_deleted = 'N'
           and receiver = 'I'
           and pr.insure_company_channel ='ZA01'  
      )
      group by insure_company_channel,comm_date
      ),
    b9 as
     (
      SELECT
        comm_date,
        insure_company_channel,
        count(distinct  C_CLAIM_CASE_NO) zdshal
      FROM
      (
        select
            cat.C_CLAIM_CASE_NO,
               substring(pr.back_time,1,10)   comm_date,
               CASE
        WHEN substring(substring_index(alro.extra, 'policyType', -1),4,1) = '1'  THEN '众安暖哇_团'
        WHEN substring(substring_index(alro.extra, 'policyType', -1),4,1) = '2'  THEN '众安暖哇_个'
          ELSE ''
         END  insure_company_channel
          FROM claim_ods.postback_record pr
          LEFT JOIN claim_ods.case_audit_task cat
            on cat.c_claim_case_no = pr.app_no
          LEFT JOIN claim_ods.accept_list_record alro
      ON pr.accept_num = alro.accept_num AND alro.DEL_FLAG = '0'
         WHERE pr.back_status in ('2', '21')
           and pr.is_deleted = 'N'
  ##          and (pr.postback_way = 'W' or pr.postback_way is null)
           and pr.insure_company_channel ='ZA01'
            and cat.C_HANDLE_CDE = '1' 

       )
       group by insure_company_channel,comm_date

    ),
    b10 as
     (
      SELECT
        insure_company_channel,
        comm_date,
        count(distinct accept_num) fh_auto_claim_count
      FROM
      (
        select
          CASE
    WHEN substring(substring_index(a.extra, 'policyType', -1),4,1) = '1'  THEN '众安暖哇_团'
    WHEN substring(substring_index(a.extra, 'policyType', -1),4,1) = '2'  THEN '众安暖哇_个'
          ELSE ''
         END  insure_company_channel,
              substring(pr.back_time,1,10)  comm_date,
              a.accept_num
          from claim_ods.accept_list_record a
          join claim_ods.claim c
            on a.accept_num = c.acceptance_no
           and c.delete_flag = '0'
          join claim_ods.postback_record pr
            on pr.app_no = c.claim_no
           and pr.is_deleted = 'N'
           and pr.back_status in ('2', '21')
           and pr.receiver = 'I'
          join claim_ods.case_audit_task ca
            on ca.C_CLAIM_CASE_NO = c.claim_no
           and ca.C_DEL_FLAG = '0'
         where (pr.postback_way = 'W' or pr.postback_way is null)
           and ca.C_REVIEWER_STAFF = '系统自动'
           and pr.insure_company_channel ='ZA01' 
      )
        group by insure_company_channel,comm_date

    ),
    b11 as
     (
        SELECT
          insure_company_channel,
          comm_date,
          count(distinct id) 问题件案件数
        FROM
        (
        SELECT
           CASE
     WHEN substring(substring_index(a.extra, 'policyType', -1),4,1) = '1'  THEN '众安暖哇_团'
     WHEN substring(substring_index(a.extra, 'policyType', -1),4,1) = '2' THEN '众安暖哇_个'
          ELSE ''
         END  insure_company_channel,
               substring(c.create_time,1,10)  comm_date,
               c.id
          from claim_ods.accept_list_record a
          join claim_ods.claim c
            on a.accept_num = c.acceptance_no
           and c.delete_flag = '0'
          join claim_ods.question_claim q
            on c.claim_no = q.claim_no
            where a.insure_company_channel ='ZA01'
        )
        GROUP BY insure_company_channel, comm_date
     ),
    b12 as
     (
       SELECT
        insure_company_channel,
        comm_date,
        count(distinct id) 住院案件数
       FROM
       (
        SELECT
        CASE
     WHEN substring(substring_index(a.extra, 'policyType', -1),4,1) = '1'  THEN '众安暖哇_团'
     WHEN substring(substring_index(a.extra, 'policyType', -1),4,1) = '2'  THEN '众安暖哇_个'
          ELSE ''
         END  insure_company_channel,
        substring(a.T_CRT_TIME,1,10) comm_date,
         c.id
      FROM
        claim_ods.accept_list_record a
      JOIN claim_ods.claim c
      ON a.accept_num = c.acceptance_no AND c.delete_flag = '0'
      WHERE  a.insure_company_channel  in ('PA02','ZA01')
      and  c.delete_flag = '0'
        AND EXISTS (
          SELECT
            1
          FROM
            claim_ods.bill b
          WHERE
            b.claim_id = c.id
            AND b.delete_flag = '0'
            AND b.treatment_date IS NULL
        )  

       )
       GROUP BY insure_company_channel, comm_date
     ),
    b13 as
     (##审核自动化率计算： 平安用老算法，非平安的用这个新算法：
          SELECT
            CASE
    WHEN substring(substring_index(alro.extra, 'policyType', -1),4,1) = '1'  THEN '众安暖哇_团'
    WHEN substring(substring_index(alro.extra, 'policyType', -1),4,1) = '2'  THEN '众安暖哇_个'
          ELSE ''
         END insure_company_channel,
            substring(cat.T_CRT_TM,1,10) comm_date,
            count(distinct case when  cat.C_HANDLE_CDE = '1' then cat.c_claim_case_no else null end) num,
            count(distinct cat.c_claim_case_no) sh_claim_count,
            count(distinct case when  cat.C_HANDLE_CDE = '1' then cat.c_claim_case_no else null end)/count(distinct cat.c_claim_case_no)
                                                    as shzdhl
          FROM
            claim_ods.claim c
          LEFT JOIN claim_ods.case_audit_task cat
          ON cat.c_claim_case_no = c.claim_no AND cat.C_DEL_FLAG = '0'
          LEFT JOIN claim_ods.accept_list_record alro
          ON c.ACCEPTANCE_NO = alro.accept_num AND alro.DEL_FLAG = '0'
          WHERE cat.T_CRT_TM IS NOT NULL  and  c.insure_company_channel ='ZA01'
        GROUP BY insure_company_channel, comm_date

  ) ,b14 as
    (
      select DISTINCT b1.insure_company_channel,

               b1.comm_date gmt_created,
               coalesce(b1.jjl, 0) jjl,
               coalesce(ba.cancel_vol, 0) cancel_vol,
               coalesce(b2.xxqd, 0) xxqd,
               coalesce(b3.xsqd, 0) xsqd,
               coalesce(b4.lrl, 0) lrl,
               coalesce(b5.drhcal, 0) drhcal,
               coalesce(b6.blchcal, 0) blchcal,
               coalesce(b7.qlchcal, 0) qlchcal,
               coalesce(b8.hcsbal, 0) hcsbal,
               coalesce(b9.zdshal, 0) zdshal,
               coalesce(b13.shzdhl, 0) as shzdhl_rw,
			    coalesce(b13.num, 0) as ZDSHAL_RW,
               coalesce(b10.fh_auto_claim_count, 0) fh_auto_claim_count,
               coalesce(b11.问题件案件数, 0) question_claim_vol,
               coalesce(b12.住院案件数, 0) hospatal_claim_vol,
               b1.comm_date,
               b13.sh_claim_count
          from b1
          left join ba
            on b1.comm_date = ba.comm_date
           and b1.insure_company_channel = ba.insure_company_channel
          left join b2
            on b1.comm_date = b2.comm_date
           and b1.insure_company_channel = b2.insure_company_channel
          left join b3
            on b1.comm_date = b3.comm_date
           and b1.insure_company_channel = b3.insure_company_channel
          left join b4
            on b1.comm_date = b4.comm_date
           and b1.insure_company_channel = b4.insure_company_channel
          left join b5
            on b1.comm_date = b5.comm_date
           and b1.insure_company_channel = b5.insure_company_channel
          left join b6
            on b1.comm_date = b6.comm_date
           and b1.insure_company_channel = b6.insure_company_channel
          left join b7
            on b1.comm_date = b7.comm_date
           and b1.insure_company_channel = b7.insure_company_channel
          left join b8
            on b1.comm_date = b8.comm_date
           and b1.insure_company_channel = b8.insure_company_channel
          left join b9
            on b1.comm_date = b9.comm_date
           and b1.insure_company_channel = b9.insure_company_channel
          left join b10
            on b1.comm_date = b10.comm_date
           and b1.insure_company_channel = b10.insure_company_channel
          left join b11
            on b1.comm_date = b11.comm_date
           and b1.insure_company_channel = b11.insure_company_channel
          left join b12
            on b1.comm_date = b12.comm_date
           and b1.insure_company_channel = b12.insure_company_channel
          left join b13
            on b1.comm_date = b13.comm_date
           and b1.insure_company_channel = b13.insure_company_channel
         where (coalesce(b1.jjl, 0) > 0 or coalesce(b5.drhcal, 0) > 0)
      and  b1.comm_date  =   %s

  )

    SELECT
      DISTINCT
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
      cast(case
        when coalesce(drhcal, 0) = 0 THEN 0
        ELSE coalesce(qlchcal, 0) / coalesce(drhcal, 0) END as DECIMAL(10,4)) perc_all_flow,
      hcsbal,
      zdshal,
	  ZDSHAL_RW,
      cast(case
        when coalesce(qlchcal, 0) = 0 THEN  0
        ELSE coalesce(zdshal, 0) / coalesce(qlchcal, 0)  END as DECIMAL(10,4)) shzdhl,
        cast(case when shzdhl_rw>1 then 1 else shzdhl_rw end as DECIMAL(10,4)),
        fh_auto_claim_count,
        cast(case
        when coalesce(qlchcal, 0) = 0 THEN  0
        ELSE coalesce(fh_auto_claim_count, 0) / coalesce(qlchcal, 0) END as DECIMAL(10,4)) as fhzdhl,
        question_claim_vol,
        hospatal_claim_vol,
        replace(substring(gmt_created,1,10),'-',''),
        sh_claim_count

    FROM  b14 where insure_company_channel is not null;

        """
        cursor.execute(sql_13, (variable_value,))
        # cursor.execute(sql_13)

        print("success")


finally:
    # 关闭数据库连接
    connection.close()
    end = datetime.now()
    end_time = end.strftime("%Y-%m-%d %H:%M:%S")
    # 输出当前时间
    print("程序结束时间：", end_time)
