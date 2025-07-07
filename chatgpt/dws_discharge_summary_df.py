import pymysql
from datetime import datetime
import configparser
import argparse

# config = configparser.ConfigParser()
#
# # 读取配置文件
# config.read('/opt/adb_report/config.ini')
# 阿里云AnalyticDB for MySQL的相关参数
host = 'am-uf61afo16ust6f600167320.ads.aliyuncs.com'  # 替换为您的AnalyticDB实例的endpoint
port = 3306  # 或者使用实际提供的端口号
user = 'claim_all'
password = 'S#5DH1ar%*1n'
db_name = 'claim_ods'


# # 创建命令行参数解析器
# parser = argparse.ArgumentParser(description='Execute SQL with variable parameter')
# parser.add_argument('value', type=str, help='The value to use in the SQL query')
# args = parser.parse_args()

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


        # variable_value = args.value
        sql_fff = f""" truncate  table   claim_dws.dws_discharge_summary_df """
        # 执行SQL语句，传入变量参数
        # cursor.execute(sql_fff, (variable_value,))
        cursor.execute(sql_fff)


        # 将结果数据写入结果表
        sql_13 = f"""
insert  into claim_dws.dws_discharge_summary_df
WITH RankedData AS (
    SELECT
        C_APP_NO,
        url,
        text,
        dt,
        ROW_NUMBER() OVER (PARTITION BY url ORDER BY C_APP_NO) AS rn
    FROM
        claim_dwd.DWD_OUT_HOSPITAL_SUMMARY_DF_NEW
),
    DistinctData AS (
        SELECT DISTINCT
            C_APP_NO,
            url,
            text,
            dt
        FROM
            RankedData
        WHERE
            rn = 1
    ) ,
    app_no_data as (
    SELECT C_APP_NO,
           dt,
       GROUP_CONCAT(url ORDER BY url SEPARATOR ';') AS url,
       GROUP_CONCAT(text ORDER BY text SEPARATOR ';') AS text
FROM DistinctData
GROUP BY C_APP_NO,dt
    )
 ,final_table_data as (
select a.C_APP_NO ,                ##申请号
       d.first_insure_date ,       ##首次投保日期
       a.url ,                     ##图片地址
       a.text ,                    ##识别文本信息
       b.treatment_date,           ## 就诊日期
       b.in_hospital_date,         ## 入院日期
       b.is_stricken,              ##是否重疾 Y 是 N 否
       b.diagnose_code,            ##原疾病代码
       b.diagnose_name,            ##原疾病名称
       b.match_diagnose_code,      ##疾病匹配代码
       b.match_diagnose_name,      ##疾病匹配名称
       b.diagnose_code_auto,       ##匹配疾病CODE 系统
       b.diagnose_name_auto,       ##匹配疾病名称系统
       b.is_medical_history,       ##是否既往病史
       b.past_history,             ##既往史
       b.diagnose_year,            ##诊断年限
       b.INSURE_COMPANY_CHANNEL,    ## 大渠道
       a.dt from
    app_no_data a
left join ( SELECT
       t.claim_no,
       t.treatment_date,           ## 就诊日期
       t.in_hospital_date,         ## 入院日期
       t.is_stricken,             ##是否重疾 Y 是 N 否
       t.diagnose_code,            ##原疾病代码
       t.diagnose_name,            ##原疾病名称
       t.match_diagnose_code,      ##疾病匹配代码
       t.match_diagnose_name,      ##疾病匹配名称
       t.diagnose_code_auto,       ##匹配疾病CODE 系统
       t.diagnose_name_auto,       ##匹配疾病名称 系统
       t.is_medical_history,       ##是否既往病史
       t.past_history,             ##既往史
       t.diagnose_year,            ##诊断年限
       t.INSURE_COMPANY_CHANNEL,    ## 大渠道
        rank() over (partition by t.INSURE_COMPANY_CHANNEL,t.claim_no order by t.treatment_date  desc ) as rn
FROM
    claim_ods.bill t ) b
 on a.C_APP_NO = b.claim_no and b.rn = 1
left join  (select  apply_no,policy_part_no
from claim_ods.apply_claim  group by apply_no,policy_part_no )  c on a.C_APP_NO = c.apply_no
left join  (select policy_no,min(first_insure_date) first_insure_date
from  claim_ods.claim_policy  group by policy_no)  d on c.policy_part_no = d.policy_no)
select C_APP_NO,
       min(first_insure_date) ,
       min(url) ,
       min(text) ,
       min(treatment_date),           ## 就诊日期
       min(in_hospital_date),         ## 入院日期
       min(is_stricken),      ##是否重疾 Y 是 N 否
       min(diagnose_code),            ##原疾病代码
       min(diagnose_name),            ##原疾病名称
       min(match_diagnose_code),      ##疾病匹配代码
       min(match_diagnose_name),      ##疾病匹配名称
       min(diagnose_code_auto),       ##匹配疾病CODE 系统
       min(diagnose_name_auto),       ##匹配疾病名称系统
       min(is_medical_history),       ##是否既往病史
       min(past_history),             ##既往史
       min(diagnose_year),            ##诊断年限
       min(INSURE_COMPANY_CHANNEL),    ## 大渠道
       min(dt)
from final_table_data  group by C_APP_NO;
        """
        # cursor.execute(sql_13, (variable_value,))
        cursor.execute(sql_13)



        print("success")


finally:
    # 关闭数据库连接
    connection.close()
    end = datetime.now()
    end_time = end.strftime("%Y-%m-%d %H:%M:%S")
    # 输出当前时间
    print("程序结束时间：", end_time)
