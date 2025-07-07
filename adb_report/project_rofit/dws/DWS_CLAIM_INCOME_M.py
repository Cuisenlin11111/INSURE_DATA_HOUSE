import sys
sys.path.append(r"D:\因朔桔智能科技有限公司\pycharm\pycharm")
from database import DatabaseConnection

from datetime import datetime, date, timedelta

today = date.today()
# 计算昨天的日期
yesterday = today - timedelta(days=1)
# 提取昨天所在的年月，格式为 YYYY-MM
yesterday_month = yesterday.strftime("%Y-%m")

sql_query = f"""
  -- @description: 项目利润月统计-更新版
  -- @param:
  -- @return:
  -- @alter:
  -- @time: 2024-06-20 15:01:06
  -- @author: 01
  -- @version: 1.0.0

  INSERT INTO CLAIM_DWS.DWS_CLAIM_INCOME_M
(
    insurance_company_channel,
    DT_MONTH,
    claim_num,
    DRHCAL,
    claim_income,
    dt
)
with t1 as (
## 泰康渠道案件收入
SELECT INSURE_COMPANY_CHANNEL,
       `YEAR_MONTH`,
       sum(CLAIM_VOL) AS claim_num,
       sum(fee) AS claim_income,
       replace(current_date,'-','') AS dt
FROM CLAIM_DWD.DWD_TK_BILL_TOTAL
where INSURE_COMPANY_CHANNEL<>'泰康养老广东分公司'
  AND `YEAR_MONTH` = '{yesterday_month}'
GROUP BY INSURE_COMPANY_CHANNEL, `YEAR_MONTH`

union all

select INSURE_COMPANY_CHANNEL,
       `YEAR_MONTH`,
       sum(CLAIM_VOL) AS claim_num,
       sum(fee) AS claim_income,
       replace(current_date,'-','') AS dt
from CLAIM_DWD.DWD_TK07_TOTAL
WHERE `YEAR_MONTH` = '{yesterday_month}'
GROUP BY INSURE_COMPANY_CHANNEL, `YEAR_MONTH`

union all

select INSURE_COMPANY_CHANNEL,
       `YEAR_MONTH`,
       sum(CLAIM_VOL) AS claim_num,
       sum(fee) AS claim_income,
       replace(current_date,'-','') AS dt
from CLAIM_DWD.DWD_TK10_TOTAL
WHERE `YEAR_MONTH` = '{yesterday_month}'
GROUP BY INSURE_COMPANY_CHANNEL, `YEAR_MONTH`


union all

select INSURE_COMPANY_CHANNEL,
       `YEAR_MONTH`,
       sum(CLAIM_VOL) AS claim_num,
       sum(fee) AS claim_income,
       replace(current_date,'-','') AS dt
from CLAIM_DWD.DWD_TK11_TOTAL
 WHERE `YEAR_MONTH` = '{yesterday_month}'
GROUP BY INSURE_COMPANY_CHANNEL, `YEAR_MONTH`

UNION ALL

select INSURE_COMPANY_CHANNEL,
       `YEAR_MONTH`,
       sum(CLAIM_VOL) AS claim_num,
       sum(fee) AS claim_income,
       replace(current_date,'-','') AS dt
from CLAIM_DWD.DWD_TK09_TOTAL
where `YEAR_MONTH` = '{yesterday_month}'
GROUP BY INSURE_COMPANY_CHANNEL, `YEAR_MONTH`

union all

select INSURE_COMPANY_CHANNEL,
       `YEAR_MONTH`,
       sum(CLAIM_VOL) AS claim_num,
       sum(fee) AS claim_income,
       replace(current_date,'-','') AS dt
from CLAIM_DWD.DWD_TK12_TOTAL
WHERE `YEAR_MONTH` =  '{yesterday_month}'
GROUP BY INSURE_COMPANY_CHANNEL, `YEAR_MONTH`

UNION ALL

## 平安产险案件收入
SELECT '平安产险' AS INSURE_COMPANY_CHANNEL,
       `YEAR_MONTH`,
       sum(HALF_FLOW_HC + ALL_FLOW_HC) AS claim_num,
       sum(fee_total) AS claim_income,
       replace(current_date,'-','') AS dt
FROM CLAIM_DWD.DWD_PA02_TOTAL
WHERE `YEAR_MONTH` = '{yesterday_month}'
GROUP BY `YEAR_MONTH`

UNION ALL
## 太保健康
SELECT INSURE_COMPANY_CHANNEL,
       DATE_MON AS `YEAR_MONTH`,
       sum(CLAIM_NUM) AS claim_num,
       sum(TOTAL) AS claim_income,
       replace(current_date,'-','') AS dt
FROM CLAIM_DWD.DWD_CP08_TOTAL
WHERE DATE_MON = '{yesterday_month}'
GROUP BY INSURE_COMPANY_CHANNEL, DATE_MON

UNION ALL
## 太保财
SELECT INSURE_COMPANY_CHANNEL,
       DATE_MON AS `YEAR_MONTH`,
       sum(CLAIM_NUM) AS claim_num,
       case when INSURE_COMPANY_CHANNEL = '太保产险大连分公司' then 11158
            when INSURE_COMPANY_CHANNEL = '太保产险上海分公司' then 1700 +sum(TOTAL)
           else  sum(TOTAL)  end AS claim_income,
       replace(current_date,'-','') AS dt
FROM CLAIM_DWD.DWD_CP02_TOTAL
WHERE DATE_MON = '{yesterday_month}'
GROUP BY INSURE_COMPANY_CHANNEL, DATE_MON

UNION ALL
## 大家养老
SELECT INSURE_COMPANY_CHANNEL,
       DATE_MON AS `YEAR_MONTH`,
       sum(CLAIM_NUM) AS claim_num,
       sum(TOTAL) AS claim_income,
       replace(current_date,'-','') AS dt
FROM CLAIM_DWD.DWD_DJ01_TOTAL
WHERE DATE_MON = '{yesterday_month}'
GROUP BY INSURE_COMPANY_CHANNEL, DATE_MON

UNION ALL
## 渤海人寿
SELECT INSURE_COMPANY_CHANNEL,
       DATE_MON AS `YEAR_MONTH`,
       sum(CLAIM_NUM) AS claim_num,
       sum(TOTAL) AS claim_income,
       replace(current_date,'-','') AS dt
FROM CLAIM_DWD.DWD_BH01_TOTAL
WHERE DATE_MON = '{yesterday_month}'
GROUP BY INSURE_COMPANY_CHANNEL, DATE_MON

UNION ALL

-- 暖哇科技
SELECT INSURE_COMPANY_CHANNEL,
       DATE_MON AS `YEAR_MONTH`,
       sum(CLAIM_NUM) AS claim_num,
       sum(TOTAL) AS claim_income,
       replace(current_date,'-','') AS dt
FROM CLAIM_DWD.DWD_ZA01_BILL_TOTAL
WHERE DATE_MON = '{yesterday_month}'
GROUP BY INSURE_COMPANY_CHANNEL, DATE_MON

UNION ALL

-- 中智
SELECT INSURE_COMPANY_CHANNEL,
       SUBSTR(GMT_CREATED,1,7)  `YEAR_MONTH`,
       SUM(DRHCAL) AS claim_num,
       '415000' AS claim_income,
       replace(current_date,'-','') AS dt
FROM CLAIM_DWS.DWS_CLAIM_COUNT_DAY
WHERE INSURE_COMPANY_CHANNEL = '中智'
  AND substr(GMT_CREATED,1,7) = '{yesterday_month}'
GROUP BY  SUBSTR(GMT_CREATED,1,7)

UNION ALL

-- 中国人寿财产保险
select INSURE_COMPANY_CHANNEL,
       DATE_MON `YEAR_MONTH`,
       CLAIM_NUM claim_num,
       TOTAL  claim_income,
       replace(current_date,'-','') AS dt
from CLAIM_DWD.DWD_GS01_TOTAL
WHERE DATE_MON = '{yesterday_month}'

UNION ALL
-- 中国人民财产保险
SELECT INSURE_COMPANY_CHANNEL,
       DATE_MON AS `YEAR_MONTH`,
       sum(CLAIM_NUM) AS claim_num,
       sum(TOTAL) AS claim_income,
       replace(current_date,'-','') AS dt
FROM CLAIM_DWD.DWD_RB01_TOTAL
WHERE DATE_MON = '{yesterday_month}'
GROUP BY INSURE_COMPANY_CHANNEL, DATE_MON

UNION ALL
         -- 长生人寿
select INSURE_COMPANY_CHANNEL,
        `YEAR_MONTH`,
       CLAIM_VOL claim_num,
       FEE  claim_income,
       replace(current_date,'-','') AS dt
from CLAIM_DWD.DWD_CS01_TOTAL
WHERE `YEAR_MONTH` = '{yesterday_month}'

UNION ALL

-- 太保高端
select INSURE_COMPANY_CHANNEL,
        `YEAR_MONTH`,
       CLAIM_VOL claim_num,
       FEE  claim_income,
       replace(current_date,'-','') AS dt
from CLAIM_DWD.DWD_CP11_TOTAL
WHERE `YEAR_MONTH` = '{yesterday_month}'

),
    t2 as (select case when  INSURE_COMPANY_CHANNEL='泰康养老甘肃分公司'   then '泰康养老甘肃-半流程' else INSURE_COMPANY_CHANNEL end  INSURE_COMPANY_CHANNEL,
       substr(GMT_CREATED,1,7) `YEAR_MONTH`,
       sum(DRHCAL)  cnt
from claim_dws.DWS_CLAIM_COUNT_DAY
WHERE substr(GMT_CREATED,1,7) = '{yesterday_month}'
group by INSURE_COMPANY_CHANNEL,substr(GMT_CREATED,1,7))
select
      t1.INSURE_COMPANY_CHANNEL,
       t1.`YEAR_MONTH`,
       cast(claim_num as bigint),
       cast(case when t1.INSURE_COMPANY_CHANNEL='太保产险宁波分公司' then t1.claim_num else  t2.cnt end as bigint),
       cast(t1.claim_income as decimal(10,2)),
       dt
from t1
    left join t2 on t1.INSURE_COMPANY_CHANNEL=t2.INSURE_COMPANY_CHANNEL and t1.`YEAR_MONTH`=t2.`YEAR_MONTH`
 order by  t1.`YEAR_MONTH`  desc;

"""

def truncate_table(table_name='CLAIM_DWS.DWS_CLAIM_INCOME_M'):
    with DatabaseConnection() as conn:
        # 只删除昨天对应月的数据
        truncate_sql = f"DELETE FROM {table_name} WHERE DT_MONTH = '{yesterday_month}'"
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