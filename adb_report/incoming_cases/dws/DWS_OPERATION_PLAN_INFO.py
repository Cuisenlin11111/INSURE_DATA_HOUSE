# import sys
# sys.path.append(r"E:\pycharm\database")
from database import DatabaseConnection
from datetime import datetime



sql_query = f"""
  -- @description: 运营计划表
  -- @param:
  -- @return:
  -- @alter:
  -- @time: 2024-06-20 15:01:06
  -- @author: 01
  -- @version: 1.0.0
insert into `CLAIM_DWS`.`DWS_OPERATION_PLAN_INFO`
WITH pp AS (
    SELECT
        渠道,
        年月,
        dt_month,
        日计划平均进件量,
        当月实际发生天数,
        当月累积案件,
        CASE
            WHEN 当月实际发生天数 = 0 THEN 0
            ELSE 当月累积案件 / 当月实际发生天数
        END AS 日实际平均进件量,
        CASE
            WHEN 当月实际发生天数 = 0 THEN 0
            ELSE 当月累积案件 / 当月实际发生天数 - 日计划平均进件量
        END AS 日平均进件量差值,
        CASE
            WHEN 当月实际发生天数 = 0 THEN 0
            ELSE (当月累积案件 / 当月实际发生天数 - 日计划平均进件量) * 当月实际发生天数
        END AS 月累积差值
    FROM (
        SELECT DISTINCT
            t2.insure_company_channel AS 渠道,
            t2.year_month AS 年月,
            t2.dt_month,
            CASE
                WHEN t2.insure_company_channel = '暖哇科技' AND t2.year_month = '2023-01' THEN 270
                WHEN t2.insure_company_channel = '暖哇科技' AND t2.year_month = '2023-02' THEN 440
                WHEN t2.insure_company_channel = '暖哇科技' AND t2.year_month = '2023-03' THEN 540
                WHEN t2.insure_company_channel = '暖哇科技' AND t2.year_month = '2023-04' THEN 540
                WHEN t2.insure_company_channel = '暖哇科技' AND t2.year_month = '2023-05' THEN 540
                WHEN t2.insure_company_channel = '暖哇科技' AND t2.year_month = '2023-06' THEN 540
                -- 平安产险
                WHEN t2.insure_company_channel = '平安产险' AND t2.year_month = '2023-01' THEN 1230
                WHEN t2.insure_company_channel = '平安产险' AND t2.year_month = '2023-02' THEN 1800
                WHEN t2.insure_company_channel = '平安产险' AND t2.year_month = '2023-03' THEN 2200
                WHEN t2.insure_company_channel = '平安产险' AND t2.year_month = '2023-04' THEN 2700
                WHEN t2.insure_company_channel = '平安产险' AND t2.year_month = '2023-05' THEN 2700
                WHEN t2.insure_company_channel = '平安产险' AND t2.year_month = '2023-06' THEN 2700
                -- 泰康全渠道
                WHEN t2.insure_company_channel = '泰康全渠道' AND t2.year_month = '2023-01' THEN 1160
                WHEN t2.insure_company_channel = '泰康全渠道' AND t2.year_month = '2023-02' THEN 2000
                WHEN t2.insure_company_channel = '泰康全渠道' AND t2.year_month = '2023-03' THEN 2700
                WHEN t2.insure_company_channel = '泰康全渠道' AND t2.year_month = '2023-04' THEN 3900
                WHEN t2.insure_company_channel = '泰康全渠道' AND t2.year_month = '2023-05' THEN 3900
                WHEN t2.insure_company_channel = '泰康全渠道' AND t2.year_month = '2023-06' THEN 3900
                ELSE 0
            END AS 日计划平均进件量,
            t2.当月实际发生天数,
            t2.month_jjl AS 当月累积案件
        FROM CLAIM_DWS.DWS_CLAIM_COUNT_DAY t1
        RIGHT JOIN (
            -- 除去泰康的渠道
            SELECT
                insure_company_channel,
                a2.year_month,
                a2.dt_month,
                month_jjl,
                当月实际发生天数
            FROM (
                SELECT
                    insure_company_channel,
                    SUBSTR(gmt_created, 1, 7) year_month,
                    SUM(jjl) month_jjl
                FROM CLAIM_DWS.DWS_CLAIM_COUNT_DAY
                WHERE gmt_created < CURDATE() AND insure_company_channel NOT IN ('暖哇科技', '平安产险')
                GROUP BY insure_company_channel, SUBSTR(gmt_created, 1, 7)
            ) a1
            RIGHT JOIN (
                SELECT
                    CASE
                        WHEN year_month = DATE_FORMAT(NOW(), '%Y-%m') THEN ab.当月实际发生天数
                        WHEN year_month > DATE_FORMAT(NOW(), '%Y-%m') THEN 0
                        ELSE 22
                    END AS 当月实际发生天数,
                    abc.year_month,
                    abc.dt_month
                FROM (
                    SELECT
                        COUNT(DISTINCT date_dtd) 当月实际发生天数,
                        year_month,
                        dt_month
                    FROM `CLAIM_ODS`.`DIM_CHANNEL_DATE`
                    WHERE dt_week IN ('星期一', '星期二', '星期三', '星期四', '星期五') AND dt_year <= DATE_FORMAT(NOW(), '%Y')
                    GROUP BY year_month, dt_month
                ) abc
                LEFT JOIN (
                    SELECT
                        COUNT(*) 当月实际发生天数,
                        dt_month
                    FROM CLAIM_ODS.`DIM_DATE_WEEK`
                    WHERE dt_month = DATE_FORMAT(NOW(), '%Y-%m') AND date_dt <= DATE_FORMAT(DATE_SUB(NOW(), INTERVAL 1 DAY), '%Y-%m-%d') AND dt_week IN ('星期一', '星期二', '星期三', '星期四', '星期五')
                    GROUP BY dt_month
                ) ab ON abc.year_month = ab.dt_month
            ) a2 ON a1.year_month = a2.year_month
            UNION ALL
            -- 泰康全渠道（补数据）
            SELECT
                '泰康全渠道' insure_company_channel,
                a2.year_month,
                a2.dt_month,
                NVL(SUM(month_jjl), 0) month_jjl,
                当月实际发生天数
            FROM (
                SELECT
                    SUBSTR(gmt_created, 1, 7) year_month,
                    SUBSTR(gmt_created, 6, 2) month,
                    SUM(jjl) month_jjl
                FROM CLAIM_DWS.DWS_CLAIM_COUNT_DAY
                WHERE gmt_created < CURDATE() AND insure_company_channel LIKE '%泰康养老%'  and  insure_company_channel<>'泰康养老全渠道'
                GROUP BY insure_company_channel, SUBSTR(gmt_created, 1, 7), SUBSTR(gmt_created, 6, 2)
            ) a1
            RIGHT JOIN (
                SELECT
                    CASE
                        WHEN year_month = DATE_FORMAT(NOW(), '%Y-%m') THEN ab.当月实际发生天数
                        WHEN year_month > DATE_FORMAT(NOW(), '%Y-%m') THEN 0
                        ELSE 22
                    END AS 当月实际发生天数,
                    abc.year_month,
                    abc.dt_month
                FROM (
                    SELECT
                        COUNT(DISTINCT date_dtd) 当月实际发生天数,
                        year_month,
                        dt_month
                    FROM `CLAIM_ODS`.`DIM_CHANNEL_DATE`
                    WHERE dt_week IN ('星期一', '星期二', '星期三', '星期四', '星期五') AND dt_year <= DATE_FORMAT(NOW(), '%Y')
                    GROUP BY year_month, dt_month
                ) abc
                LEFT JOIN (
                    SELECT
                        COUNT(*) 当月实际发生天数,
                        dt_month
                    FROM CLAIM_ODS.`DIM_DATE_WEEK`
                    WHERE dt_month = DATE_FORMAT(NOW(), '%Y-%m') AND date_dt <= DATE_FORMAT(DATE_SUB(NOW(), INTERVAL 1 DAY), '%Y-%m-%d') AND dt_week IN ('星期一', '星期二', '星期三', '星期四', '星期五')
                    GROUP BY dt_month
                ) ab ON abc.year_month = ab.dt_month
            ) a2 ON a1.year_month = a2.year_month
            GROUP BY a2.year_month, a2.dt_month, 当月实际发生天数
            UNION ALL
            -- 平安产险（补数据）
            SELECT
                '平安产险' insure_company_channel,
                a2.year_month,
                a2.dt_month,
                NVL(SUM(month_jjl), 0) month_jjl,
                当月实际发生天数
            FROM (
                SELECT
                    SUBSTR(gmt_created, 1, 7) year_month,
                    SUBSTR(gmt_created, 6, 2) month,
                    SUM(jjl) month_jjl
                FROM CLAIM_DWS.DWS_CLAIM_COUNT_DAY
                WHERE gmt_created < CURDATE() AND insure_company_channel = '平安产险'
                GROUP BY insure_company_channel, SUBSTR(gmt_created, 1, 7), SUBSTR(gmt_created, 6, 2)
            ) a1
            RIGHT JOIN (
                SELECT
                    CASE
                        WHEN year_month = DATE_FORMAT(NOW(), '%Y-%m') THEN ab.当月实际发生天数
                        WHEN year_month > DATE_FORMAT(NOW(), '%Y-%m') THEN 0
                        ELSE 22
                    END AS 当月实际发生天数,
                    abc.year_month,
                    abc.dt_month
                FROM (
                    SELECT
                        COUNT(DISTINCT date_dtd) 当月实际发生天数,
                        year_month,
                        dt_month
                    FROM `CLAIM_ODS`.`DIM_CHANNEL_DATE`
                    WHERE dt_week IN ('星期一', '星期二', '星期三', '星期四', '星期五') AND dt_year <= DATE_FORMAT(NOW(), '%Y')
                    GROUP BY year_month, dt_month
                ) abc
                LEFT JOIN (
                    SELECT
                        COUNT(*) 当月实际发生天数,
                        dt_month
                    FROM CLAIM_ODS.`DIM_DATE_WEEK`
                    WHERE dt_month = DATE_FORMAT(NOW(), '%Y-%m') AND date_dt <= DATE_FORMAT(DATE_SUB(NOW(), INTERVAL 1 DAY), '%Y-%m-%d') AND dt_week IN ('星期一', '星期二', '星期三', '星期四', '星期五')
                    GROUP BY dt_month
                ) ab ON abc.year_month = ab.dt_month
            ) a2 ON a1.year_month = a2.year_month
            GROUP BY a2.year_month, a2.dt_month, 当月实际发生天数
            UNION ALL
            -- 暖哇科技（补数据）
            SELECT
                '暖哇科技' insure_company_channel,
                a2.year_month,
                a2.dt_month,
                NVL(SUM(month_jjl), 0) month_jjl,
                当月实际发生天数
            FROM (
                SELECT
                    SUBSTR(gmt_created, 1, 7) year_month,
                    SUBSTR(gmt_created, 6, 2) month,
                    SUM(jjl) month_jjl
                FROM CLAIM_DWS.DWS_CLAIM_COUNT_DAY
                WHERE gmt_created < CURDATE() AND insure_company_channel = '暖哇科技'
                GROUP BY insure_company_channel, SUBSTR(gmt_created, 1, 7), SUBSTR(gmt_created, 6, 2)
            ) a1
            RIGHT JOIN (
                SELECT
                    CASE
                        WHEN year_month = DATE_FORMAT(NOW(), '%Y-%m') THEN ab.当月实际发生天数
                        WHEN year_month > DATE_FORMAT(NOW(), '%Y-%m') THEN 0
                        ELSE 22
                    END AS 当月实际发生天数,
                    abc.year_month,
                    abc.dt_month
                FROM (
                    SELECT
                        COUNT(DISTINCT date_dtd) 当月实际发生天数,
                        year_month,
                        dt_month
                    FROM `CLAIM_ODS`.`DIM_CHANNEL_DATE`
                    WHERE dt_week IN ('星期一', '星期二', '星期三', '星期四', '星期五') AND dt_year <= DATE_FORMAT(NOW(), '%Y')
                    GROUP BY year_month, dt_month
                ) abc
                LEFT JOIN (
                    SELECT
                        COUNT(*) 当月实际发生天数,
                        dt_month
                    FROM CLAIM_ODS.`DIM_DATE_WEEK`
                    WHERE dt_month = DATE_FORMAT(NOW(), '%Y-%m') AND date_dt <= DATE_FORMAT(DATE_SUB(NOW(), INTERVAL 1 DAY), '%Y-%m-%d') AND dt_week IN ('星期一', '星期二', '星期三', '星期四', '星期五')
                    GROUP BY dt_month
                ) ab ON abc.year_month = ab.dt_month
            ) a2 ON a1.year_month = a2.year_month
            GROUP BY a2.year_month, a2.dt_month, 当月实际发生天数
        ) t2 ON t1.insure_company_channel = t2.insure_company_channel AND SUBSTR(t1.gmt_created, 1, 7) = t2.year_month
    )
)
SELECT
    渠道,
    SUBSTR(年月, 1, 4) AS 年份,
    指标,
    cast(NVL(SUM(CASE WHEN dt_month = '01' THEN claim_vol END), 0) as decimal (10,2)) AS 一月,
    cast(NVL(SUM(CASE WHEN dt_month = '02' THEN claim_vol END), 0)  as decimal (10,2)) AS 二月,
    cast(NVL(SUM(CASE WHEN dt_month = '03' THEN claim_vol END), 0)  as decimal (10,2)) AS 三月,
    cast(NVL(SUM(CASE WHEN dt_month = '04' THEN claim_vol END), 0) as decimal (10,2))  AS 四月,
    cast(NVL(SUM(CASE WHEN dt_month = '05' THEN claim_vol END), 0)  as decimal (10,2)) AS 五月,
    cast(NVL(SUM(CASE WHEN dt_month = '06' THEN claim_vol END), 0)  as decimal (10,2)) AS 六月,
    cast(NVL(SUM(CASE WHEN dt_month = '07' THEN claim_vol END), 0)  as decimal (10,2)) AS 七月,
    cast(NVL(SUM(CASE WHEN dt_month = '08' THEN claim_vol END), 0)  as decimal (10,2)) AS 八月,
    cast(NVL(SUM(CASE WHEN dt_month = '09' THEN claim_vol END), 0) as decimal (10,2))  AS 九月,
    cast(NVL(SUM(CASE WHEN dt_month = '10' THEN claim_vol END), 0) as decimal (10,2))  AS 十月,
    cast(NVL(SUM(CASE WHEN dt_month = '11' THEN claim_vol END), 0)  as decimal (10,2)) AS 十一月,
    cast(NVL(SUM(CASE WHEN dt_month = '12' THEN claim_vol END), 0)  as decimal (10,2)) AS 十二月
FROM (
    SELECT
        渠道,
        年月,
        '日计划平均进件量' AS 指标,
        日计划平均进件量 AS claim_vol,
        dt_month
    FROM pp
    UNION ALL
    SELECT
        渠道,
        年月,
        '日实际平均进件量' AS 指标,
        日实际平均进件量 AS claim_vol,
        dt_month
    FROM pp
    UNION ALL
    SELECT
        渠道,
        年月,
        '日平均进件量差值' AS 指标,
        日平均进件量差值 AS claim_vol,
        dt_month
    FROM pp
    UNION ALL
    SELECT
        渠道,
        年月,
        '月累积差值' AS 指标,
        月累积差值 AS claim_vol,
        dt_month
    FROM pp
) AS unpivoted_data
WHERE SUBSTR(年月, 1, 4) >= '2023' AND 渠道 IS NOT NULL
GROUP BY 渠道, SUBSTR(年月, 1, 4), 指标;

"""
def truncate_table(table_name='CLAIM_DWS.DWS_OPERATION_PLAN_INFO'):
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
