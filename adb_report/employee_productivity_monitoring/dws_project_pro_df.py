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

        # 删除当天数据
        sql_fff = f""" delete  from  claim_dws.dws_project_pro_df """
        cursor.execute(sql_fff)
        # 将结果数据写入结果表
        sql_13 = f"""insert into claim_dws.dws_project_pro_df
SELECT
    cast(ROW_NUMBER() OVER (ORDER BY work_hours_factor DESC) as int) AS rank,
  group_name,
  cast(
    CASE
        WHEN EXTRACT(HOUR FROM NOW()) < 9 THEN 0
        WHEN EXTRACT(HOUR FROM NOW()) >= 9 AND EXTRACT(HOUR FROM NOW()) <= 11 THEN work_hours_factor*60 /(((EXTRACT(HOUR FROM NOW()) - 9) * 60 + EXTRACT(MINUTE FROM NOW())) * staff_count)
        WHEN EXTRACT(HOUR FROM NOW()) > 11 AND EXTRACT(HOUR FROM NOW()) <13 THEN  work_hours_factor/ (3*staff_count)
        WHEN EXTRACT(HOUR FROM NOW()) >= 13 AND EXTRACT(HOUR FROM NOW()) <= 18 THEN work_hours_factor*60 / (((EXTRACT(HOUR FROM NOW()) - 10) * 60 + EXTRACT(MINUTE FROM NOW())) * staff_count)
        WHEN EXTRACT(HOUR FROM NOW()) > 18 THEN work_hours_factor/ (8*staff_count)
        ELSE 0
    END
AS DECIMAL(10,2)
)  AS performance_coefficient, -- 或者适当调整精度和刻度
    cast(work_hours_factor as DECIMAL(10,2)) ,
    TIME_FORMAT(CURRENT_TIME, '%H:%i') AS update_time,
    DATE_FORMAT(now(), '%Y%m%d')
FROM (
  SELECT
    group_name,
    SUM(cnt * work_hours_factor) AS work_hours_factor,
    COUNT(DISTINCT STAFF_NAME) AS staff_count
  FROM (
    SELECT
      CASE
        WHEN STAFF_NAME IN ('周艳玲','惠树苗','李静','孙娅娟A','李磊','陈浩','冯旭超','张颖','常佳','孙娅娟','吴慧','肖明可','党蕊苗','高琳琳','葛曼菲','党杨妮','程欣','陈君萍','甘翠萍','王倩','雷艳') THEN '泰康项目'
        WHEN STAFF_NAME IN ('杨瑾','程妙','王盈博','郭亚佩','张海侠') THEN '平安财'
        WHEN STAFF_NAME IN ('王莎莎','詹千','葛琼洁','王瑞','王卡卡','曾瑛','闵哲') THEN '暖哇科技'
        WHEN STAFF_NAME IN ('张秀秀','唐萌花','王菲菲','王娟','张迪','安少佳') THEN '中智'
        WHEN STAFF_NAME IN ('周乔怡') THEN '大家养老'
        WHEN STAFF_NAME IN ('刘硕') THEN '湖南医惠保'
        WHEN STAFF_NAME IN ('张瑞丽') THEN '太保产险+渤海人寿'
        WHEN STAFF_NAME IN ('樊宵雅','雒佳楠','毛梦泽') THEN '太保健康'
        WHEN STAFF_NAME IN ('郑佩佩','杨斐') THEN '国寿财'
        WHEN STAFF_NAME IN ('韦露','张萧','张罕','任妮妮') THEN '标准化'
        ELSE ''
      END AS group_name,
      cnt,
      work_hours_factor,
      STAFF_NAME
    FROM claim_dws.dws_employee_comp_num where  dt=DATE_FORMAT(now(), '%Y%m%d')
  ) ff
  WHERE group_name <> ''
  GROUP BY group_name
) t1; """
        cursor.execute(sql_13)

        # 获取结果（如果适用）
        # sql_abc_lll = f""" select *  from tmp_final"""
        # cursor.execute(sql_abc_lll)
        # result = cursor.fetchall()
        # print(result)
        print("success")


finally:
    # 关闭数据库连接
    connection.close()
    end = datetime.now()
    end_time = end.strftime("%Y-%m-%d %H:%M:%S")
    # 输出当前时间
    print("程序结束时间：", end_time)
