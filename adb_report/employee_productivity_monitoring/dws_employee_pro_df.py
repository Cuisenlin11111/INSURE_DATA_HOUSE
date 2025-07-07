# import sys
# sys.path.append(r"E:\pycharm\database")
from database import DatabaseConnection
from datetime import datetime



sql_query = f"""
  -- @description: 人工扣费原因分析表
  -- @param:
  -- @return:
  -- @alter:
  -- @time: 2024-10-12 15:01:06
  -- @author: 01
  -- @version: 1.0.0

insert into claim_dws.dws_employee_pro_df
SELECT
      cast(rank() OVER (ORDER BY work_hours_factor DESC) as int) AS rank_num,
            CASE
        WHEN STAFF_NAME IN ('周艳玲','惠树苗','李静','孙娅娟A','李磊','陈浩','冯旭超','张颖','常佳','孙娅娟','吴慧','肖明可','党蕊苗','高琳琳','葛曼菲','党杨妮','程欣','陈君萍','甘翠萍','王倩','雷艳''谢彩玉',
'路婷婷',
'李雪') THEN '泰康项目'
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
    STAFF_NAME AS name,
cast(
   CASE
    WHEN EXTRACT(HOUR FROM NOW()) < 9 THEN 0
    WHEN EXTRACT(HOUR FROM NOW()) >= 9 AND EXTRACT(HOUR FROM NOW()) <= 11 THEN
        CASE
            WHEN STAFF_NAME = '毛梦微' THEN
                work_hours_factor * 60 / (((EXTRACT(HOUR FROM NOW()) - 9) * 60 * 0.3 + EXTRACT(MINUTE FROM NOW())) * 0.3)
--             WHEN STAFF_NAME = '任妮妮' THEN
--                 work_hours_factor * 60 / (((EXTRACT(HOUR FROM NOW()) - 9) * 60 * 0.8 + EXTRACT(MINUTE FROM NOW())) * 0.8)
                WHEN STAFF_NAME = '党蕊苗' THEN
                work_hours_factor * 60 / (((EXTRACT(HOUR FROM NOW()) - 9) * 60 * 0.2 + EXTRACT(MINUTE FROM NOW())) * 0.2)
            ELSE
                work_hours_factor * 60 / ((EXTRACT(HOUR FROM NOW()) - 9) * 60 + EXTRACT(MINUTE FROM NOW()))
        END
    WHEN EXTRACT(HOUR FROM NOW()) > 11 AND EXTRACT(HOUR FROM NOW()) < 13 THEN
        CASE
            WHEN STAFF_NAME = '毛梦微' THEN
                work_hours_factor / (3 * 0.3 )
--                 WHEN STAFF_NAME = '任妮妮' THEN
--                 work_hours_factor / (3 * 0.8 )
                WHEN STAFF_NAME = '党蕊苗' THEN
                work_hours_factor / (3 * 0.2 )
            ELSE
                work_hours_factor / 3
        END
    WHEN EXTRACT(HOUR FROM NOW()) >= 13 AND EXTRACT(HOUR FROM NOW()) < 18 THEN
        CASE
            WHEN STAFF_NAME = '毛梦微' THEN
                work_hours_factor * 60 / (((EXTRACT(HOUR FROM NOW()) - 10) * 60 * 0.3 + EXTRACT(MINUTE FROM NOW())) * 0.3)
--             WHEN STAFF_NAME = '任妮妮' THEN
--                 work_hours_factor * 60 / (((EXTRACT(HOUR FROM NOW()) - 10) * 60 * 0.8 + EXTRACT(MINUTE FROM NOW())) * 0.8)
                WHEN STAFF_NAME = '党蕊苗' THEN
                work_hours_factor * 60 / (((EXTRACT(HOUR FROM NOW()) - 10) * 60 * 0.2 + EXTRACT(MINUTE FROM NOW())) * 0.2)
            ELSE
                work_hours_factor * 60 / ((EXTRACT(HOUR FROM NOW()) - 10) * 60 + EXTRACT(MINUTE FROM NOW()))
        END
    WHEN EXTRACT(HOUR FROM NOW()) >= 18 THEN
        CASE
            WHEN STAFF_NAME = '毛梦微' THEN
                work_hours_factor / (8 * 0.3 )
--                 WHEN STAFF_NAME = '任妮妮' THEN
--                 work_hours_factor / (8 * 0.8 )
                WHEN STAFF_NAME = '党蕊苗' THEN
                work_hours_factor / (8 * 0.2 )
            ELSE
                work_hours_factor / 8
        END
    ELSE 0
END
AS DECIMAL(10,2)
)  AS performance_coefficient, -- 或者适当调整精度和刻度
    cast(work_hours_factor as DECIMAL(10,2)) ,
    TIME_FORMAT(CURRENT_TIME, '%H:%i') AS update_time,
    DATE_FORMAT(now(), '%Y%m%d')
FROM ( select  STAFF_NAME,sum(cnt*work_hours_factor) as  work_hours_factor  from    claim_dws.dws_employee_comp_num   aa
            where  dt=DATE_FORMAT(now(), '%Y%m%d')
          group by   STAFF_NAME)  ff  where work_hours_factor is not null and STAFF_NAME in ('温茜',
'程欣',
'惠树苗',
'周艳玲',
'张倩',
'秦露',
'郑丽平',
'李欢欢',
'葛丹红',
'杨瑾',
'程妙',
'王盈博',
'郭亚佩',
'张海侠',
'王莎莎',
'詹千',
'葛琼洁',
'王瑞',
'王卡卡',
'曾瑛',
'闵哲',
'王娜英',
'张秀秀',
'唐萌花',
'王菲菲',
'王娟',
'张迪',
'安少佳',
'周乔怡',
'张瑞丽',
'樊宵雅',
'韦露',
'张萧',
'张罕',
'李静',
'孙娅娟A',
'李磊',
'陈浩',
'冯旭超','张颖','常佳','任妮妮','雷艳','孙娅娟','吴慧','肖明可','党蕊苗','高琳琳','郑佩佩','杨斐','雒佳楠','毛梦泽','葛曼菲','党杨妮','陈君萍','甘翠萍','王倩''谢彩玉',
'路婷婷','刘硕',
'李雪')

"""
def truncate_table(table_name='claim_dws.dws_employee_pro_df'):
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
