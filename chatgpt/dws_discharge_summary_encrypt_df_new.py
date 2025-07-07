import pymysql
import re
import configparser
from datetime import datetime

# config = configparser.ConfigParser()
#
# # 读取配置文件
# config.read('/opt/adb_report/config.ini')
# 阿里云AnalyticDB for MySQL的相关参数
host = 'am-uf61afo16ust6f600167320.ads.aliyuncs.com'  # 替换为您的AnalyticDB实例的endpoint
port = 3306  # 或者使用实际提供的端口号
user = 'claim_all'
password = 'S#5DH1ar%*1n'
db_name = 'claim_dws'

# 连接到数据库
# 创建连接
connection = pymysql.connect(host=host,
                             port=port,
                             user=user,
                             password=password,
                             db=db_name,
                             charset='utf8mb4')  # 可根据实际情况指定字符集
now = datetime.now()
start_time = now.strftime("%Y-%m-%d %H:%M:%S")

print("程序开始时间：", start_time)






try:
    # 创建一个新的游标来执行查询
    with connection.cursor() as cursor_select:
        # 执行SQL查询以获取原始数据
        sql_select = """  
select
    claim_no ,
    first_insure_date,
    image_url ,
    recognized_text ,
    treatment_date ,
    admission_date ,
    is_major_disease ,
    original_diagnosis_code ,
    original_diagnosis_name ,
    matched_diagnosis_code ,
    matched_diagnosis_name,
    auto_diagnosis_code ,
    auto_diagnosis_name ,
    has_medical_history ,
    past_medical_history ,
    diagnosis_year,
    insurance_company_channel ,
    dt
from claim_dws.dws_discharge_summary_df where  dt>='20240726' ;
        """
        cursor_select.execute(sql_select)

        # 创建一个新的游标来执行插入操作
        with connection.cursor() as cursor_insert:
            # 准备插入语句
            sql_insert = """  
            INSERT INTO   
                claim_dws.dws_discharge_summary_encrypt_df_new (  
                        claim_no ,
                        first_insure_date,
                        image_url ,
                        recognized_text ,
                        treatment_date ,
                        admission_date ,
                        is_major_disease ,
                        original_diagnosis_code ,
                        original_diagnosis_name ,
                        matched_diagnosis_code ,
                        matched_diagnosis_name,
                        auto_diagnosis_code ,
                        auto_diagnosis_name ,
                        is_medical_history ,
                        past_medical_history ,
                        diagnosis_year,
                        insurance_company_channel ,
                        dt
                ) VALUES (%s, %s,%s, %s, %s,%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)    
            """
            insert_data_list = []
            # 遍历查询结果并处理数据
            for row in cursor_select.fetchall():
                recognized_text = row[3]
                # 使用正则表达式替换身份证号
                id_card_pattern = re.compile(r'(\d{4})\d{10}(\d{4})')
                masked_text = id_card_pattern.sub(lambda m: f"{m.group(1)}***{m.group(2)}", recognized_text)
                phone_pattern = re.compile(r'1\d{10}')  # 匹配以1开头的11位数字，即典型的中国大陆手机号格式
                masked_text_fal = phone_pattern.sub(lambda m: f"{m.group(0)[:3]}****{m.group(0)[-4:]}", masked_text)
                cleaned_text = re.sub(r'[^\x00-\x7F\u4e00-\u9fa5]+', '', masked_text_fal)

                # 去除指定的特殊字符（如@、#），但保留其他特殊字符如¥%
                final_text = re.sub(r'[@#&¥]', '', cleaned_text)
                result_a = re.sub(r"姓名(\w{2,3})", "姓名张三;", final_text)
                # 替换 8 位数字的后四位
                result_b = re.sub(r"(\d{4})\d{4}", r"\1****", result_a)
                result_c = re.sub(r";名(\w{2,3})\;", ";姓名李四;", result_b)
                result_d =re.sub(r"姓名 (\w{2,3})", "姓名张三;", result_c)
                result_e = re.sub(r"姓名:(\w{2,3})", "姓名张三;", result_d)
                result_f = re.sub(r"姓名：(\w{2,3})", "姓名张三;", result_e)



                # 准备插入的数据
                insert_data = (
                    row[0],
                    row[1],
                    row[2],
                    result_f,  # 加密后的recognized_text
                    row[4],
                    row[5],
                    row[6],
                    row[7],
                    row[8],
                    row[9],
                    row[10],
                    row[11],
                    row[12],
                    row[13],
                    row[14],
                    row[15],
                    row[16],
                    row[17]
                )
                insert_data_list.append(insert_data)
                # 执行插入操作
                #print(sql_insert)
            cursor_insert.executemany(sql_insert, insert_data_list)

    # 提交事务
    connection.commit()
    print('success')

    connection.commit()
    print('success')

finally:
    # 关闭数据库连接
    connection.close()
    end = datetime.now()
    end_time = end.strftime("%Y-%m-%d %H:%M:%S")
    print("程序结束时间：", end_time)