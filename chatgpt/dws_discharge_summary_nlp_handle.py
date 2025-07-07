  # -*- coding: utf-8 -*-

import requests
import json
import re
import pymysql
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


def get_access_token():
    """
    使用 API Key：N4LZz3a9DQBgsylKjyJQ4jrn
     Secret Key：PkXRqIOZwMZ9DOZqqNHBUOgcy4zEozoY
     获取access_token，替换下列示例中的应用API Key、应用Secret Key
    """

    url = "https://aip.baidubce.com/oauth/2.0/token?grant_type=client_credentials&client_id=N4LZz3a9DQBgsylKjyJQ4jrn&client_secret=PkXRqIOZwMZ9DOZqqNHBUOgcy4zEozoY"

    payload = json.dumps("")
    headers = {
        'Content-Type': 'application/json',
        'Accept': 'application/json'
    }

    response = requests.request("POST", url, headers=headers, data=payload)
    return response.json().get("access_token")


def main(text):
    url = "https://aip.baidubce.com/rpc/2.0/ai_custom/v1/wenxinworkshop/chat/ernie-lite-8k?access_token=" + get_access_token()

    payload = json.dumps({
        "messages": [
            {
                "role": "user",
                "content": text
            }
        ]
    })
    headers = {
        'Content-Type': 'application/json'
    }

    try:
        response = requests.request("POST", url, headers=headers, data=payload)
        bb = response.text
        data = json.loads(bb)

        # 添加错误处理
        if 'error_code' in data:
            print(f"Error occurred: {data['error_msg']}")
            return None  # 或者返回一个默认值或错误标志

        result_content = data.get('result')
        if result_content is None:
            print("Result not found in the response.")
            return None

        return result_content
    except Exception as e:
        # 返回或记录错误信息
        print(f"An error occurred: {str(e)}")
        return None
    #print(result_content)

if __name__ == '__main__':
    #main(text)
    try:
        # 创建一个新的游标来执行查询
        with connection.cursor() as cursor_select:
            # 执行 SQL 查询以获取原始数据
            sql_select = f"""
            SELECT 
                claim_no,
                first_insure_date,
                image_url,
                recognized_text,
                treatment_date,
                admission_date,
                is_major_disease,
                original_diagnosis_code,
                original_diagnosis_name,
                matched_diagnosis_code,
                matched_diagnosis_name,
                auto_diagnosis_code,
                auto_diagnosis_name,
                is_medical_history,
                past_medical_history,
                diagnosis_year,
                insurance_company_channel,
                dt
            FROM claim_dws.dws_discharge_summary_encrypt_df_new where dt>='20240726'  ;
            """
            cursor_select.execute(sql_select)

            # 将所有行读取到列表中
            rows = cursor_select.fetchall()

        # 准备插入语句，在循环外定义
        sql_insert = f"""
        INSERT INTO 
            claim_dws.dws_discharge_summary_nlp_handle (
                claim_no,
                first_insure_date,
                image_url,
                recognized_text,
                treatment_date,
                admission_date,
                is_major_disease,
                original_diagnosis_code,
                original_diagnosis_name,
                matched_diagnosis_code,
                matched_diagnosis_name,
                auto_diagnosis_code,
                auto_diagnosis_name,
                is_medical_history,
                past_medical_history,
                diagnosis_year,
                insurance_company_channel,
                dt
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """

        # 定义一个空列表来存储处理后的数据
        processed_rows = []

        # 处理每一行，并准备插入数据
        for row in rows:
            recognized_text = row[3]

            # 使用正则表达式替换身份证号
            text =  recognized_text + '  整理上述文本信息,信息尽可能要全，不要漏掉信息 ,以json 格式返回信息,不需要多余提示信息，json 内不需要注释 结构信息主要包含 1患者信息（包含姓名，住院号，身份证号等）;2主诉信息;3症状信息;4检查信息;5病史信息（现病史、既往史）;6治疗信息（治疗经过等）7诊断信息（入院诊断，出院诊断等）'
            #print(text)
            #result_f = main(text).replace('json' , '').replace( '```', '')
            result_from_main = main(text)
            if result_from_main is not None and isinstance(result_from_main, str):
                result_f = result_from_main.replace('json' , '').replace( '```', '')
            else:
                # 处理 None 或非字符串的情况，例如设置默认值或抛出异常
                result_f = ''  # 或者你可以决定抛出一个异常
            #print(result_f)

            # 准备数据用于插入
            insert_data = (
                row[0],
                row[1],
                row[2],
                result_f,  # 加密后的 recognized_text
                row[4],
                row[5],
                row[6],
                row[7],
                row[8],
                row[9],
                row[10],
                row[11],
                row[12],
                row[13],  # 调整列名以匹配表结构
                row[14],
                row[15],
                row[16],
                row[17]
            )
            processed_rows.append(insert_data)

            # 当收集了1000行数据时，执行批量插入
            if len(processed_rows) == 1000:
                with connection.cursor() as cursor_insert:
                    cursor_insert.executemany(sql_insert, processed_rows)
                connection.commit()
                processed_rows = []

        # 如果最后剩下的数据不足1000行，也要进行插入
        if processed_rows:
            with connection.cursor() as cursor_insert:
                cursor_insert.executemany(sql_insert, processed_rows)
            connection.commit()

        print('成功')

    finally:
        # 关闭数据库连接
        connection.close()
        end = datetime.now()
        end_time = end.strftime("%Y-%m-%d %H:%M:%S")
        print("程序结束时间:", end_time)