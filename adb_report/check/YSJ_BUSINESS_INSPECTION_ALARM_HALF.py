# 导入相关模块
# import sys
# sys.path.append(r"E:\pycharm\database")
from database import DatabaseConnection
from datetime import datetime
from datetime import date
import requests  # 用于发送钉钉消息
import configparser  # 如果使用config.ini
import time
import os

# 获取今天的日期并格式化
today = date.today()
formatted_today = today.strftime("%Y-%m-%d")

def read_sql_file(file_path):
    try:
        with open(file_path, 'r', encoding='utf-8') as file:
            sql_query = file.read()
        return sql_query.format(formatted_date=formatted_today)
    except Exception as e:
        print(f"读取 SQL 文件失败: {e}")
        return None

def execute_query_and_check_results(sql_query, file_name, alert_message_template):
    with DatabaseConnection() as conn:
        with conn.cursor() as cursor:
            cursor.execute(sql_query)
            results = cursor.fetchall()
            if results:
                print(f"数据异常: {file_name}")
                # 构建包含异常值的告警信息
                alert_message = alert_message_template.format(
                    异常描述=f"{file_name}数据异常",
                    异常渠道列表=" ".join([row.get('INSURE_COMPANY_CHANNEL') for row in results if row.get('INSURE_COMPANY_CHANNEL')])
                )
                send_dingtalk_alert(alert_message)
            return len(results) == 0

def send_dingtalk_alert(message):
    """发送钉钉消息的函数"""
    url = "https://oapi.dingtalk.com/robot/send?access_token=a5ade45ee1ad3def47b5690918e5163b590da9c9749aeb3332326af7d4072313"
    payload = {
        "msgtype": "text",
        "text": {
            "content": message
        }
    }
    try:
        response = requests.post(url, json=payload)
        response.raise_for_status()
    except requests.RequestException as e:
        print(f"发送钉钉消息失败: {e}")

if __name__ == "__main__":
    start_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    print("程序开始时间：", start_time)


    # 获取当前脚本所在目录，以此为基础构建配置文件的相对路径
    config_file_path = os.path.join(os.path.dirname(__file__), 'half_check_config.ini')

    # 读取配置文件（这里以config.ini为例，使用yaml时需修改）
    config = configparser.ConfigParser()
    config.read(config_file_path, encoding='utf-8')

    all_success = True

    for section in config.sections():
        sql_file_path = config[section]['sql_file_path']
        print(sql_file_path)
        file_name = config[section]['file_name']
        alert_message_template = config[section]['alert_message_template']
        sql_query = read_sql_file(sql_file_path)
        if sql_query:
            success = execute_query_and_check_results(sql_query, file_name, alert_message_template)
            time.sleep(2)
            all_success &= success

    end_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    print("程序结束时间：", end_time)