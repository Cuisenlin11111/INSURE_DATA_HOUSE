import requests
import sys
# sys.path.append(r"E:\pycharm\database")
from database import DatabaseConnection
from datetime import datetime, date, timedelta
now = datetime.now()
# 计算60天前的日期时间
ago_60_days = now - timedelta(days=7)
# 格式化为指定的字符串格式

# 格式化为指定的字符串格式
formatted_date = ago_60_days.strftime('%Y-%m-%d')

sql_query = f"""
   select   DISTINCT PR.accept_num,PR.app_no from claim_ods.postback_record pr
         left join claim_ods.tk07_half_group_postback  thg  on  pr.accept_num=thg.accept_num   AND thg.is_deleted='N'
         where  thg.accept_num     is   null
and             pr.insure_company_channel='TK07' AND   postback_way='H'
           AND  PR.is_deleted='N'  AND SUBSTR(PR.back_time,1,10)>='{formatted_date}'
   and  pr.back_status='2'
"""


def insert_data(sql_query):
    try:
        with DatabaseConnection() as conn:
            with conn.cursor() as cursor:
                cursor.execute(sql_query)
                results = cursor.fetchall()
                conn.commit()
                # 打印查询结果，用于调试
                print("查询结果：", results)

        # 如果查询结果为空，直接返回，不发送消息
        if not results:
            print("查询结果为空，不发送消息。")
            return

        # 构建钉钉机器人消息
        message = "好久不见，甚是想念！     查询结果如下：\n"
        for row in results:
            # 确认 row 是字典类型
            if isinstance(row, dict):
                accept_num = row.get('accept_num')
                app_no = row.get('app_no')
                # 打印每个结果的详细信息，用于调试
                print(f"accept_num: {accept_num}, app_no: {app_no}")
                message += f"accept_num: {accept_num}, app_no: {app_no}\n"
            else:
                print(f"Unexpected row format: {row}")

        # 钉钉机器人URL
        webhook_url = "https://oapi.dingtalk.com/robot/send?access_token=ab92673d04431f36d5bf8cf75dad70aebbd9f1a2e84050b82dbaad674009c8a3"

        # 发送钉钉消息
        headers = {
            "Content-Type": "application/json"
        }
        data = {
            "msgtype": "text",
            "text": {
                "content": message
            }
        }
        response = requests.post(webhook_url, headers=headers, json=data)
        if response.status_code == 200:
            print("消息发送成功")
        else:
            print(f"消息发送失败，状态码: {response.status_code}，响应内容: {response.text}")
    except Exception as e:
        print(f"发生错误: {e}")


if __name__ == "__main__":
    insert_data(sql_query)