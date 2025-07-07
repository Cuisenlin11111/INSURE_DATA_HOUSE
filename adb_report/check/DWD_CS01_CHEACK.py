import requests
import sys
# sys.path.append(r"E:\pycharm\database")
from database import DatabaseConnection

# 更新后的 SQL 查询语句
sql_query = f"""
   select p.apply_policy_no from claim_ods.claim_policy p 
   where INSURE_COMPANY_CHANNEL = 'CS01' and p.extra not like '%mainInsuredRelation%' ;
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
        message = "hi,早上好     查询结果如下：\n"
        for row in results:
            # 假设查询结果是字典形式
            if isinstance(row, dict):
                apply_policy_no = row.get('apply_policy_no')
                if apply_policy_no:
                    # 打印每个结果的详细信息，用于调试
                    print(f"apply_policy_no: {apply_policy_no}")
                    message += f"apply_policy_no: {apply_policy_no}\n"
                else:
                    print("未找到 apply_policy_no 字段")
            else:
                print(f"Unexpected row format: {row}")

        # 钉钉机器人URL
        webhook_url = "https://oapi.dingtalk.com/robot/send?access_token=9ab2edc4c6731e4d18ebccc1b359e64fac86da8751b074dcd022555ed541cf0e"

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
