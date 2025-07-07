import pymysql
from datetime import datetime
import pandas as pd

# 阿里云AnalyticDB for MySQL的相关参数
host = 'am-uf61afo16ust6f600167320.ads.aliyuncs.com'  # 替换为您的AnalyticDB实例的endpoint
port = 3306  # 或者使用实际提供的端口号
user = 'claim_all'
password = 'S#5DH1ar%*1n'
db_name = 'claim_dim'

## 手动插入数据


# 创建连接
connection = pymysql.connect(host=host,
                             port=port,
                             user=user,
                             password=password,
                             db=db_name,
                             charset='utf8mb4')  # 可根据实际情况指定字符集

excel_file_path = r'C:\Users\hjkj-028\Desktop\王恒\导入数据\太保财上海分公司10月.xlsx'

now = datetime.now()
start_time = now.strftime("%Y-%m-%d %H:%M:%S")
# 输出当前时间
print("程序开始时间：", start_time)

try:
    # 使用 pandas 读取 Excel 文件
    df = pd.read_excel(excel_file_path)

    # 创建游标对象
    with connection.cursor() as cursor:
        # 循环遍历 DataFrame 中的每一行
        for index, row in df.iterrows():
            # 构建 SQL 插入语句
            columns = ', '.join(row.index)
            values = ', '.join(['%s'] * len(row))
            sql = f"INSERT INTO CLAIM_DWD.DWD_CP07_DETAIL ({columns}) VALUES ({values})"

            # 执行 SQL 插入语句
            cursor.execute(sql, tuple(row))

        # 提交事务
        connection.commit()
        print("成功插入数据")

finally:
    # 关闭数据库连接
    connection.close()
    end = datetime.now()
    end_time = end.strftime("%Y-%m-%d %H:%M:%S")
    # 输出当前时间
    print("程序结束时间：", end_time)