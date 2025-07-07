import pymysql
from datetime import datetime
import openpyxl

# 阿里云AnalyticDB for MySQL的相关参数
host = 'am-uf61afo16ust6f600167320.ads.aliyuncs.com'
port = 3306
user = 'claim_all'
password = 'S#5DH1ar%*1n'
db_name = 'claim_dim'

# 创建连接
connection = pymysql.connect(host=host,
                             port=port,
                             user=user,
                             password=password,
                             db=db_name,
                             charset='utf8mb4')

xlsx_file_path = r'C:\Users\hjkj-028\Desktop\项目人力配置.xlsx'

now = datetime.now()
start_time = now.strftime("%Y-%m-%d %H:%M:%S")
# 输出当前时间
print("程序开始时间：", start_time)

try:
    # 创建游标对象
    with connection.cursor() as cursor:
        workbook = openpyxl.load_workbook(xlsx_file_path)
        sheet = workbook.active  # 假设我们读取的是第一个工作表
        headers = [cell.value for cell in sheet[1]]  # 获取标题行

        for row in sheet.iter_rows(min_row=2, values_only=True):  # 从第二行开始读取数据
            data_dict = dict(zip(headers, row))  # 将行转换为字典

            # 构造SQL语句（这里假设XLSX文件的列名和数据库表的列名相同）
            sql = "INSERT INTO claim_dim.DIM_MANPOWER_CONFIG (%s) VALUES (%s)"
            columns = ', '.join(data_dict.keys())
            values = ', '.join(['%s'] * len(data_dict))
            data_dict['CLAIM_OPERATE_MONEY'] = int(data_dict['CLAIM_OPERATE_MONEY'].replace('=', '').replace('"', ''))
            cursor.execute(sql % (columns, values), tuple(data_dict.values()))

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