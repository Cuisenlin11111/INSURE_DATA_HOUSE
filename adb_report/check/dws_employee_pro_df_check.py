# import sys
# sys.path.append(r"E:\pycharm\database")
from database import DatabaseConnection
from datetime import datetime
from datetime import date

today = date.today()
formatted_today = today.strftime("%Y-%m-%d")


def read_sql_file(file_path):
    """读取SQL文件，并替换其中的日期占位符"""
    with open(file_path, 'r',encoding='utf-8') as file:
        sql_query = file.read()
    return sql_query.format(formatted_date=formatted_today)


def execute_query_and_check_results(sql_query, file_name):
    """执行SQL查询，并检查结果是否正常"""
    with DatabaseConnection() as conn:
        with conn.cursor() as cursor:
            cursor.execute(sql_query)
            results = cursor.fetchall()
            if results:
                print(f"数据异常: {file_name}")
                return False
            else:
                print(f"数据正常: {file_name}")
                return True


if __name__ == "__main__":
    start_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    print("程序开始时间：", start_time)

    # 假设所有的SQL文件都在同一个目录下，这里列出它们的文件名
    sql_file_paths = [
        'employee_pro_df_check.sql','ADM_PROJECT_ROFIT_M_CHECK.sql'
        # 添加更多SQL文件名...
    ]

    all_queries_successful = True
    for sql_file_path in sql_file_paths:
        sql_query = read_sql_file(sql_file_path)
        success = execute_query_and_check_results(sql_query, sql_file_path)
        all_queries_successful = all_queries_successful and success

    end_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    print("程序结束时间：", end_time)

