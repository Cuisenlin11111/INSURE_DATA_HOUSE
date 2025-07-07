import pymysql
import sys
sys.path.append(r"D:\因朔桔智能科技有限公司\pycharm\pycharm\config")
from config import load_config
import pandas as pd

class DatabaseConnection:
    def __init__(self):
        self.config = load_config()
        self.connection = None

    def __enter__(self):
        self.connection = pymysql.connect(
            host=self.config['host'],
            port=self.config['port'],
            user=self.config['user'],
            password=self.config['password'],
            charset='utf8mb4'
        )
        return self  # 返回self而不是self.connection

    def __exit__(self, exc_type, exc_val, exc_tb):
        if self.connection:
            self.connection.close()

    def execute_query_to_dataframe(self, query):
        """
        Execute an SQL query and return the results as a pandas DataFrame.
        :param query: SQL query string
        :return: pandas DataFrame containing the result set
        """
        with self.connection.cursor(pymysql.cursors.DictCursor) as cursor:
            cursor.execute(query)
            result = cursor.fetchall()
            df = pd.DataFrame(result)
        return df

    def cursor(self):
        """
        Returns a new cursor object using the connection's cursor method.
        """
        return self.connection.cursor(pymysql.cursors.DictCursor)

    def commit(self):
        """
        Commits the current transaction.
        """
        self.connection.commit()


# if __name__ == '__main__':
#     with DatabaseConnection() as db:
#         print("Connected to database.")