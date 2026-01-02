import os

from typing import List, Dict

from src.dal_util.pg_db_conn import DatabaseConnection
from src.stocks_data_reader.abstractions import StocksDataReader


class SQLDataReader(StocksDataReader):
    def __init__(self, db: DatabaseConnection):
        self.db = db

    def read_data(self) -> List[Dict[str, str]]:
        sql = """
        SELECT * FROM stocks 
        """
        conn = self.db.get_connection()
        with conn.cursor() as cursor:
            cursor.execute(sql)
            rows = cursor.fetchall()
        print(rows)
        all_stocks = []
        for row in rows:
            all_stocks.append(dict(row))
        return all_stocks

    def read_data_by_industry(self, industry: str) -> List[Dict[str, str]]:
        sql = """
        SELECT * FROM stocks 
        WHERE industry = '{}'
        """.format(industry)
        conn = self.db.get_connection()
        with conn.cursor() as cursor:
            cursor.execute(sql)
            rows = cursor.fetchall()
        print(rows)
        all_stocks = []
        for row in rows:
            all_stocks.append(dict(row))
        return all_stocks


if __name__ == "__main__":
    db = DatabaseConnection(
        dbname=os.environ['DBNAME'],
        user=os.environ['USER'],
        password=os.environ['PASSWORD'],
        host=os.environ['HOST'],
        port=os.environ['PORT']
    )
    sql_data_reader = SQLDataReader(db=db)
    # print(sql_data_reader.read_data())
    print(sql_data_reader.read_data_by_industry("Financial Services"))