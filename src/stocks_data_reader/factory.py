import os

from src.stocks_data_reader.abstractions import StocksDataReader
from src.stocks_data_reader.sql_data_reader import SQLDataReader
from src.stocks_data_reader.file_data_reader import FileDataReader
from src.dal_util.pg_db_conn import DatabaseConnection


class DataReaderFactory:
    @staticmethod
    def get_stocks_data_reader(data_store: str) -> StocksDataReader:
        if data_store == "file":
            return FileDataReader()
        elif data_store == "sql":
            db = DatabaseConnection(
                dbname=os.environ['DBNAME'],
                user=os.environ['USER'],
                password=os.environ['PASSWORD'],
                host=os.environ['HOST'],
                port=os.environ['PORT']
            )
            return SQLDataReader(db)
        else:
            raise NotImplementedError
