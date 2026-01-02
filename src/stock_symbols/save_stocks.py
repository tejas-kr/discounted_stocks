import os
import glob
import csv

from typing import List, Dict, Protocol
from psycopg2.extras import execute_values
from pathlib import Path

from src.dal_util.pg_db_conn import DatabaseConnection
from src.consts import ALL_STOCKS_LIST_CSV


class IDataSource(Protocol):
    def get_data(self) -> List[Dict[str, str]]:
        ...


class IDataProcessor(Protocol):
    def process_data(self, data: List[Dict[str, str]]) -> List[Dict[str, str]]:
        ...


class IDataSaver(Protocol):
    def save_data(self, data: List[Dict[str, str]]) -> None:
        ...


class CSVDataSource:
    def __init__(self, directory: str = "./csvs"):
        self.directory = directory

    def get_data(self) -> List[Dict[str, str]]:
        csv_files = glob.glob(f"{self.directory}/*.csv")
        combined_data: List[Dict[str, str]] = []

        for csv_path in csv_files:
            with open(csv_path, mode="r", encoding="utf-8") as file:
                reader = csv.DictReader(file)
                for row in reader:
                    combined_data.append(dict(row))
        return combined_data


class StockDataProcessor:
    def process_data(self, data: List[Dict[str, str]]) -> List[Dict[str, str]]:
        # Remove duplicates based on all fields
        return list({
            tuple(sorted(d.items())): d
            for d in data
        }.values())


class DatabaseDataSaver:
    def __init__(self, connection):
        self.connection = connection

    def save_data(self, data: List[Dict[str, str]]) -> None:
        query = """
        INSERT INTO stocks (symbol, company_name, industry, isin)
        VALUES %s
        ON CONFLICT (symbol) DO NOTHING;
        """
        values = [
            (row['Symbol'], row['Company Name'], row['Industry'], row['ISIN Code'])
            for row in data
        ]
        with self.connection.cursor() as cursor:
            execute_values(cursor, query, values)
        self.connection.commit()


class FileDataSaver:
    def __init__(self):
        self.all_stocks_list_path = Path(ALL_STOCKS_LIST_CSV)

    def save_data(self, data: List[Dict[str, str]]) -> None:
        values = [
            {
                "symbol": row["Symbol"],
                "company_name": row["Company Name"],
                "industry": row["Industry"],
                "isin": row["ISIN Code"],
            }
            for row in data
        ]

        fieldnames = ["symbol", "company_name", "industry", "isin"]

        with self.all_stocks_list_path.open(mode="w", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=fieldnames)
            writer.writeheader()
            writer.writerows(values)


if __name__ == "__main__":
    # Dependency injection setup
    data_source: IDataSource = CSVDataSource()
    data_processor: IDataProcessor = StockDataProcessor()

    db = DatabaseConnection(
        dbname=os.environ['DBNAME'],
        user=os.environ['USER'],
        password=os.environ['PASSWORD'],
        host=os.environ['HOST'],
        port=os.environ['PORT']
    )
    connection = db.get_connection()
    pg_sql_data_saver: IDataSaver = DatabaseDataSaver(connection)

    # Orchestrate the process
    raw_data = data_source.get_data()
    processed_data = data_processor.process_data(raw_data)
    pg_sql_data_saver.save_data(processed_data)
    db.close_connection()

    file_data_saver: IDataSaver = FileDataSaver()
    file_data_saver.save_data(processed_data)
