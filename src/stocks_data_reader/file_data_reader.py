import csv

from typing import List, Dict
from pathlib import Path

from yfinance.domain import industry

from src.stocks_data_reader.abstractions import StocksDataReader
from src.consts import ALL_STOCKS_LIST_CSV


class FileDataReader(StocksDataReader):
    def __init__(self):
        self.file_path = Path(ALL_STOCKS_LIST_CSV)
    def read_data(self) -> List[Dict[str, str]]:
        all_stocks_list: List[Dict[str, str]] = []
        with open(self.file_path) as csv_file:
            csv_reader = csv.reader(csv_file, delimiter=',')
            headers = next(csv_reader)
            for row in csv_reader:
                all_stocks_list.append(
                    {
                        headers[0]: row[0],
                        headers[1]: row[1],
                        headers[2]: row[2],
                        headers[3]: row[3],
                    }
                )
        return all_stocks_list

    def read_data_by_industry(self, industry: str) -> List[Dict[str, str]]:
        all_stocks_list: List[Dict[str, str]] = []
        with open(self.file_path) as csv_file:
            csv_reader = csv.reader(csv_file, delimiter=',')
            headers = next(csv_reader)
            for row in csv_reader:
                if row[2] == industry:
                    all_stocks_list.append(
                        {
                            headers[0]: row[0],
                            headers[1]: row[1],
                            headers[2]: row[2],
                            headers[3]: row[3],
                        }
                    )
        return all_stocks_list


if __name__ == "__main__":
    reader = FileDataReader()
    all_stocks = reader.read_data()
    # print(all_stocks)
    # print(len(all_stocks))
    all_stocks_list = reader.read_data_by_industry("Financial Services")
    print(all_stocks_list)


