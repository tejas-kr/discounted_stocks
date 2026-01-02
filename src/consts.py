from pathlib import Path


_src_path = Path(__file__).resolve().parent
ALL_STOCKS_LIST_CSV = _src_path / "all_stocks_list.csv"

IS_SQL = False
