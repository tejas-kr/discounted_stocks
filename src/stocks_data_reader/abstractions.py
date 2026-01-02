from abc import ABC, abstractmethod
from typing import List, Dict


class StocksDataReader(ABC):
    @abstractmethod
    def read_data(self) -> List[Dict[str, str]]:
        ...

    @abstractmethod
    def read_data_by_industry(self, industry: str) -> List[Dict[str, str]]:
        ...
