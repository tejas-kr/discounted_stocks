import io
import os
import csv
import json

import requests
import yfinance as yf

from pytz import utc
from datetime import datetime
from typing import List, Dict
from fastapi.params import Query
from abc import ABC, abstractmethod
from fastapi import FastAPI, Path, BackgroundTasks, HTTPException

from src.stocks_data_reader.factory import DataReaderFactory
from src.consts import IS_SQL


class IStockDataFetcher(ABC):
    @abstractmethod
    def fetch_stock_info(self, symbol: str) -> Dict:
        ...


class IDiscountCalculator(ABC):
    @abstractmethod
    def calculate_discount(self, info: Dict) -> float:
        ...


class IDiscountEvaluator(ABC):
    @abstractmethod
    def evaluate_status(self, info: Dict, discount_pct: float) -> str:
        ...


class IMessage(ABC):
    @abstractmethod
    def send_message(self, message: str) -> None:
        ...

    @abstractmethod
    def send_file(self, contents: bytes, filename: str) -> None:
        ...


class YFinanceStockFetcher(IStockDataFetcher):
    def fetch_stock_info(self, symbol: str) -> Dict:
        try:
            stock = yf.Ticker(symbol + ".NS")
            return stock.info
        except Exception as e:
            print(f"Error fetching data for {symbol}: {e}")
            return {}


class StandardDiscountCalculator(IDiscountCalculator):
    def calculate_discount(self, info: Dict) -> float:
        current_price = info.get('currentPrice') or info.get('regularMarketPrice')
        high_52 = info.get('fiftyTwoWeekHigh')
        if current_price and high_52:
            return ((high_52 - current_price) / high_52) * 100
        return 0.0


class FundamentalMarketDiscountEvaluator(IDiscountEvaluator):
    def evaluate_status(self, info: Dict, discount_pct: float) -> str:
        pe_ratio = info.get('trailingPE')
        pb_ratio = info.get('priceToBook')
        is_fundamental_discount = (pe_ratio and pe_ratio < 20) and (pb_ratio and pb_ratio < 2)
        is_market_discount = discount_pct > 20
        return "DISCOUNTED" if (is_fundamental_discount or is_market_discount) else "FAIR/HIGH"


class StockAnalyzer:
    def __init__(self, fetcher: IStockDataFetcher, calculator: IDiscountCalculator,
                 evaluator: IDiscountEvaluator, messanger: IMessage,
                 only_discount: bool = True):
        self.fetcher = fetcher
        self.calculator = calculator
        self.evaluator = evaluator
        self.messanger = messanger
        self.only_discount = only_discount

        self.send_as_text_message = False
        self.send_as_file = True

    def analyze_stocks(self, stocks: List[Dict]) -> None:
        results = []
        for stock in stocks:
            symbol = stock['symbol']
            company_name = stock['company_name']
            info = self.fetcher.fetch_stock_info(symbol)
            print(f"Symbol: {symbol}    info: {info}")
            if not info:
                continue
            discount_pct = self.calculator.calculate_discount(info)
            status = self.evaluator.evaluate_status(info, discount_pct)

            current_price = info.get('currentPrice') or info.get('regularMarketPrice')
            pe_ratio = info.get('trailingPE')
            pb_ratio = info.get('priceToBook')

            results.append({
                "Symbol": symbol,
                "CompanyName": company_name,
                "Price": current_price,
                "PE": round(pe_ratio, 2) if pe_ratio else "N/A",
                "PB": round(pb_ratio, 2) if pb_ratio else "N/A",
                "Discount % (52w High)": f"{discount_pct:.2f}%",
                "Status": status
            })

        if self.only_discount:
            results = [item for item in results if item['Status'] == 'DISCOUNTED']

        if self.send_as_text_message:
            batch_limit = 10
            for i in range(0, len(results), batch_limit):
                self.messanger.send_message(json.dumps(results[i:i+batch_limit], indent=2))

        if self.send_as_file:
            text_buffer = io.StringIO()

            writer = csv.DictWriter(
                text_buffer,
                fieldnames=results[0].keys(),
            )
            writer.writeheader()
            writer.writerows(results)

            bytes_buffer = io.BytesIO(text_buffer.getvalue().encode('utf-8'))
            bytes_buffer.seek(0)

            csv_bytes = bytes_buffer.getvalue()

            self.messanger.send_file(
                contents=csv_bytes,
                filename="GiftFromDiscountedStocks_" + datetime.now(tz=utc).strftime("%Y%m%d-%H%M%S") + ".csv"
            )


class TelegramMessanger(IMessage):
    def __init__(self, chat_id: str):
        self.telegram_token = os.environ["TELEGRAM_TOKEN"]
        self.chat_id = chat_id
        self.base_url = f"https://api.telegram.org/bot{self.telegram_token}"

    def send_message(self, message: str) -> None:
        url = f"{self.base_url}/sendMessage"
        payload = {
            "chat_id": self.chat_id,
            "text": message
        }
        resp = requests.post(url, data=payload)
        print(resp.status_code)
        print(resp.text)

    def send_file(self, contents: bytes, filename: str) -> None:
        url = f"{self.base_url}/sendDocument"
        files = {
            "document": (filename, contents, "text/csv")
        }
        data = {
            "chat_id": self.chat_id,
            "caption": "📊 Here is your CSV file as per your request"
        }
        try:
            response = requests.post(url, data=data, files=files)
            response.raise_for_status()  # Check for HTTP errors
            print("CSV sent successfully!")
        except Exception as exp:
            print("Failed to send file!", exp)


app = FastAPI()

@app.get("/health")
async def health():
    return {"status": "ok"}

@app.get("/discounted_stocks")
async def discounted_stocks(background_tasks: BackgroundTasks, telegram_chat_id: str):
    try:
        all_stocks: List[Dict] = DataReaderFactory.get_stocks_data_reader(
            data_store="sql" if IS_SQL else "file"
        ).read_data()

        # Dependency injection setup
        fetcher: IStockDataFetcher = YFinanceStockFetcher()
        calculator: IDiscountCalculator = StandardDiscountCalculator()
        evaluator: IDiscountEvaluator = FundamentalMarketDiscountEvaluator()
        telegram_messanger: IMessage = TelegramMessanger(telegram_chat_id)
        analyzer = StockAnalyzer(fetcher, calculator, evaluator, telegram_messanger)

        background_tasks.add_task(analyzer.analyze_stocks, all_stocks)
        return {"message": "Job has been started successfully"}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/all_stocks_status")
async def all_stocks_status(background_tasks: BackgroundTasks, telegram_chat_id: str):
    try:
        all_stocks: List[Dict] = DataReaderFactory.get_stocks_data_reader(
            data_store="sql" if IS_SQL else "file"
        ).read_data()

        # Dependency injection setup
        fetcher: IStockDataFetcher = YFinanceStockFetcher()
        calculator: IDiscountCalculator = StandardDiscountCalculator()
        evaluator: IDiscountEvaluator = FundamentalMarketDiscountEvaluator()
        telegram_messanger: IMessage = TelegramMessanger(telegram_chat_id)
        analyzer = StockAnalyzer(fetcher, calculator, evaluator, telegram_messanger, only_discount=False)

        background_tasks.add_task(analyzer.analyze_stocks, all_stocks)
        return {"message": "Job has been started successfully"}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/industry/{industry}")
async def industry(
        background_tasks: BackgroundTasks,
        telegram_chat_id: str,
        industry: str = Path(
            ...,
            description=("The list of available industries - "
                         "Information Technology, Metals & Mining, Healthcare, Forest Materials, Construction, "
                         "Services, Textiles, Power, Diversified, Utilities, Construction Materials, "
                         "Financial Services, Consumer Durables, Consumer Services, Fast Moving Consumer Goods, "
                         "Telecommunication, Chemicals, Oil Gas & Consumable Fuels, Realty, Capital Goods, "
                         "Media Entertainment & Publication, Automobile and Auto Components, ")
        ),
        only_discount: bool = Query(True, description="Whether or not to only use discount data")
):
    try:
        all_stocks: List[Dict] = DataReaderFactory.get_stocks_data_reader(
            data_store="sql" if IS_SQL else "file"
        ).read_data_by_industry(industry)

        # Dependency injection setup
        fetcher: IStockDataFetcher = YFinanceStockFetcher()
        calculator: IDiscountCalculator = StandardDiscountCalculator()
        evaluator: IDiscountEvaluator = FundamentalMarketDiscountEvaluator()
        telegram_messanger: IMessage = TelegramMessanger(telegram_chat_id)
        analyzer = StockAnalyzer(fetcher, calculator, evaluator, telegram_messanger, only_discount=only_discount)

        background_tasks.add_task(analyzer.analyze_stocks, all_stocks)
        return {"message": "Job has been started successfully"}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
