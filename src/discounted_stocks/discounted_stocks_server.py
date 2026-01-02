import os
import json
from enum import Enum

import requests
import yfinance as yf

from abc import ABC, abstractmethod
from typing import List, Dict
from fastapi import FastAPI, Path, BackgroundTasks, HTTPException
from fastapi.params import Query

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


class INotification(ABC):
    @abstractmethod
    def send_telegram_notification(self, message: str) -> None:
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
                 evaluator: IDiscountEvaluator, notificator: INotification,
                 only_discount: bool = True):
        self.fetcher = fetcher
        self.calculator = calculator
        self.evaluator = evaluator
        self.notificator = notificator
        self.only_discount = only_discount

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

        batch_limit = 10
        for i in range(0, len(results), batch_limit):
            self.notificator.send_telegram_notification(json.dumps(results[i:i+batch_limit], indent=2))


class TelegramNotification(INotification):
    def __init__(self):
        self.telegram_token = os.environ["TELEGRAM_TOKEN"]
        self.chat_id = os.environ["TELEGRAM_CHAT_ID"]

    def send_telegram_notification(self, message: str) -> None:
        url = f"https://api.telegram.org/bot{self.telegram_token}/sendMessage"
        payload = {
            "chat_id": self.chat_id,
            "text": message
        }
        resp = requests.post(url, data=payload)
        print(resp.status_code)
        print(resp.text)


app = FastAPI()

@app.get("/health")
async def health():
    return {"status": "ok"}

@app.get("/discounted_stocks")
async def discounted_stocks(background_tasks: BackgroundTasks):
    try:
        all_stocks: List[Dict] = DataReaderFactory.get_stocks_data_reader(
            data_store="sql" if IS_SQL else "file"
        ).read_data()

        # Dependency injection setup
        fetcher: IStockDataFetcher = YFinanceStockFetcher()
        calculator: IDiscountCalculator = StandardDiscountCalculator()
        evaluator: IDiscountEvaluator = FundamentalMarketDiscountEvaluator()
        telegram_notification: INotification = TelegramNotification()
        analyzer = StockAnalyzer(fetcher, calculator, evaluator, telegram_notification)

        background_tasks.add_task(analyzer.analyze_stocks, all_stocks)
        return {"message": "Job has been started successfully"}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/all_stocks_status")
async def all_stocks_status(background_tasks: BackgroundTasks):
    try:
        all_stocks: List[Dict] = DataReaderFactory.get_stocks_data_reader(
            data_store="sql" if IS_SQL else "file"
        ).read_data()

        # Dependency injection setup
        fetcher: IStockDataFetcher = YFinanceStockFetcher()
        calculator: IDiscountCalculator = StandardDiscountCalculator()
        evaluator: IDiscountEvaluator = FundamentalMarketDiscountEvaluator()
        telegram_notification: INotification = TelegramNotification()
        analyzer = StockAnalyzer(fetcher, calculator, evaluator, telegram_notification, only_discount=False)

        background_tasks.add_task(analyzer.analyze_stocks, all_stocks)
        return {"message": "Job has been started successfully"}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/industry/{industry}")
async def industry(
        background_tasks: BackgroundTasks,
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
        telegram_notification: INotification = TelegramNotification()
        analyzer = StockAnalyzer(fetcher, calculator, evaluator, telegram_notification, only_discount=only_discount)

        background_tasks.add_task(analyzer.analyze_stocks, all_stocks)
        return {"message": "Job has been started successfully"}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
