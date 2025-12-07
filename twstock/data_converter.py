"""
資料轉換器 - 將原始 API 資料轉換為 Pydantic models
替代 pandas DataFrame 操作以提升效能
"""

from datetime import date, datetime
from typing import Any, Dict, List, Optional

from .models import (
    BatchStockData,
    InstitutionalInvestors,
    MajorInvestors,
    MarginTrading,
    StockDaily,
    StockEPS,
    StockInfo,
    StockList,
)


class DataConverter:
    """資料轉換器"""

    @staticmethod
    def safe_float(value: Any) -> Optional[float]:
        """安全轉換為浮點數"""
        if value is None or value == "" or value == "N/A":
            return None
        try:
            return float(value)
        except (ValueError, TypeError):
            return None

    @staticmethod
    def safe_int(value: Any) -> Optional[int]:
        """安全轉換為整數"""
        if value is None or value == "" or value == "N/A":
            return None
        try:
            return int(value)
        except (ValueError, TypeError):
            return None

    @staticmethod
    def safe_date(value: Any) -> Optional[date]:
        """安全轉換為日期"""
        if value is None or value == "":
            return None
        try:
            if isinstance(value, str):
                # 嘗試不同的日期格式
                for fmt in ["%Y-%m-%d", "%Y/%m/%d", "%Y%m%d"]:
                    try:
                        return datetime.strptime(value, fmt).date()
                    except ValueError:
                        continue
            elif isinstance(value, datetime):
                return value.date()
            elif isinstance(value, date):
                return value
        except (ValueError, TypeError):
            pass
        return None

    def convert_stock_info(
        self, raw_data: Dict[str, Any], stock_id: str
    ) -> Optional[StockInfo]:
        """轉換股票基本資料"""
        try:
            return StockInfo(
                id=stock_id,
                capital=self.safe_float(raw_data.get("capital")),
                outstanding_shares=self.safe_float(raw_data.get("outstanding_shares")),
                per=self.safe_float(raw_data.get("PER")),
                cash_dividend=self.safe_float(raw_data.get("cash_dividend")),
                stock_dividend=self.safe_float(raw_data.get("stock_dividend")),
                updated_at=datetime.now(),
            )
        except Exception as e:
            print(f"Error converting stock info for {stock_id}: {e}")
            return None

    def convert_daily_data(
        self, raw_data: List[Dict[str, Any]], stock_id: str
    ) -> List[StockDaily]:
        """轉換每日交易資料"""
        daily_data = []

        for row in raw_data:
            try:
                trade_date = self.safe_date(row.get("date"))
                if trade_date is None:
                    continue

                daily_record = StockDaily(
                    stock_id=stock_id,
                    date=trade_date,
                    volume=self.safe_float(row.get("volume")),
                    open=self.safe_float(row.get("open")),
                    high=self.safe_float(row.get("high")),
                    low=self.safe_float(row.get("low")),
                    close=self.safe_float(row.get("close")),
                )
                daily_data.append(daily_record)

            except Exception as e:
                print(f"Error converting daily data for {stock_id}, row {row}: {e}")
                continue

        return daily_data

    def convert_eps_data(
        self, raw_data: Dict[str, Any], stock_id: str
    ) -> List[StockEPS]:
        """轉換 EPS 資料"""
        eps_data = []

        for key, value in raw_data.items():
            if "Q" in key and "EPS" in key:
                try:
                    # 解析季度資訊，例如 "2023Q1_EPS"
                    parts = key.split("_")[0]  # 取 "2023Q1"
                    year_quarter = parts.split("Q")
                    if len(year_quarter) == 2:
                        year = self.safe_int(year_quarter[0])
                        quarter = self.safe_int(year_quarter[1])
                        eps_value = self.safe_float(value)

                        if year and quarter and eps_value is not None:
                            eps_record = StockEPS(
                                stock_id=stock_id,
                                year=year,
                                quarter=quarter,
                                eps=eps_value,
                            )
                            eps_data.append(eps_record)

                except Exception as e:
                    print(f"Error converting EPS data for {stock_id}, key {key}: {e}")
                    continue

        return eps_data

    def convert_institutional_data(
        self, raw_data: List[Dict[str, Any]], stock_id: str
    ) -> List[InstitutionalInvestors]:
        """轉換機構投資人資料"""
        institutional_data = []

        for row in raw_data:
            try:
                trade_date = self.safe_date(row.get("date"))
                if trade_date is None:
                    continue

                institutional_record = InstitutionalInvestors(
                    stock_id=stock_id,
                    date=trade_date,
                    foreign=self.safe_float(row.get("foreign")),
                    investment_trust=self.safe_float(row.get("investment_trust")),
                    dealer=self.safe_float(row.get("dealer")),
                    sum_holding_rate=self.safe_float(row.get("sum_holding_rate")),
                    foreign_holding_rate=self.safe_float(
                        row.get("foreign_holding_rate")
                    ),
                    investment_trust_holding_rate=self.safe_float(
                        row.get("investment_trust_holding_rate")
                    ),
                    dealer_holding_rate=self.safe_float(row.get("dealer_holding_rate")),
                )
                institutional_data.append(institutional_record)

            except Exception as e:
                print(
                    f"Error converting institutional data for {stock_id}, row {row}: {e}"
                )
                continue

        return institutional_data

    def convert_major_investor_data(
        self, raw_data: List[Dict[str, Any]], stock_id: str
    ) -> List[MajorInvestors]:
        """轉換主力投資人資料"""
        major_investor_data = []

        for row in raw_data:
            try:
                trade_date = self.safe_date(row.get("date"))
                if trade_date is None:
                    continue

                major_investor_record = MajorInvestors(
                    stock_id=stock_id,
                    date=trade_date,
                    major_investors=self.safe_float(row.get("major_investors")),
                    agent_diff=self.safe_float(row.get("agent_diff")),
                    skp5=self.safe_float(row.get("skp5")),
                    skp20=self.safe_float(row.get("skp20")),
                )
                major_investor_data.append(major_investor_record)

            except Exception as e:
                print(
                    f"Error converting major investor data for {stock_id}, row {row}: {e}"
                )
                continue

        return major_investor_data

    def convert_margin_trading_data(
        self, raw_data: List[Dict[str, Any]], stock_id: str
    ) -> List[MarginTrading]:
        """轉換融資融券資料"""
        margin_trading_data = []

        for row in raw_data:
            try:
                trade_date = self.safe_date(row.get("date"))
                if trade_date is None:
                    continue

                margin_trading_record = MarginTrading(
                    stock_id=stock_id,
                    date=trade_date,
                    lending_balance=self.safe_float(row.get("lending_balance")),
                    borrowing_balance=self.safe_float(row.get("borrowing_balance")),
                    balance_limit=self.safe_float(row.get("balance_limit")),
                )
                margin_trading_data.append(margin_trading_record)

            except Exception as e:
                print(
                    f"Error converting margin trading data for {stock_id}, row {row}: {e}"
                )
                continue

        return margin_trading_data

    def convert_stock_list(self, raw_data: List[Dict[str, Any]]) -> List[StockList]:
        """轉換股票清單"""
        stock_list = []

        for row in raw_data:
            try:
                stock_record = StockList(
                    id=row.get("id", ""),
                    name=row.get("name", ""),
                    type=row.get("type"),
                    country=row.get("country", "TW"),
                    market=row.get("market", "TWSE"),
                    is_active=row.get("is_active", True),
                )
                stock_list.append(stock_record)

            except Exception as e:
                print(f"Error converting stock list row {row}: {e}")
                continue

        return stock_list

    def convert_wantgoo_response(
        self, wantgoo_data: Dict[str, Any], stock_id: str
    ) -> BatchStockData:
        """轉換 Wantgoo API 回應為批次股票資料"""
        batch_data = BatchStockData()

        try:
            # 轉換股票基本資料
            if "info" in wantgoo_data:
                batch_data.stock_info = self.convert_stock_info(
                    wantgoo_data["info"], stock_id
                )

            # 轉換每日交易資料
            if "daily" in wantgoo_data and isinstance(wantgoo_data["daily"], list):
                batch_data.daily_data = self.convert_daily_data(
                    wantgoo_data["daily"], stock_id
                )

            # 轉換 EPS 資料
            if "info" in wantgoo_data:
                batch_data.eps_data = self.convert_eps_data(
                    wantgoo_data["info"], stock_id
                )

            # 轉換機構投資人資料
            if "institutional_investors" in wantgoo_data:
                batch_data.institutional_data = self.convert_institutional_data(
                    wantgoo_data["institutional_investors"], stock_id
                )

            # 轉換主力投資人資料
            if "major_investors" in wantgoo_data:
                batch_data.major_investor_data = self.convert_major_investor_data(
                    wantgoo_data["major_investors"], stock_id
                )

            # 轉換融資融券資料
            if "margin_trading" in wantgoo_data:
                batch_data.margin_trading_data = self.convert_margin_trading_data(
                    wantgoo_data["margin_trading"], stock_id
                )

        except Exception as e:
            print(f"Error converting Wantgoo response for {stock_id}: {e}")

        return batch_data

    def extract_from_pandas_dataframe(
        self, df, stock_id: str, data_type: str
    ) -> List[Any]:
        """從 pandas DataFrame 提取資料 (向後兼容)"""
        if df is None or df.empty:
            return []

        try:
            # 將 DataFrame 轉換為字典列表
            records = df.to_dict("records")

            if data_type == "daily":
                return self.convert_daily_data(records, stock_id)
            elif data_type == "institutional":
                return self.convert_institutional_data(records, stock_id)
            elif data_type == "major_investor":
                return self.convert_major_investor_data(records, stock_id)
            elif data_type == "margin_trading":
                return self.convert_margin_trading_data(records, stock_id)
            else:
                return []

        except Exception as e:
            print(
                f"Error extracting from DataFrame for {stock_id}, type {data_type}: {e}"
            )
            return []
