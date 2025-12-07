"""
Pydantic models for data validation and serialization
"""

from datetime import datetime
from typing import List, Optional

from pydantic import BaseModel, Field, field_validator


class StockInfo(BaseModel):
    """股票基本資料"""

    id: str = Field(..., description="股票代碼")
    capital: Optional[float] = Field(None, description="股本")
    outstanding_shares: Optional[float] = Field(None, description="流通股數")
    per: Optional[float] = Field(None, description="本益比")
    cash_dividend: Optional[float] = Field(None, description="現金股利")
    stock_dividend: Optional[float] = Field(None, description="股票股利")
    updated_at: datetime = Field(default_factory=datetime.now, description="更新時間")

    class Config:
        from_attributes = True


class StockDaily(BaseModel):
    """股票日線資料"""

    id: Optional[int] = Field(None, description="ID")
    stock_id: str = Field(..., description="股票代碼")
    date: date = Field(..., description="日期")
    volume: Optional[float] = Field(None, description="成交量")
    open: Optional[float] = Field(None, description="開盤價")
    high: Optional[float] = Field(None, description="最高價")
    low: Optional[float] = Field(None, description="最低價")
    close: Optional[float] = Field(None, description="收盤價")
    created_at: datetime = Field(default_factory=datetime.now, description="建立時間")

    @field_validator("volume", "open", "high", "low", "close", mode="before")
    @classmethod
    def validate_floats(cls, v):
        if v is None or v == "":
            return None
        try:
            return float(v)
        except (ValueError, TypeError):
            return None

    class Config:
        from_attributes = True


class StockEPS(BaseModel):
    """股票 EPS 資料"""

    stock_id: str = Field(..., description="股票代碼")
    year: int = Field(..., description="年度")
    quarter: int = Field(..., description="季度")
    eps: Optional[float] = Field(None, description="EPS")

    class Config:
        from_attributes = True


class InstitutionalInvestors(BaseModel):
    """機構投資人資料"""

    id: Optional[int] = Field(None, description="ID")
    stock_id: str = Field(..., description="股票代碼")
    date: date = Field(..., description="日期")
    foreign: Optional[float] = Field(None, description="外資")
    investment_trust: Optional[float] = Field(None, description="投信")
    dealer: Optional[float] = Field(None, description="自營商")
    sum_holding_rate: Optional[float] = Field(None, description="總持股比率")
    foreign_holding_rate: Optional[float] = Field(None, description="外資持股比率")
    investment_trust_holding_rate: Optional[float] = Field(
        None, description="投信持股比率"
    )
    dealer_holding_rate: Optional[float] = Field(None, description="自營商持股比率")

    @field_validator(
        "foreign",
        "investment_trust",
        "dealer",
        "sum_holding_rate",
        "foreign_holding_rate",
        "investment_trust_holding_rate",
        "dealer_holding_rate",
        mode="before",
    )
    @classmethod
    def validate_floats(cls, v):
        if v is None or v == "":
            return None
        try:
            return float(v)
        except (ValueError, TypeError):
            return None

    class Config:
        from_attributes = True


class MajorInvestors(BaseModel):
    """主力投資人資料"""

    id: Optional[int] = Field(None, description="ID")
    stock_id: str = Field(..., description="股票代碼")
    date: date = Field(..., description="日期")
    major_investors: Optional[float] = Field(None, description="主力買賣超")
    agent_diff: Optional[float] = Field(None, description="券商分點差額")
    skp5: Optional[float] = Field(None, description="5日主力")
    skp20: Optional[float] = Field(None, description="20日主力")

    @field_validator("major_investors", "agent_diff", "skp5", "skp20", mode="before")
    @classmethod
    def validate_floats(cls, v):
        if v is None or v == "":
            return None
        try:
            return float(v)
        except (ValueError, TypeError):
            return None

    class Config:
        from_attributes = True


class MarginTrading(BaseModel):
    """融資融券資料"""

    id: Optional[int] = Field(None, description="ID")
    stock_id: str = Field(..., description="股票代碼")
    date: date = Field(..., description="日期")
    lending_balance: Optional[float] = Field(None, description="融資餘額")
    borrowing_balance: Optional[float] = Field(None, description="融券餘額")
    balance_limit: Optional[float] = Field(None, description="融資融券額度")

    @field_validator(
        "lending_balance", "borrowing_balance", "balance_limit", mode="before"
    )
    @classmethod
    def validate_floats(cls, v):
        if v is None or v == "":
            return None
        try:
            return float(v)
        except (ValueError, TypeError):
            return None

    class Config:
        from_attributes = True


class StockList(BaseModel):
    """股票清單"""

    id: str = Field(..., description="股票代碼")
    name: str = Field(..., description="股票名稱")
    type: Optional[str] = Field(None, description="類型")
    country: str = Field(default="TW", description="國家")
    market: str = Field(default="TWSE", description="市場")
    is_active: bool = Field(default=True, description="是否啟用")
    updated_at: datetime = Field(default_factory=datetime.now, description="更新時間")

    class Config:
        from_attributes = True


class BatchStockData(BaseModel):
    """批次股票資料"""

    stock_info: Optional[StockInfo] = None
    daily_data: List[StockDaily] = Field(default_factory=list)
    eps_data: List[StockEPS] = Field(default_factory=list)
    institutional_data: List[InstitutionalInvestors] = Field(default_factory=list)
    major_investor_data: List[MajorInvestors] = Field(default_factory=list)
    margin_trading_data: List[MarginTrading] = Field(default_factory=list)

    def has_data(self) -> bool:
        """檢查是否有任何資料"""
        return any(
            [
                self.stock_info is not None,
                len(self.daily_data) > 0,
                len(self.eps_data) > 0,
                len(self.institutional_data) > 0,
                len(self.major_investor_data) > 0,
                len(self.margin_trading_data) > 0,
            ]
        )

    def get_total_records(self) -> int:
        """取得總記錄數"""
        return (
            (1 if self.stock_info else 0)
            + len(self.daily_data)
            + len(self.eps_data)
            + len(self.institutional_data)
            + len(self.major_investor_data)
            + len(self.margin_trading_data)
        )
