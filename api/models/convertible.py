"""可轉債相關 Pydantic Models"""

from datetime import datetime
from typing import List, Optional

from pydantic import BaseModel, Field


class CBSignal(BaseModel):
    """可轉債訊號"""

    name: str
    triggered: bool = True
    score: int
    description: Optional[str] = None


class ConvertibleBondListItem(BaseModel):
    """可轉債列表項目"""

    bond_id: str = Field(..., description="可轉債代碼")
    name: str = Field(..., description="可轉債名稱")
    underlying_stock_id: str = Field(..., description="標的股代碼")
    underlying_stock_name: str = Field("", description="標的股名稱")
    close: Optional[float] = Field(None, description="CB 收盤價")
    conversion_price: Optional[float] = Field(None, description="轉換價")
    conversion_value: Optional[float] = Field(None, description="轉換價值")
    premium_rate: Optional[float] = Field(None, description="溢價率 (%)")
    arbitrage_spread: Optional[float] = Field(None, description="套利空間 (%)")
    normalized_score: int = Field(0, description="套利評分 (0-100)")
    signal_count: int = Field(0, description="觸發訊號數")
    risk_level: str = Field("觀望", description="風險等級")
    maturity_date: Optional[datetime] = Field(None, description="到期日")
    volume: Optional[float] = Field(None, description="成交量")
    last_updated: Optional[datetime] = Field(None, description="最後更新")


class ConvertibleBondDetail(BaseModel):
    """可轉債詳情"""

    # 基本資訊
    bond_id: str
    name: str
    underlying_stock_id: str
    underlying_stock_name: str = ""
    issue_date: Optional[datetime] = None
    maturity_date: Optional[datetime] = None
    put_date: Optional[datetime] = None
    put_price: Optional[float] = None
    coupon_rate: Optional[float] = None
    issued_amount: Optional[float] = None
    outstanding_amount: Optional[float] = None

    # 交易資訊
    close: Optional[float] = None
    volume: Optional[float] = None
    conversion_price: Optional[float] = None
    conversion_value: Optional[float] = None
    premium_rate: Optional[float] = None
    arbitrage_spread: Optional[float] = None

    # 標的股
    underlying_close: Optional[float] = None
    underlying_overall_strength: int = 0

    # 訊號
    normalized_score: int = 0
    raw_score: int = 0
    signal_count: int = 0
    risk_level: str = "觀望"
    signals: List[CBSignal] = Field(default_factory=list)


class ConvertibleBondHistoryPoint(BaseModel):
    """可轉債歷史資料點"""

    date: datetime
    close: Optional[float] = None
    volume: Optional[float] = None
    conversion_value: Optional[float] = None
    premium_rate: Optional[float] = None
    underlying_close: Optional[float] = None


class PaginationMetadata(BaseModel):
    total: int
    page: int
    page_size: int
    total_pages: int
    has_next: bool
    has_prev: bool


class ConvertibleBondListResponse(BaseModel):
    """可轉債列表回應"""

    data: List[ConvertibleBondListItem]
    pagination: PaginationMetadata
