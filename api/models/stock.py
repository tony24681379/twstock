"""股票相關 Pydantic Models"""

from datetime import datetime
from typing import List, Optional

from pydantic import BaseModel, Field


class ChipSignal(BaseModel):
    """籌碼訊號"""

    name: str = Field(..., description="訊號名稱")
    triggered: bool = Field(..., description="是否觸發")
    score: int = Field(..., description="訊號分數")
    description: Optional[str] = None


class StockListItem(BaseModel):
    """股票列表項目"""

    stock_id: str = Field(..., description="股票代碼")
    name: str = Field(..., description="股票名稱")
    close_price: float = Field(..., description="收盤價")

    # 三種強度（新增）
    chip_strength: int = Field(..., ge=0, le=100, description="籌碼強度 (0-100)")
    technical_strength: int = Field(..., ge=0, le=100, description="技術強度 (0-100)")
    overall_strength: int = Field(..., ge=0, le=100, description="綜合強度 (0-100)")

    signal_count: int = Field(..., description="觸發的訊號總數")
    expected_return: float = Field(..., description="預期報酬率 (%)")
    win_rate: float = Field(..., description="歷史勝率 (%)")
    risk_level: str = Field(..., description="風險等級")

    # 訊號列表（新增）
    chip_signals: List[ChipSignal] = Field(default_factory=list, description="籌碼訊號")
    technical_signals: List[ChipSignal] = Field(
        default_factory=list, description="技術訊號"
    )

    # 向後相容（棄用但保留）
    signal_strength: int = Field(..., description="已棄用，請使用 overall_strength")
    signals: List[ChipSignal] = Field(
        default_factory=list, description="完整訊號列表（含分數和詳情）"
    )
    major_signals: List[str] = Field(
        default_factory=list, description="主要訊號列表（向後相容）"
    )

    last_updated: datetime = Field(..., description="最後更新時間")

    class Config:
        json_schema_extra = {
            "example": {
                "stock_id": "2330",
                "name": "台積電",
                "close_price": 580.0,
                "chip_strength": 85,
                "technical_strength": 72,
                "overall_strength": 80,
                "signal_strength": 80,
                "signal_count": 5,
                "expected_return": 2.61,
                "win_rate": 60.2,
                "risk_level": "低",
                "chip_signals": [
                    {
                        "name": "完美結構",
                        "triggered": True,
                        "score": 20,
                        "description": "大戶↑ 超大戶↑ 散戶↓",
                    },
                    {"name": "大戶連買3週", "triggered": True, "score": 12},
                ],
                "technical_signals": [
                    {
                        "name": "多頭排列",
                        "triggered": True,
                        "score": 15,
                        "description": "MA5 > MA10 > MA20",
                    },
                    {"name": "黃金交叉", "triggered": True, "score": 10},
                ],
                "major_signals": ["完美結構", "大戶連買3週", "多頭排列"],
                "last_updated": "2026-01-04T10:00:00",
            }
        }


class PaginationMetadata(BaseModel):
    """分頁中繼資料"""

    total: int = Field(..., description="總筆數")
    page: int = Field(..., description="當前頁碼")
    page_size: int = Field(..., description="每頁筆數")
    total_pages: int = Field(..., description="總頁數")
    has_next: bool = Field(..., description="是否有下一頁")
    has_prev: bool = Field(..., description="是否有上一頁")


class StockListResponse(BaseModel):
    """股票列表回應"""

    data: List[StockListItem] = Field(..., description="股票列表")
    pagination: PaginationMetadata = Field(..., description="分頁資訊")


class BasicInfo(BaseModel):
    """股票基本資訊"""

    stock_id: str
    name: str
    industry: Optional[str] = None
    outstanding_shares: Optional[float] = None


class PriceInfo(BaseModel):
    """價格資訊"""

    close: float
    open: float
    high: float
    low: float
    volume: float
    date: datetime
    change: float = Field(..., description="漲跌")
    change_percent: float = Field(..., description="漲跌幅 %")


class ConcentrationSummary(BaseModel):
    """籌碼集中度摘要"""

    large_holders_pct: float = Field(..., description="大戶持股比例 (%)")
    super_large_holders_pct: float = Field(..., description="超大戶持股比例 (%)")
    retail_pct: float = Field(..., description="散戶持股比例 (%)")
    latest_date: datetime = Field(..., description="最新資料日期")


class StockDetail(BaseModel):
    """股票詳細資訊（詳情頁專用，不包含三種強度）"""

    basic_info: BasicInfo
    price_info: PriceInfo

    # 訊號列表
    chip_signals: List[ChipSignal] = Field(default_factory=list, description="籌碼訊號")
    technical_signals: List[ChipSignal] = Field(
        default_factory=list, description="技術訊號"
    )

    expected_return: float = Field(0.0, description="預期報酬率 (%)")
    win_rate: float = Field(0.0, description="歷史勝率 (%)")
    concentration_summary: ConcentrationSummary

    # 向後相容
    signal_strength: int = Field(..., description="已棄用，請使用 overall_strength")
    signals: List[ChipSignal] = Field(default_factory=list, description="完整訊號列表")


class ConcentrationDataPoint(BaseModel):
    """籌碼集中度資料點"""

    week_label: str = Field(..., description="週次標籤 (W1, W2, ...)")
    date: datetime
    moreThan400_pct: float = Field(..., description="大戶持股比例 (%)")
    moreThan1000_pct: float = Field(..., description="超大戶持股比例 (%)")
    lessThan20_pct: float = Field(..., description="散戶持股比例 (%)")
    moreThan400_change: float = Field(..., description="大戶變化 (%)")
    moreThan1000_change: float = Field(..., description="超大戶變化 (%)")
    lessThan20_change: float = Field(..., description="散戶變化 (%)")


class StockHistory(BaseModel):
    """股票歷史籌碼集中度"""

    stock_id: str
    data: List[ConcentrationDataPoint]


class InstitutionalData(BaseModel):
    """三大法人買賣超資料"""

    date: datetime = Field(..., description="日期")
    foreign: Optional[float] = Field(None, description="外資買賣超 (張)")
    investment_trust: Optional[float] = Field(None, description="投信買賣超 (張)")
    dealer: Optional[float] = Field(None, description="自營商買賣超 (張)")
    sum_holding_rate: Optional[float] = Field(
        None, description="三大法人持股比率合計 (%)"
    )
    foreign_holding_rate: Optional[float] = Field(None, description="外資持股比率 (%)")
    investment_trust_holding_rate: Optional[float] = Field(
        None, description="投信持股比率 (%)"
    )
    dealer_holding_rate: Optional[float] = Field(None, description="自營商持股比率 (%)")


class MajorInvestorData(BaseModel):
    """主力買賣超資料"""

    date: datetime = Field(..., description="日期")
    major_investors: Optional[float] = Field(None, description="主力買賣超 (張)")
    agent_diff: Optional[float] = Field(None, description="分點差額 (張)")
    skp20: Optional[float] = Field(None, description="20日主力 (張)")


class MarginTradingData(BaseModel):
    """融資融券資料"""

    date: datetime = Field(..., description="日期")
    lending_balance: Optional[float] = Field(None, description="融資餘額 (張)")
    borrowing_balance: Optional[float] = Field(None, description="融券餘額 (張)")
    balance_limit: Optional[float] = Field(None, description="融資融券限額")


class ChipsData(BaseModel):
    """完整籌碼資料"""

    stock_id: str = Field(..., description="股票代碼")
    institutional: List[InstitutionalData] = Field(
        default_factory=list, description="三大法人資料"
    )
    major: List[MajorInvestorData] = Field(default_factory=list, description="主力資料")
    margin: List[MarginTradingData] = Field(
        default_factory=list, description="融資融券資料"
    )
    concentration: Optional[ConcentrationSummary] = Field(
        None, description="籌碼集中度摘要"
    )
    concentration: Optional[ConcentrationSummary] = Field(
        None, description="籌碼集中度摘要"
    )
