"""股票相關 Pydantic Models"""

from datetime import datetime
from typing import Dict, List, Optional

from pydantic import BaseModel, Field


class ChipSignal(BaseModel):
    """籌碼訊號"""

    name: str = Field(..., description="訊號名稱")
    triggered: bool = Field(..., description="是否觸發")
    score: int = Field(..., description="訊號分數")
    description: Optional[str] = None


class AlertStatus(BaseModel):
    """警示狀態"""

    is_attention_stock: bool = Field(default=False, description="是否為注意股")
    is_disposal_stock: bool = Field(default=False, description="是否為處置股")

    # 注意股詳情
    attention_count: Optional[int] = Field(None, description="累計注意次數")
    attention_reason: Optional[str] = Field(None, description="注意交易資訊")
    attention_date: Optional[str] = Field(None, description="最近注意日期")

    # 處置股詳情
    disposal_type: Optional[str] = Field(None, description="處置類型（第一次處置/第二次處置）")
    disposal_announced_date: Optional[str] = Field(None, description="處置公布日期")
    disposal_start_date: Optional[str] = Field(None, description="處置起始日期")
    disposal_end_date: Optional[str] = Field(None, description="處置迄日")
    disposal_condition: Optional[str] = Field(None, description="處置條件")
    disposal_measure: Optional[str] = Field(None, description="處置措施")
    disposal_content: Optional[str] = Field(None, description="處置內容")

    # 系統預警
    system_warning: bool = Field(default=False, description="系統預警標記")
    warning_score: int = Field(default=0, description="風險分數 0-100")
    warning_reasons: List[str] = Field(default_factory=list, description="預警原因列表")


class StockListItem(BaseModel):
    """股票列表項目"""

    stock_id: str = Field(..., description="股票代碼")
    name: str = Field(..., description="股票名稱")
    close_price: float = Field(..., description="收盤價")

    # 警示狀態（新增）
    alert_status: Optional[AlertStatus] = Field(None, description="警示狀態")

    # 三種強度（籌碼 + 技術 + 基本面）
    chip_strength: int = Field(..., ge=-25, le=89, description="籌碼強度（原始分數）")
    technical_strength: int = Field(..., ge=-170, le=194, description="技術強度（原始分數）")
    fundamental_strength: int = Field(..., ge=-73, le=93, description="基本面強度（原始分數）")
    overall_strength: int = Field(..., ge=0, le=100, description="綜合強度 (0-100)")

    # 權重資訊
    weights: Dict[str, float] = Field(
        default={"chip": 0.4, "technical": 0.3, "fundamental": 0.3},
        description="計算 overall_strength 使用的權重"
    )

    signal_count: int = Field(..., description="觸發的訊號總數")
    expected_return: float = Field(..., description="預期報酬率 (%)")
    win_rate: float = Field(..., description="歷史勝率 (%)")
    risk_level: str = Field(..., description="風險等級")

    # 訊號列表
    chip_signals: List[ChipSignal] = Field(default_factory=list, description="籌碼訊號")
    technical_signals: List[ChipSignal] = Field(default_factory=list, description="技術訊號")
    fundamental_signals: List[ChipSignal] = Field(
        default_factory=list, description="基本面訊號"
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
                "chip_strength": 32,
                "technical_strength": 25,
                "fundamental_strength": 45,
                "overall_strength": 78,
                "weights": {"chip": 0.5, "technical": 0.3, "fundamental": 0.2},
                "signal_strength": 78,
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
                "fundamental_signals": [
                    {
                        "name": "由虧轉正",
                        "triggered": True,
                        "score": 30,
                        "description": "最近一季EPS 1.5，前季虧損 -0.5"
                    },
                    {
                        "name": "本益比合理",
                        "triggered": True,
                        "score": 15,
                        "description": "PER=18.5 < 20"
                    }
                ],
                "major_signals": ["完美結構", "大戶連買3週", "多頭排列", "由虧轉正"],
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


class EPSDetail(BaseModel):
    """逐季 EPS 詳細資料"""

    year: int = Field(..., description="年份")
    quarter: int = Field(..., ge=1, le=4, description="季度 (1-4)")
    eps: float = Field(..., description="每股盈餘")

    class Config:
        from_attributes = True


class CapitalInfo(BaseModel):
    """股本與股數資訊"""

    capital: Optional[float] = Field(None, description="實收資本額（百萬）")
    outstanding_shares: Optional[float] = Field(None, description="股本（千股）")
    stock_dividend: Optional[float] = Field(None, description="股票股利")

    class Config:
        from_attributes = True


class HoldingInfo(BaseModel):
    """持股結構資訊"""

    director_ratio: Optional[float] = Field(None, description="董監持股比率 (%)")
    foreign_holding_rate: Optional[float] = Field(None, description="外資持股率 (%)")
    investment_trust_holding_rate: Optional[float] = Field(
        None, description="投信持股率 (%)"
    )
    dealer_holding_rate: Optional[float] = Field(None, description="自營商持股率 (%)")
    latest_date: Optional[datetime] = Field(None, description="最新資料日期")

    class Config:
        from_attributes = True


class MonthlyRevenueDetail(BaseModel):
    """月營收詳細資料"""

    year: int = Field(..., description="年份")
    month: int = Field(..., ge=1, le=12, description="月份 (1-12)")
    revenue: float = Field(..., description="月營收（千元）")
    mom_change: Optional[float] = Field(None, description="月增率 (%)")
    yoy_change: Optional[float] = Field(None, description="年增率 (%)")
    cumulative_revenue: Optional[float] = Field(None, description="累計營收（千元）")
    cumulative_yoy_change: Optional[float] = Field(None, description="累計年增率 (%)")

    class Config:
        from_attributes = True


class FundamentalInfo(BaseModel):
    """基本面資訊（詳細頁專用）"""

    # 獲利能力
    eps_recent_4q: List[float] = Field(..., description="最近4季EPS")
    eps_trend: str = Field(..., description="上升/下降/持平")
    eps_stability: float = Field(..., description="EPS標準差")
    eps_avg: float = Field(..., description="平均EPS")

    # 估值指標
    per: Optional[float] = Field(None, description="本益比")
    dividend_yield: float = Field(..., description="股利殖利率 (%)")
    payout_ratio: float = Field(..., description="配息率 (%)")
    cash_dividend: float = Field(..., description="現金股利")

    # 訊號
    fundamental_signals: List[ChipSignal] = Field(
        default_factory=list, description="基本面訊號"
    )
    fundamental_strength: int = Field(..., ge=-73, le=93, description="基本面強度評分（原始分數）")

    # 🆕 營收成長（新增）
    revenue_recent_12m: List[float] = Field(
        default_factory=list, description="最近12個月營收（千元）"
    )
    revenue_yoy_avg: float = Field(0.0, description="平均年增率 (%)")
    revenue_trend: str = Field("持平", description="營收趨勢（上升/下降/持平）")

    # 🆕 詳細數據（Tabs 架構用）
    eps_details: List[EPSDetail] = Field(
        default_factory=list, description="逐季 EPS 詳細資料"
    )
    capital_info: Optional[CapitalInfo] = Field(None, description="股本資訊")
    holding_info: Optional[HoldingInfo] = Field(None, description="持股結構")
    revenue_details: List[MonthlyRevenueDetail] = Field(
        default_factory=list, description="月營收詳細資料"
    )


class StockDetail(BaseModel):
    """股票詳細資訊（詳情頁專用，不包含三種強度）"""

    basic_info: BasicInfo
    price_info: PriceInfo

    # 警示狀態（新增）
    alert_status: Optional[AlertStatus] = Field(None, description="警示狀態")

    # 訊號列表（更新：新增基本面資訊）
    chip_signals: List[ChipSignal] = Field(default_factory=list, description="籌碼訊號")
    technical_signals: List[ChipSignal] = Field(
        default_factory=list, description="技術訊號"
    )
    fundamental_info: Optional[FundamentalInfo] = Field(None, description="基本面資訊")

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

    # 張數欄位（資料庫儲存）
    foreign_shares: Optional[float] = Field(None, description="外資買賣超 (張)")
    investment_trust_shares: Optional[float] = Field(None, description="投信買賣超 (張)")
    dealer_shares: Optional[float] = Field(None, description="自營商買賣超 (張)")

    # 百分比欄位（API 即時計算）
    foreign: Optional[float] = Field(None, description="外資買賣超 (%)")
    investment_trust: Optional[float] = Field(None, description="投信買賣超 (%)")
    dealer: Optional[float] = Field(None, description="自營商買賣超 (%)")

    # 持股比率（保持不變）
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
    lending_change: float = Field(0.0, description="融資餘額變化 (張)")
    borrowing_change: float = Field(0.0, description="融券餘額變化 (張)")


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
