import asyncio
import hashlib
import logging
import os
from contextlib import asynccontextmanager
from datetime import date, datetime
from typing import Any, Dict, List, Optional

logger = logging.getLogger(__name__)

import pandas as pd
from sqlalchemy import (
    BigInteger,
    Boolean,
    Column,
    DateTime,
    Float,
    Index,
    Integer,
    String,
    create_engine,
    text,
)
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.pool import NullPool

try:
    from .db_utils import prepare_dataframe_for_db, to_python_datetime
except ImportError:
    from db_utils import prepare_dataframe_for_db, to_python_datetime

Base = declarative_base()


class StockInfo(Base):
    """股票基本資訊表"""

    __tablename__ = "stock_info"

    stock_id = Column(String, primary_key=True, index=True, comment="股票代號")
    capital = Column(Float, comment="資本額")
    outstanding_shares = Column(Float, comment="流通股數")
    per = Column(Float, comment="本益比")
    cash_dividend = Column(Float, comment="現金股利")
    stock_dividend = Column(Float, comment="股票股利")
    updated_at = Column(DateTime, default=datetime.now, comment="更新時間")

    __table_args__ = (Index("idx_stock_info_updated", "updated_at"),)


class StockDaily(Base):
    """股票每日交易資料表"""

    __tablename__ = "stock_daily"

    id = Column(BigInteger, primary_key=True, index=True)
    stock_id = Column(String, nullable=False, index=True, comment="股票代號")
    date = Column(DateTime, nullable=False, comment="交易日期")
    volume = Column(Float, comment="成交量")
    open = Column(Float, comment="開盤價")
    high = Column(Float, comment="最高價")
    low = Column(Float, comment="最低價")
    close = Column(Float, comment="收盤價")
    created_at = Column(DateTime, default=datetime.now, comment="建立時間")

    __table_args__ = (
        Index("idx_stock_daily_stock_date", "stock_id", "date", unique=True),
        Index("idx_stock_daily_date", "date"),
    )


class InstitutionalInvestors(Base):
    """三大法人買賣超表"""

    __tablename__ = "institutional_investors"

    id = Column(BigInteger, primary_key=True, index=True)
    stock_id = Column(String, nullable=False, index=True, comment="股票代號")
    date = Column(DateTime, nullable=False, comment="交易日期")
    foreign_shares = Column(Float, comment="外資買賣超(張)")
    investment_trust_shares = Column(Float, comment="投信買賣超(張)")
    dealer_shares = Column(Float, comment="自營商買賣超(張)")
    sum_holding_rate = Column(Float, comment="三大法人持股比率合計(%)")
    foreign_holding_rate = Column(Float, comment="外資持股比率(%)")
    investment_trust_holding_rate = Column(Float, comment="投信持股比率(%)")
    dealer_holding_rate = Column(Float, comment="自營商持股比率(%)")

    __table_args__ = (
        Index("idx_institutional_stock_date", "stock_id", "date", unique=True),
    )


class MajorInvestors(Base):
    """主力買賣超表"""

    __tablename__ = "major_investors"

    id = Column(BigInteger, primary_key=True, index=True)
    stock_id = Column(String, nullable=False, index=True, comment="股票代號")
    date = Column(DateTime, nullable=False, comment="交易日期")
    major_investors = Column(Float, comment="主力買賣超")
    agent_diff = Column(Float, comment="分點差額")
    skp5 = Column(Float, comment="5日主力")
    skp20 = Column(Float, comment="20日主力")

    __table_args__ = (Index("idx_major_stock_date", "stock_id", "date", unique=True),)


class MarginTrading(Base):
    """融資融券表"""

    __tablename__ = "margin_trading"

    id = Column(BigInteger, primary_key=True, index=True)
    stock_id = Column(String, nullable=False, index=True, comment="股票代號")
    date = Column(DateTime, nullable=False, comment="交易日期")
    lending_balance = Column(Float, comment="融資餘額")
    borrowing_balance = Column(Float, comment="融券餘額")
    balance_limit = Column(Float, comment="融資融券限額")

    __table_args__ = (Index("idx_margin_stock_date", "stock_id", "date", unique=True),)


class ConcentrationData(Base):
    """籌碼集中度數據表"""

    __tablename__ = "concentration_data"

    id = Column(BigInteger, primary_key=True, index=True)
    stock_id = Column(String, nullable=False, index=True, comment="股票代號")
    date = Column(DateTime, nullable=False, comment="交易日期")
    more_than_400 = Column(Float, comment=">400張大股東占比")
    more_than_1000 = Column(Float, comment=">1000張大股東占比")
    less_than_20 = Column(Float, comment="<20張散戶占比")
    close = Column(Float, comment="收盤價")
    director_ratio = Column(Float, comment="董監持股比率")
    rate_of_foreign_holding = Column(Float, comment="外資持股比率")
    rate_of_ing_holding = Column(Float, comment="投信持股比率")
    rate_of_dealer_holding = Column(Float, comment="自營商持股比率")
    created_at = Column(DateTime, default=datetime.now, comment="建立時間")

    __table_args__ = (
        Index("idx_concentration_stock_date", "stock_id", "date", unique=True),
        Index("idx_concentration_date", "date"),
    )


class StockEPS(Base):
    """股票EPS資料表"""

    __tablename__ = "stock_eps"

    stock_id = Column(String, primary_key=True, index=True, comment="股票代號")
    year = Column(Integer, primary_key=True, comment="年度")
    quarter = Column(Integer, primary_key=True, comment="季度")
    eps = Column(Float, comment="每股盈餘")

    __table_args__ = (
        Index("idx_eps_stock", "stock_id"),
        Index("idx_eps_year_quarter", "year", "quarter"),
    )


class StockDividend(Base):
    """股票歷史股利資料表"""

    __tablename__ = "stock_dividend"

    stock_id = Column(String, primary_key=True, index=True, comment="股票代號")
    year = Column(Integer, primary_key=True, comment="年度（民國年）")
    cash_dividend = Column(Float, comment="現金股利")
    stock_dividend = Column(Float, comment="股票股利")
    updated_at = Column(DateTime, default=datetime.now, comment="更新時間")

    __table_args__ = (
        Index("idx_dividend_stock", "stock_id"),
        Index("idx_dividend_year", "year"),
    )


class StockMonthlyRevenue(Base):
    """股票月營收資料表"""

    __tablename__ = "stock_monthly_revenue"

    stock_id = Column(String, primary_key=True, index=True, comment="股票代號")
    year = Column(Integer, primary_key=True, comment="年份")
    month = Column(Integer, primary_key=True, comment="月份 (1-12)")
    revenue = Column(Float, comment="月營收（千元）")
    mom_change = Column(Float, comment="月增率 (MoM %)")
    yoy_change = Column(Float, comment="年增率 (YoY %)")
    cumulative_revenue = Column(Float, comment="累計營收（千元）")
    cumulative_yoy_change = Column(Float, comment="累計年增率 (%)")
    updated_at = Column(DateTime, default=datetime.now, comment="更新時間")

    __table_args__ = (
        Index("idx_revenue_stock", "stock_id"),
        Index("idx_revenue_year_month", "year", "month"),
    )


class StockList(Base):
    """股票清單"""

    __tablename__ = "stock_list"

    stock_id = Column(String, primary_key=True, index=True, comment="股票代號")
    name = Column(String, comment="股票名稱")
    type = Column(String, comment="類型(Stock/ETF/Index)")
    country = Column(String, default="TW", comment="國家")
    market = Column(String, comment="市場(Listed/OTC/Emerging)")
    is_active = Column(Boolean, default=True, comment="是否啟用")
    updated_at = Column(DateTime, default=datetime.now, comment="更新時間")

    __table_args__ = (Index("idx_stock_list_active_market", "is_active", "market"),)


class StockTechnicalIndicators(Base):
    """股票技術指標預計算表"""

    __tablename__ = "stock_technical_indicators"

    id = Column(BigInteger, primary_key=True, index=True)
    stock_id = Column(String, nullable=False, index=True, comment="股票代號")
    date = Column(DateTime, nullable=False, comment="交易日期")
    ma5 = Column(Float, comment="5日均線")
    ma10 = Column(Float, comment="10日均線")
    ma20 = Column(Float, comment="20日均線")
    ma60 = Column(Float, comment="60日均線")
    macd = Column(Float, comment="MACD")
    macd_signal = Column(Float, comment="MACD 信號線")
    macd_hist = Column(Float, comment="MACD 柱狀圖")
    k9 = Column(Float, comment="KD K值")
    d9 = Column(Float, comment="KD D值")
    rsi = Column(Float, comment="RSI")
    adx = Column(Float, comment="ADX")
    bollinger_upper = Column(Float, comment="布林通道上軌")
    bollinger_middle = Column(Float, comment="布林通道中軌")
    bollinger_lower = Column(Float, comment="布林通道下軌")
    updated_at = Column(DateTime, default=datetime.now, comment="更新時間")

    __table_args__ = (
        Index("idx_tech_indicators_stock_date", "stock_id", "date", unique=True),
        Index("idx_tech_indicators_updated", "updated_at"),
    )


class StockTechnicalSignals(Base):
    """股票技術訊號預計算表"""

    __tablename__ = "stock_technical_signals"

    stock_id = Column(String, primary_key=True, index=True, comment="股票代號")
    date = Column(DateTime, nullable=False, comment="最新訊號日期")
    raw_score = Column(Integer, nullable=False, comment="原始分數 (-120~+164)")
    normalized_score = Column(Integer, nullable=False, comment="標準化分數 (0-100)")
    signal_count = Column(Integer, nullable=False, comment="訊號數量")
    buy_signals = Column(Integer, default=0, comment="買入訊號數量")
    sell_signals = Column(Integer, default=0, comment="賣出訊號數量")
    signals_json = Column(String, comment="訊號詳情 JSON")
    signals_detail = Column(
        JSONB, comment="訊號詳細資訊（包含trigger_date、type、is_valid等）"
    )
    updated_at = Column(DateTime, default=datetime.now, comment="更新時間")

    __table_args__ = (
        Index("idx_tech_signals_score", "normalized_score"),
        Index("idx_tech_signals_updated", "updated_at"),
    )


class StockSignalHistory(Base):
    """股票技術訊號歷史記錄表

    追蹤每個技術訊號的觸發日期和有效期，用於實現訊號時效性管理：
    - 交叉/事件型訊號（crossover）：觸發後顯示5天，之後過期
    - 狀態型訊號（state）：持續驗證有效性，條件失效時立即移除
    """

    __tablename__ = "stock_signal_history"

    id = Column(Integer, primary_key=True, autoincrement=True, comment="主鍵ID")
    stock_id = Column(String, nullable=False, index=True, comment="股票代號")
    signal_name = Column(
        String, nullable=False, comment="訊號名稱（如：黃金交叉、多頭排列）"
    )
    signal_type = Column(
        String,
        nullable=False,
        comment="訊號類型：crossover（交叉/事件型，5天過期）或 state（狀態型，持續驗證）",
    )
    trigger_date = Column(DateTime, nullable=False, comment="訊號首次觸發日期")
    last_valid_date = Column(
        DateTime,
        nullable=True,
        comment="最後驗證有效的日期（NULL表示已失效）",
    )
    score = Column(Integer, nullable=False, comment="訊號分數（正數=買入，負數=賣出）")
    signal_metadata = Column(
        JSONB, comment="額外元數據（JSON格式，可儲存觸發時的技術指標值）"
    )
    created_at = Column(DateTime, default=datetime.now, comment="記錄建立時間")
    updated_at = Column(
        DateTime, default=datetime.now, onupdate=datetime.now, comment="記錄更新時間"
    )

    __table_args__ = (
        # 複合唯一索引：同一股票的同一訊號在同一天只能有一筆記錄
        Index(
            "idx_unique_stock_signal",
            "stock_id",
            "signal_name",
            "trigger_date",
            unique=True,
        ),
        # 查詢優化索引
        Index("idx_signal_history_stock_date", "stock_id", "trigger_date"),
        Index("idx_signal_history_type", "signal_type"),
        Index("idx_signal_history_valid", "last_valid_date"),
        Index("idx_signal_history_stock_name", "stock_id", "signal_name"),
    )


class StockAlertStatus(Base):
    """股票警示狀態表"""

    __tablename__ = "stock_alert_status"

    # 主鍵
    stock_id = Column(String, primary_key=True, index=True, comment="股票代號")

    # 注意股資訊
    is_attention_stock = Column(Boolean, default=False, comment="是否為注意股")
    attention_count = Column(Integer, comment="累計注意次數")
    attention_reason = Column(String, comment="注意交易資訊")
    attention_date = Column(DateTime, comment="最近注意日期")

    # 處置股資訊
    is_disposal_stock = Column(Boolean, default=False, comment="是否為處置股")
    disposal_announced_date = Column(DateTime, comment="處置公布日期")
    disposal_start_date = Column(DateTime, comment="處置起始日期")
    disposal_end_date = Column(DateTime, comment="處置迄日")
    disposal_type = Column(String, comment="處置類型（第一次處置/第二次處置）")
    disposal_condition = Column(String, comment="處置條件（連續三次等）")
    disposal_measure = Column(String, comment="處置措施")
    disposal_content = Column(String, comment="處置內容（完整說明）")

    # 系統預警（輔助）
    system_warning = Column(Boolean, default=False, comment="系統預警標記")
    warning_reasons = Column(JSONB, comment="預警原因列表")
    warning_score = Column(Integer, default=0, comment="風險分數 0-100")

    # 元數據
    last_checked_at = Column(DateTime, comment="最後檢查時間")
    updated_at = Column(
        DateTime, default=datetime.now, onupdate=datetime.now, comment="更新時間"
    )
    created_at = Column(DateTime, default=datetime.now, comment="建立時間")

    __table_args__ = (
        Index("idx_alert_attention", "is_attention_stock"),
        Index("idx_alert_disposal", "is_disposal_stock"),
        Index("idx_alert_updated", "updated_at"),
    )


class StockUpdateTracker(Base):
    """股票更新追蹤表"""

    __tablename__ = "stock_update_tracker"

    stock_id = Column(String, primary_key=True, index=True, comment="股票代號")
    info_updated_at = Column(DateTime, comment="基本資訊更新時間")
    daily_updated_at = Column(DateTime, comment="每日資料更新時間")
    analysis_updated_at = Column(DateTime, comment="分析資料更新時間")
    info_loaded = Column(Boolean, default=False, comment="基本資訊已載入")
    daily_loaded = Column(Boolean, default=False, comment="每日資料已載入")
    daily_first_date = Column(DateTime, comment="每日資料最早日期")
    daily_last_date = Column(DateTime, comment="每日資料最新日期")
    last_checked_at = Column(DateTime, comment="最後檢查時間")
    created_at = Column(DateTime, default=datetime.now, comment="建立時間")
    updated_at = Column(
        DateTime, default=datetime.now, onupdate=datetime.now, comment="更新時間"
    )


class ConvertibleBond(Base):
    """可轉換公司債基本資訊表"""

    __tablename__ = "convertible_bond"

    bond_id = Column(String, primary_key=True, index=True, comment="可轉債代碼")
    name = Column(String, comment="可轉債名稱")
    underlying_stock_id = Column(String, index=True, comment="標的股票代碼")
    conversion_price = Column(Float, comment="轉換價格")
    issue_date = Column(DateTime, comment="發行日期")
    maturity_date = Column(DateTime, comment="到期日期")
    put_date = Column(DateTime, nullable=True, comment="下次賣回日")
    put_price = Column(Float, nullable=True, comment="賣回價格")
    coupon_rate = Column(Float, comment="票面利率")
    issued_amount = Column(Float, comment="發行總額（張）")
    outstanding_amount = Column(Float, comment="流通在外餘額（張）")
    is_active = Column(Boolean, default=True, comment="是否仍在交易")
    updated_at = Column(DateTime, default=datetime.now, comment="更新時間")

    __table_args__ = (
        Index("idx_cb_underlying", "underlying_stock_id"),
        Index("idx_cb_active", "is_active"),
    )


class ConvertibleBondDaily(Base):
    """可轉債每日交易資料表"""

    __tablename__ = "convertible_bond_daily"

    id = Column(BigInteger, primary_key=True, index=True)
    bond_id = Column(String, nullable=False, index=True, comment="可轉債代碼")
    date = Column(DateTime, nullable=False, comment="交易日期")
    open = Column(Float, comment="開盤價")
    high = Column(Float, comment="最高價")
    low = Column(Float, comment="最低價")
    close = Column(Float, comment="收盤價（面額基準）")
    volume = Column(Float, comment="成交量")
    underlying_close = Column(Float, comment="標的股收盤價")
    conversion_value = Column(Float, comment="轉換價值")
    premium_rate = Column(Float, comment="溢價率 (%)")
    arbitrage_spread = Column(Float, comment="套利空間 (%)")

    __table_args__ = (
        Index("idx_cb_daily_bond_date", "bond_id", "date", unique=True),
        Index("idx_cb_daily_date", "date"),
    )


class ConvertibleBondSignals(Base):
    """可轉債套利訊號表"""

    __tablename__ = "convertible_bond_signals"

    bond_id = Column(String, primary_key=True, index=True, comment="可轉債代碼")
    date = Column(DateTime, comment="計算日期")
    raw_score = Column(Integer, comment="原始分數")
    normalized_score = Column(Integer, comment="標準化分數 (0-100)")
    signal_count = Column(Integer, comment="觸發訊號數")
    risk_level = Column(String, comment="風險等級")
    signals_json = Column(JSONB, comment="訊號詳情")
    underlying_stock_id = Column(String, index=True, comment="標的股代碼")
    premium_rate = Column(Float, comment="最新溢價率 (%)")
    conversion_value = Column(Float, comment="最新轉換價值")
    updated_at = Column(DateTime, default=datetime.now, comment="更新時間")

    __table_args__ = (
        Index("idx_cb_signals_score", "normalized_score"),
        Index("idx_cb_signals_underlying", "underlying_stock_id"),
    )


class StockListCache(Base):
    """股票列表預計算快取表

    預先計算三維分數（籌碼/技術/基本面），API 查詢時僅需 SQL SELECT + 動態加權排序分頁。
    消除 /api/stocks endpoint 的 OOM 問題。
    """

    __tablename__ = "stock_list_cache"

    stock_id = Column(String, primary_key=True, index=True, comment="股票代號")
    name = Column(String, comment="股票名稱")
    close_price = Column(Float, comment="收盤價")
    last_date = Column(DateTime, comment="最新資料日期")

    # 籌碼維度
    chip_raw_score = Column(Integer, default=0, comment="籌碼原始分數 (-25~+89)")
    chip_normalized = Column(Integer, default=0, comment="籌碼標準化分數 (0-100)")
    chip_signals_json = Column(String, comment="籌碼訊號 JSON")
    expected_return = Column(Float, default=0.0, comment="預期報酬率 (%)")
    win_rate = Column(Float, default=0.0, comment="歷史勝率 (%)")

    # 技術維度
    tech_raw_score = Column(Integer, default=0, comment="技術原始分數 (-120~+164)")
    tech_normalized = Column(Integer, default=0, comment="技術標準化分數 (0-100)")
    tech_signals_json = Column(String, comment="技術訊號 JSON")

    # 基本面維度
    fund_raw_score = Column(Integer, default=0, comment="基本面原始分數 (-110~+139)")
    fund_normalized = Column(Integer, default=0, comment="基本面標準化分數 (0-100)")
    fund_signals_json = Column(String, comment="基本面訊號 JSON")

    # 聚合
    signal_count = Column(Integer, default=0, comment="訊號總數")
    risk_level = Column(String, default="中", comment="風險等級")

    # 警示 & 可轉債
    alert_status_json = Column(String, comment="警示狀態 JSON")
    cb_arbitrage_score = Column(Integer, comment="可轉債套利評分")
    cb_signals_json = Column(String, comment="可轉債訊號 JSON")

    updated_at = Column(DateTime, default=datetime.now, comment="更新時間")

    __table_args__ = (
        Index("idx_slc_chip", "chip_normalized"),
        Index("idx_slc_tech", "tech_normalized"),
        Index("idx_slc_fund", "fund_normalized"),
    )


class DatabaseManager:
    """PostgreSQL 資料庫管理器"""

    def __init__(self, database_url: Optional[str] = None):
        """
        初始化資料庫連線

        Args:
            database_url: PostgreSQL 連線字串
        """
        if database_url is None:
            # 使用環境變數中的 PostgreSQL 連線
            database_url = os.environ.get(
                "DATABASE_URL",
                "postgresql+asyncpg://twstock_user:twstock_password123@localhost:5432/twstock",
            )

        # 驗證是 PostgreSQL 連線
        if not "postgresql" in database_url:
            raise ValueError(
                "只支援 PostgreSQL 資料庫，請提供正確的 PostgreSQL 連線字串"
            )

        # PostgreSQL 並發設定
        self._db_semaphore = asyncio.Semaphore(20)

        # API 最新資料日期（由 fetcher 設定）
        self.api_latest_date = None

        # 異步引擎設定
        self.async_engine = create_async_engine(
            database_url,
            echo=False,
            poolclass=NullPool,
            future=True,
            pool_pre_ping=True,
            pool_recycle=300,
        )
        self.async_session_factory = async_sessionmaker(
            self.async_engine, class_=AsyncSession, expire_on_commit=False
        )

        # 同步引擎（用於初始化）
        sync_url = database_url.replace("+asyncpg", "")
        self.sync_engine = create_engine(sync_url, echo=False)

        print(f"Database initialized: {database_url.split('@')[0]}@***")

    async def init_database(self):
        """初始化資料庫表格（處理多實例同時啟動的競態條件）"""
        from sqlalchemy.exc import IntegrityError, OperationalError

        try:
            async with self.async_engine.begin() as conn:
                await conn.run_sync(Base.metadata.create_all)
        except (IntegrityError, OperationalError):
            # 多個 Cloud Run 實例同時啟動時，create_all 可能因
            # pg_type_typname_nsp_index 重複而失敗，忽略即可
            pass

    @asynccontextmanager
    async def get_session(self):
        """取得資料庫 session (使用 context manager)"""
        async with self.async_session_factory() as session:
            try:
                yield session
                await session.commit()
            except Exception:
                await session.rollback()
                raise

    async def close(self):
        """關閉資料庫連線"""
        await self.async_engine.dispose()

    async def save_stock_info(self, stock_id: str, info_data: pd.Series):
        """儲存股票基本資訊（使用批次 SQL INSERT）"""
        stock_id = stock_id.lower()  # 統一使用小寫 ID
        async with self._db_semaphore:
            async with self.get_session() as session:
                # 儲存基本資訊
                await session.execute(
                    text(
                        """
                        INSERT INTO stock_info (stock_id, capital, outstanding_shares, per, cash_dividend, stock_dividend, updated_at)
                        VALUES (:stock_id, :capital, :outstanding_shares, :per, :cash_dividend, :stock_dividend, :updated_at)
                        ON CONFLICT (stock_id) DO NOTHING
                    """
                    ),
                    {
                        "stock_id": stock_id,
                        "capital": (
                            float(info_data.get("capital", 1.0))
                            if pd.notna(info_data.get("capital"))
                            else 1.0
                        ),
                        "outstanding_shares": (
                            float(info_data.get("outstanding_shares", 1.0))
                            if pd.notna(info_data.get("outstanding_shares"))
                            else 1.0
                        ),
                        "per": (
                            float(info_data.get("PER"))
                            if pd.notna(info_data.get("PER"))
                            else None
                        ),
                        "cash_dividend": (
                            float(info_data.get("cash_dividend", 0))
                            if pd.notna(info_data.get("cash_dividend"))
                            else 0
                        ),
                        "stock_dividend": (
                            float(info_data.get("stock_dividend", 0))
                            if pd.notna(info_data.get("stock_dividend"))
                            else 0
                        ),
                        "updated_at": datetime.now(),
                    },
                )

                # 批次儲存 EPS 資料
                eps_records = []
                for key in info_data.index:
                    if "Q" in key and "/" in key:
                        try:
                            year, quarter = key.split("/")
                            quarter = int(quarter.replace("Q", ""))
                            year = int(year)
                            eps_value = (
                                float(info_data[key])
                                if pd.notna(info_data[key])
                                else None
                            )

                            if eps_value is not None:
                                eps_records.append(
                                    {
                                        "stock_id": stock_id,
                                        "year": year,
                                        "quarter": quarter,
                                        "eps": eps_value,
                                    }
                                )
                        except Exception as e:
                            print(f"Error preparing EPS for {key}: {e}")

                # 批次插入 EPS 資料
                if eps_records:
                    await session.execute(
                        text(
                            """
                            INSERT INTO stock_eps (stock_id, year, quarter, eps)
                            VALUES (:stock_id, :year, :quarter, :eps)
                            ON CONFLICT (stock_id, year, quarter) DO NOTHING
                        """
                        ),
                        eps_records,
                    )

    async def save_stock_dividends(self, stock_id: str, dividends_list: List[dict]):
        """批次儲存歷史股利資料（UPSERT）

        Args:
            stock_id: 股票代碼
            dividends_list: [{year, cash_dividend, stock_dividend}, ...]
        """
        stock_id = stock_id.lower()
        if not dividends_list:
            return

        async with self._db_semaphore:
            async with self.get_session() as session:
                await session.execute(
                    text(
                        """
                        INSERT INTO stock_dividend (stock_id, year, cash_dividend, stock_dividend, updated_at)
                        VALUES (:stock_id, :year, :cash_dividend, :stock_dividend, :updated_at)
                        ON CONFLICT (stock_id, year) DO UPDATE SET
                            cash_dividend = EXCLUDED.cash_dividend,
                            stock_dividend = EXCLUDED.stock_dividend,
                            updated_at = EXCLUDED.updated_at
                    """
                    ),
                    [
                        {
                            "stock_id": stock_id,
                            "year": d["year"],
                            "cash_dividend": d.get("cash_dividend", 0),
                            "stock_dividend": d.get("stock_dividend", 0),
                            "updated_at": datetime.now(),
                        }
                        for d in dividends_list
                    ],
                )

    async def save_daily_data(self, stock_id: str, daily_data: pd.DataFrame):
        """儲存每日交易資料（使用批次 SQL INSERT）"""
        stock_id = stock_id.lower()  # 統一使用小寫 ID
        if daily_data.empty:
            print(f"Daily data for {stock_id} is empty, skipping save")
            return

        # 預處理 DataFrame
        daily_data = prepare_dataframe_for_db(daily_data)

        if daily_data.empty:
            print(
                f"ERROR: DataFrame became empty after prepare_dataframe_for_db for {stock_id}"
            )
            return

        async with self._db_semaphore:
            async with self.get_session() as session:
                # 準備所有資料的列表
                daily_records = []
                inst_records = []
                major_records = []
                margin_records = []

                for _, row in daily_data.iterrows():
                    # 處理日期
                    if "date" in row and pd.notna(row["date"]):
                        if isinstance(row["date"], str):
                            date_value = datetime.strptime(row["date"], "%Y-%m-%d")
                        elif hasattr(row["date"], "to_pydatetime"):
                            date_value = row["date"].to_pydatetime()
                        else:
                            date_value = row["date"]
                    else:
                        continue

                    # 每日價格資料
                    unique_str = f"{stock_id}_{date_value.strftime('%Y%m%d')}"
                    hash_obj = hashlib.md5(unique_str.encode())
                    simple_id = int(hash_obj.hexdigest()[:15], 16)

                    daily_records.append(
                        {
                            "id": simple_id,
                            "stock_id": stock_id,
                            "date": date_value,
                            "volume": safe_float(row.get("volume")),
                            "open": safe_float(row.get("open")),
                            "high": safe_float(row.get("high")),
                            "low": safe_float(row.get("low")),
                            "close": safe_float(row.get("close")),
                        }
                    )

                    # 法人資料
                    if "foreign_shares" in row:
                        inst_id = int(
                            hashlib.md5(
                                f"{stock_id}_inst_{date_value.strftime('%Y%m%d')}".encode()
                            ).hexdigest()[:15],
                            16,
                        )
                        inst_records.append(
                            {
                                "id": inst_id,
                                "stock_id": stock_id,
                                "date": date_value,
                                "foreign_shares": safe_float(row.get("foreign_shares")),
                                "investment_trust_shares": safe_float(
                                    row.get("investment_trust_shares")
                                ),
                                "dealer_shares": safe_float(row.get("dealer_shares")),
                                "sum_holding_rate": safe_float(
                                    row.get("sum_holding_rate")
                                ),
                                "foreign_holding_rate": safe_float(
                                    row.get("foreign_holding_rate")
                                ),
                                "investment_trust_holding_rate": safe_float(
                                    row.get("investment_trust_holding_rate")
                                ),
                                "dealer_holding_rate": safe_float(
                                    row.get("dealer_holding_rate")
                                ),
                            }
                        )

                    # 主力資料
                    if "major_investors" in row:
                        major_id = int(
                            hashlib.md5(
                                f"{stock_id}_major_{date_value.strftime('%Y%m%d')}".encode()
                            ).hexdigest()[:15],
                            16,
                        )
                        major_records.append(
                            {
                                "id": major_id,
                                "stock_id": stock_id,
                                "date": date_value,
                                "major_investors": safe_float(
                                    row.get("major_investors")
                                ),
                                "agent_diff": safe_float(row.get("agent_diff")),
                                "skp5": safe_float(row.get("skp5")),
                                "skp20": safe_float(row.get("skp20")),
                            }
                        )

                    # 融資融券資料
                    if "lending_balance" in row:
                        margin_id = int(
                            hashlib.md5(
                                f"{stock_id}_margin_{date_value.strftime('%Y%m%d')}".encode()
                            ).hexdigest()[:15],
                            16,
                        )
                        margin_records.append(
                            {
                                "id": margin_id,
                                "stock_id": stock_id,
                                "date": date_value,
                                "lending_balance": safe_float(
                                    row.get("lending_balance")
                                ),
                                "borrowing_balance": safe_float(
                                    row.get("borrowing_balance")
                                ),
                                "balance_limit": safe_float(row.get("balance_limit")),
                            }
                        )

                # 批次插入每日價格資料
                if daily_records:
                    await session.execute(
                        text(
                            """
                            INSERT INTO stock_daily
                            (id, stock_id, date, volume, open, high, low, close)
                            VALUES (:id, :stock_id, :date, :volume, :open, :high, :low, :close)
                            ON CONFLICT (stock_id, date) DO NOTHING
                        """
                        ),
                        daily_records,
                    )

                # 批次插入法人資料
                if inst_records:
                    await session.execute(
                        text(
                            """
                            INSERT INTO institutional_investors
                            (id, stock_id, date, foreign_shares, investment_trust_shares, dealer_shares, sum_holding_rate,
                             foreign_holding_rate, investment_trust_holding_rate, dealer_holding_rate)
                            VALUES (:id, :stock_id, :date, :foreign_shares, :investment_trust_shares, :dealer_shares, :sum_holding_rate,
                                    :foreign_holding_rate, :investment_trust_holding_rate, :dealer_holding_rate)
                            ON CONFLICT (stock_id, date) DO NOTHING
                        """
                        ),
                        inst_records,
                    )

                # 批次插入主力資料
                if major_records:
                    await session.execute(
                        text(
                            """
                            INSERT INTO major_investors
                            (id, stock_id, date, major_investors, agent_diff, skp5, skp20)
                            VALUES (:id, :stock_id, :date, :major_investors, :agent_diff, :skp5, :skp20)
                            ON CONFLICT (stock_id, date) DO NOTHING
                        """
                        ),
                        major_records,
                    )

                # 批次插入融資融券資料
                if margin_records:
                    await session.execute(
                        text(
                            """
                            INSERT INTO margin_trading
                            (id, stock_id, date, lending_balance, borrowing_balance, balance_limit)
                            VALUES (:id, :stock_id, :date, :lending_balance, :borrowing_balance, :balance_limit)
                            ON CONFLICT (stock_id, date) DO NOTHING
                        """
                        ),
                        margin_records,
                    )

    async def save_concentration_data(
        self, stock_id: str, concentration_df: pd.DataFrame
    ):
        """儲存籌碼集中度資料（批次 SQL INSERT）"""
        stock_id = stock_id.lower()  # 統一使用小寫 ID
        if concentration_df.empty:
            return

        async with self._db_semaphore:
            async with self.get_session() as session:
                # 準備批次資料
                concentration_records = []

                for _, row in concentration_df.iterrows():
                    # 處理日期
                    if "date" in row and pd.notna(row["date"]):
                        if isinstance(row["date"], str):
                            date_value = datetime.strptime(row["date"], "%Y-%m-%d")
                        elif hasattr(row["date"], "to_pydatetime"):
                            date_value = row["date"].to_pydatetime()
                        else:
                            date_value = row["date"]
                    else:
                        continue

                    # 生成唯一 ID（使用 MD5 hash）
                    unique_str = f"{stock_id}_conc_{date_value.strftime('%Y%m%d')}"
                    hash_obj = hashlib.md5(unique_str.encode())
                    simple_id = int(hash_obj.hexdigest()[:15], 16)

                    concentration_records.append(
                        {
                            "id": simple_id,
                            "stock_id": stock_id,
                            "date": date_value,
                            "more_than_400": safe_float(row.get("moreThan400")),
                            "more_than_1000": safe_float(row.get("moreThan1000")),
                            "less_than_20": safe_float(row.get("lessThan20")),
                            "close": safe_float(row.get("close")),
                            "director_ratio": safe_float(row.get("directorRatio")),
                            "rate_of_foreign_holding": safe_float(
                                row.get("rateOfForeignHolding")
                            ),
                            "rate_of_ing_holding": safe_float(
                                row.get("rateOfINGHolding")
                            ),
                            "rate_of_dealer_holding": safe_float(
                                row.get("rateOfDealerHolding")
                            ),
                        }
                    )

                # 批次 INSERT（使用 ON CONFLICT DO NOTHING，有資料就忽略）
                if concentration_records:
                    await session.execute(
                        text(
                            """
                            INSERT INTO concentration_data (
                                id, stock_id, date, more_than_400, more_than_1000,
                                less_than_20, "close", director_ratio,
                                rate_of_foreign_holding, rate_of_ing_holding,
                                rate_of_dealer_holding
                            )
                            VALUES (
                                :id, :stock_id, :date, :more_than_400, :more_than_1000,
                                :less_than_20, :close, :director_ratio,
                                :rate_of_foreign_holding, :rate_of_ing_holding,
                                :rate_of_dealer_holding
                            )
                            ON CONFLICT (stock_id, date) DO NOTHING
                        """
                        ),
                        concentration_records,
                    )
                    await session.commit()

    async def save_monthly_revenue(self, stock_id: str, revenue_df: pd.DataFrame):
        """
        儲存股票月營收資料到資料庫

        Args:
            stock_id: 股票代碼
            revenue_df: 月營收 DataFrame，欄位包含：
                       - year: 年份
                       - month: 月份
                       - revenue: 月營收（千元）
                       - mom_change: 月增率 (%)
                       - yoy_change: 年增率 (%)
                       - cumulative_revenue: 累計營收（千元）
                       - cumulative_yoy_change: 累計年增率 (%)

        說明：
            - 使用 ON CONFLICT DO NOTHING 實現忽略語意
            - 主鍵衝突時不更新（保留舊資料）
            - 批次插入所有記錄（效能優化）
        """
        stock_id = stock_id.lower()  # 統一使用小寫 ID
        if revenue_df.empty:
            return

        async with self._db_semaphore:
            async with self.get_session() as session:
                # 轉換 DataFrame 為字典列表
                records = []
                for _, row in revenue_df.iterrows():
                    records.append(
                        {
                            "stock_id": stock_id,
                            "year": int(row["year"]),
                            "month": int(row["month"]),
                            "revenue": (
                                float(row["revenue"])
                                if pd.notna(row["revenue"])
                                else None
                            ),
                            "mom_change": (
                                float(row["mom_change"])
                                if pd.notna(row["mom_change"])
                                else None
                            ),
                            "yoy_change": (
                                float(row["yoy_change"])
                                if pd.notna(row["yoy_change"])
                                else None
                            ),
                            "cumulative_revenue": (
                                float(row["cumulative_revenue"])
                                if pd.notna(row["cumulative_revenue"])
                                else None
                            ),
                            "cumulative_yoy_change": (
                                float(row["cumulative_yoy_change"])
                                if pd.notna(row["cumulative_yoy_change"])
                                else None
                            ),
                            "updated_at": datetime.now(),
                        }
                    )

                # 批次 INSERT ON CONFLICT DO NOTHING（有資料就忽略）
                if records:
                    await session.execute(
                        text(
                            """
                            INSERT INTO stock_monthly_revenue (
                                stock_id, year, month, revenue, mom_change, yoy_change,
                                cumulative_revenue, cumulative_yoy_change, updated_at
                            )
                            VALUES (
                                :stock_id, :year, :month, :revenue, :mom_change, :yoy_change,
                                :cumulative_revenue, :cumulative_yoy_change, :updated_at
                            )
                            ON CONFLICT (stock_id, year, month) DO NOTHING
                        """
                        ),
                        records,
                    )
                    await session.commit()

    async def bulk_load_monthly_revenue(
        self, stock_ids: List[str], months: int = 12
    ) -> Dict[str, pd.DataFrame]:
        """
        批次載入多支股票的月營收資料（單一 SQL 查詢）

        Args:
            stock_ids: 股票代碼列表
            months: 每支股票載入最近幾個月（預設 12 個月）

        Returns:
            Dict[stock_id, DataFrame]: 每支股票的月營收 DataFrame

        說明：
            - 使用 Window Function 限制每股最多 N 個月
            - 單一 SQL 查詢載入所有股票（避免 N+1 問題）
            - 按年月排序（最新在前）
        """
        if not stock_ids:
            return {}

        # 統一使用小寫 ID
        stock_ids_lower = [sid.lower() for sid in stock_ids]

        async with self.get_session() as session:
            # SQL 查詢：使用 ROW_NUMBER() 限制每股最多 N 個月
            query = text(
                """
                WITH ranked_revenue AS (
                    SELECT
                        stock_id,
                        year,
                        month,
                        revenue,
                        mom_change,
                        yoy_change,
                        cumulative_revenue,
                        cumulative_yoy_change,
                        updated_at,
                        ROW_NUMBER() OVER (
                            PARTITION BY stock_id
                            ORDER BY year DESC, month DESC
                        ) AS rn
                    FROM stock_monthly_revenue
                    WHERE stock_id = ANY(:stock_ids)
                )
                SELECT
                    stock_id,
                    year,
                    month,
                    revenue,
                    mom_change,
                    yoy_change,
                    cumulative_revenue,
                    cumulative_yoy_change,
                    updated_at
                FROM ranked_revenue
                WHERE rn <= :months
                ORDER BY stock_id, year DESC, month DESC
            """
            )

            result = await session.execute(
                query, {"stock_ids": stock_ids_lower, "months": months}
            )
            rows = result.fetchall()

            # 轉換為 Dict[stock_id, DataFrame]
            revenue_dict = {}
            for row in rows:
                stock_id_value = row.stock_id
                if stock_id_value not in revenue_dict:
                    revenue_dict[stock_id_value] = []

                revenue_dict[stock_id_value].append(
                    {
                        "year": row.year,
                        "month": row.month,
                        "revenue": row.revenue,
                        "mom_change": row.mom_change,
                        "yoy_change": row.yoy_change,
                        "cumulative_revenue": row.cumulative_revenue,
                        "cumulative_yoy_change": row.cumulative_yoy_change,
                        "updated_at": row.updated_at,
                    }
                )

            # 轉換為 DataFrame
            for stock_id_value in revenue_dict:
                revenue_dict[stock_id_value] = pd.DataFrame(
                    revenue_dict[stock_id_value]
                )

            return revenue_dict

    async def save_stock_list(self, stocks: List[Dict[str, Any]]):
        """儲存股票清單（使用批次 SQL INSERT）"""
        if not stocks:
            return

        # 準備批次資料
        stock_records = []
        for stock in stocks:
            stock_records.append(
                {
                    "stock_id": stock.get("id").lower(),  # 統一使用小寫 ID
                    "name": stock.get("name"),
                    "type": stock.get("type"),
                    "country": stock.get("country", "TW"),
                    "market": stock.get("market", "TWSE"),
                    "is_active": stock.get("is_active", True),
                    "updated_at": datetime.now(),
                }
            )

        # 批次插入
        async with self.get_session() as session:
            await session.execute(
                text(
                    """
                    INSERT INTO stock_list
                    (stock_id, name, type, country, market, is_active, updated_at)
                    VALUES (:stock_id, :name, :type, :country, :market, :is_active, :updated_at)
                    ON CONFLICT (stock_id) DO NOTHING
                """
                ),
                stock_records,
            )

    async def get_latest_data_date(self, stock_id: str) -> Optional[datetime]:
        """取得某支股票最新的資料日期"""
        stock_id = stock_id.lower()  # 統一使用小寫 ID
        async with self.get_session() as session:
            result = await session.execute(
                text(
                    """
                    SELECT MAX(date) as latest_date
                    FROM stock_daily
                    WHERE stock_id = :stock_id
                """
                ),
                {"stock_id": stock_id},
            )
            row = result.fetchone()
            return row.latest_date if row else None

    async def bulk_check_needs_update(
        self, stock_ids: List[str], days_threshold: int = 1
    ) -> Dict[str, bool]:
        """批次檢查多支股票是否需要更新"""
        # 統一使用小寫 ID
        stock_ids = [stock_id.lower() for stock_id in stock_ids]
        result = {}

        try:
            async with self.get_session() as session:
                # 一次性查詢所有股票的最新資料日期和追蹤狀態
                db_result = await session.execute(
                    text(
                        """
                        SELECT d.stock_id, MAX(d.date) as latest_date
                        FROM stock_daily d
                        WHERE d.stock_id = ANY(:stock_ids)
                        GROUP BY d.stock_id
                    """
                    ),
                    {"stock_ids": stock_ids},
                )

                stock_dates = {
                    row.stock_id: row.latest_date for row in db_result.fetchall()
                }

                # 一次性查詢追蹤狀態
                tracker_result = await session.execute(
                    text(
                        """
                        SELECT stock_id, last_checked_at, daily_last_date
                        FROM stock_update_tracker 
                        WHERE stock_id = ANY(:stock_ids)
                    """
                    ),
                    {"stock_ids": stock_ids},
                )

                tracker_data = {
                    row.stock_id: {
                        "last_checked_at": row.last_checked_at,
                        "daily_last_date": row.daily_last_date,
                    }
                    for row in tracker_result.fetchall()
                }

                # 檢查每支股票
                today = date.today()
                for stock_id in stock_ids:
                    latest_date = stock_dates.get(stock_id)

                    if latest_date is None:
                        result[stock_id] = True  # 沒有資料，需要更新
                        continue

                    # 轉換日期並檢查
                    if isinstance(latest_date, str):
                        try:
                            latest_date = datetime.strptime(
                                latest_date, "%Y-%m-%d"
                            ).date()
                        except ValueError:
                            result[stock_id] = True
                            continue
                    elif isinstance(latest_date, datetime):
                        latest_date = latest_date.date()

                    # 如果有 API 最新日期，直接比較
                    if self.api_latest_date:
                        # 資料庫日期 < API 最新日期 → 需要更新
                        result[stock_id] = latest_date < self.api_latest_date
                        continue

                    # 沒有 API 最新日期時，使用舊的邏輯
                    # 檢查追蹤狀態，如果最近已經檢查過且 API 沒有更新資料，就不重複更新
                    tracker = tracker_data.get(stock_id)
                    if tracker:
                        last_checked = tracker.get("last_checked_at")
                        daily_last_date = tracker.get("daily_last_date")

                        if (
                            last_checked
                            and last_checked.date() >= today
                            and daily_last_date
                            and daily_last_date.date() == latest_date
                        ):
                            # 今天已經檢查過且日期一致，不需要更新
                            result[stock_id] = False
                            continue

                    # 檢查是否需要更新（考慮交易日）
                    result[stock_id] = self._needs_update_considering_trading_days(
                        latest_date, days_threshold
                    )

                return result

        except Exception as e:
            print(f"Batch check error: {e}")
            # 如果批次檢查失敗，回退到全部需要更新
            return {stock_id: True for stock_id in stock_ids}

    async def bulk_check_monthly_revenue_needs_update(
        self, stock_ids: List[str]
    ) -> Dict[str, bool]:
        """
        批次檢查多支股票的月營收是否需要更新

        Args:
            stock_ids: 股票代碼列表

        Returns:
            Dict[stock_id, needs_update]:
            - True: 需要從 API 更新
            - False: 資料庫已有最新資料

        判斷邏輯:
            1. DB 沒有資料 → 需要更新
            2. 最新月份 < 當月 → 需要更新（例如 DB 有 2025/12，現在是 2026/01）
            3. 已在當月且今日檢查過 → 不需要更新（冪等性）
        """
        stock_ids = [sid.lower() for sid in stock_ids]
        result = {}

        try:
            async with self.get_session() as session:
                # 一次查詢所有股票最新的月營收記錄（避免 N+1）
                latest_result = await session.execute(
                    text("""
                        WITH latest_revenue AS (
                            SELECT
                                stock_id,
                                year,
                                month,
                                updated_at,
                                ROW_NUMBER() OVER (
                                    PARTITION BY stock_id
                                    ORDER BY year DESC, month DESC
                                ) as rn
                            FROM stock_monthly_revenue
                            WHERE stock_id = ANY(:stock_ids)
                        )
                        SELECT stock_id, year, month, updated_at
                        FROM latest_revenue
                        WHERE rn = 1
                    """),
                    {"stock_ids": stock_ids},
                )

                latest_data = {
                    row.stock_id: {
                        "year": row.year,
                        "month": row.month,
                        "updated_at": row.updated_at,
                    }
                    for row in latest_result.fetchall()
                }

                # 判斷邏輯
                current_year = datetime.now().year
                current_month = datetime.now().month
                today = date.today()

                for stock_id in stock_ids:
                    if stock_id not in latest_data:
                        # DB 沒有資料 → 需要更新
                        result[stock_id] = True
                        continue

                    db_year = latest_data[stock_id]["year"]
                    db_month = latest_data[stock_id]["month"]
                    updated_at = latest_data[stock_id]["updated_at"]

                    # 檢查是否已是當月最新資料
                    if db_year == current_year and db_month == current_month:
                        # 已在當月更新過，檢查是否今天已經檢查過（避免重複請求）
                        if updated_at and updated_at.date() >= today:
                            # 今天已經檢查過 → 不需要更新
                            result[stock_id] = False
                            continue

                    # 判斷是否需要更新（考慮月份差異）
                    # 例如：DB 有 2025/12，現在是 2026/01 → 需要更新
                    db_month_total = db_year * 12 + db_month
                    current_month_total = current_year * 12 + current_month

                    # 最新月份 < 當月 → 需要更新
                    result[stock_id] = db_month_total < current_month_total

                return result

        except Exception as e:
            print(f"⚠️  批次檢查月營收狀態失敗: {e}")
            # 查詢失敗時，保守起見，全部標記為需要更新
            return {stock_id: True for stock_id in stock_ids}

    async def bulk_load_daily_data(
        self, stock_ids: List[str], days: int = 490
    ) -> Dict[str, pd.DataFrame]:
        """批次載入多支股票的每日資料（包含機構數據）

        Args:
            stock_ids: 股票代碼列表
            days: 載入最近幾天的資料

        Returns:
            Dict[stock_id, DataFrame]: 每支股票的 DataFrame
            DataFrame columns: [date, open, high, low, close, volume, foreign, ...]
        """
        stock_ids = [sid.lower() for sid in stock_ids]

        async with self.get_session() as session:
            # 🔧 修復：加入 LEFT JOIN 載入機構數據（技術指標計算需要）
            # 🆕 讀取張數並 JOIN stock_info 來計算百分比
            result = await session.execute(
                text(
                    f"""
                    SELECT
                        d.stock_id,
                        d.date,
                        d.open,
                        d.high,
                        d.low,
                        d.close,
                        d.volume,
                        COALESCE(i.foreign_shares, 0) as foreign_shares,
                        COALESCE(i.investment_trust_shares, 0) as investment_trust_shares,
                        COALESCE(i.dealer_shares, 0) as dealer_shares,
                        COALESCE(s.outstanding_shares, 1) as outstanding_shares,
                        COALESCE(i.foreign_holding_rate, 0) as foreign_holding_rate,
                        COALESCE(i.investment_trust_holding_rate, 0) as investment_trust_holding_rate,
                        COALESCE(i.dealer_holding_rate, 0) as dealer_holding_rate,
                        COALESCE(i.sum_holding_rate, 0) as sum_holding_rate,
                        COALESCE(m.major_investors, 0) as major_investors,
                        COALESCE(m.agent_diff, 0) as agent_diff,
                        COALESCE(m.skp5, 0) as skp5,
                        COALESCE(m.skp20, 0) as skp20,
                        COALESCE(mt.lending_balance, 0) as lending_balance,
                        COALESCE(mt.borrowing_balance, 0) as borrowing_balance,
                        COALESCE(mt.balance_limit, 0) as balance_limit
                    FROM stock_daily d
                    LEFT JOIN institutional_investors i
                        ON d.stock_id = i.stock_id AND d.date = i.date
                    LEFT JOIN stock_info s
                        ON d.stock_id = s.stock_id
                    LEFT JOIN major_investors m
                        ON d.stock_id = m.stock_id AND d.date = m.date
                    LEFT JOIN margin_trading mt
                        ON d.stock_id = mt.stock_id AND d.date = mt.date
                    WHERE d.stock_id = ANY(:stock_ids)
                    AND d.date >= CURRENT_DATE - INTERVAL '{days} days'
                    ORDER BY d.stock_id, d.date DESC
                """
                ),
                {"stock_ids": stock_ids},
            )

            # 將結果按股票代碼分組
            stock_data_dict = {}
            for row in result:
                stock_id = row.stock_id
                if stock_id not in stock_data_dict:
                    stock_data_dict[stock_id] = []

                # 🆕 從張數計算百分比
                total_stock = (
                    row.outstanding_shares / 1000 if row.outstanding_shares > 0 else 1.0
                )
                foreign_pct = (
                    round(row.foreign_shares / total_stock * 100, 2)
                    if total_stock > 0
                    else 0
                )
                investment_trust_pct = (
                    round(row.investment_trust_shares / total_stock * 100, 2)
                    if total_stock > 0
                    else 0
                )
                dealer_pct = (
                    round(row.dealer_shares / total_stock * 100, 2)
                    if total_stock > 0
                    else 0
                )

                stock_data_dict[stock_id].append(
                    {
                        "date": row.date,
                        "open": row.open,
                        "high": row.high,
                        "low": row.low,
                        "close": row.close,
                        "volume": row.volume,
                        "foreign": foreign_pct,
                        "investment_trust": investment_trust_pct,
                        "dealer": dealer_pct,
                        "foreign_holding_rate": row.foreign_holding_rate,
                        "investment_trust_holding_rate": row.investment_trust_holding_rate,
                        "dealer_holding_rate": row.dealer_holding_rate,
                        "sum_holding_rate": row.sum_holding_rate,
                        "major_investors": row.major_investors,
                        "agent_diff": row.agent_diff,
                        "skp5": row.skp5,
                        "skp20": row.skp20,
                        "lending_balance": row.lending_balance,
                        "borrowing_balance": row.borrowing_balance,
                        "balance_limit": row.balance_limit,
                    }
                )

            # 轉換為 DataFrame
            result_dict = {}
            for stock_id, data_list in stock_data_dict.items():
                result_dict[stock_id] = pd.DataFrame(data_list)

            return result_dict

    async def bulk_load_concentration_data(
        self, stock_ids: List[str], weeks: int = 10
    ) -> Dict[str, pd.DataFrame]:
        """批次載入多支股票的籌碼集中度資料（使用單一 SQL 查詢）

        Args:
            stock_ids: 股票代碼列表
            weeks: 載入最近幾週的資料（每週約 5 個交易日）

        Returns:
            Dict[stock_id, DataFrame]: 每支股票的 DataFrame
        """
        stock_ids = [sid.lower() for sid in stock_ids]
        days = weeks * 7  # 大約 10 週 = 70 天

        async with self.get_session() as session:
            # 使用單一 SQL 查詢取得所有股票的集中度資料
            result = await session.execute(
                text(
                    f"""
                    SELECT
                        stock_id,
                        date,
                        more_than_400,
                        more_than_1000,
                        less_than_20,
                        "close",
                        director_ratio,
                        rate_of_foreign_holding,
                        rate_of_ing_holding,
                        rate_of_dealer_holding
                    FROM concentration_data
                    WHERE stock_id = ANY(:stock_ids)
                    AND date >= CURRENT_DATE - INTERVAL '{days} days'
                    ORDER BY stock_id, date DESC
                """
                ),
                {"stock_ids": stock_ids},
            )

            # 將結果按股票代碼分組
            stock_data_dict = {}
            for row in result:
                stock_id = row.stock_id
                if stock_id not in stock_data_dict:
                    stock_data_dict[stock_id] = []

                stock_data_dict[stock_id].append(
                    {
                        "date": row.date,
                        "moreThan400": row.more_than_400 or 0,
                        "moreThan1000": row.more_than_1000 or 0,
                        "lessThan20": row.less_than_20 or 0,
                        "close": row.close or 0,
                        "directorRatio": row.director_ratio or 0,
                        "rateOfForeignHolding": row.rate_of_foreign_holding or 0,
                        "rateOfINGHolding": row.rate_of_ing_holding or 0,
                        "rateOfDealerHolding": row.rate_of_dealer_holding or 0,
                    }
                )

            # 轉換為 DataFrame
            result_dict = {}
            for stock_id, records in stock_data_dict.items():
                df = pd.DataFrame(records)
                # API 資料已經是週資料，已按日期降序排列，直接取前 N 週
                weekly_df = df.head(weeks)
                result_dict[stock_id] = weekly_df

            return result_dict

    async def bulk_load_stock_info(self, stock_ids: List[str]) -> Dict[str, pd.Series]:
        """批次載入多支股票的基本資訊（使用單一 SQL 查詢）

        Args:
            stock_ids: 股票代碼列表

        Returns:
            Dict[stock_id, Series]: 每支股票的資訊 Series
        """
        stock_ids = [sid.lower() for sid in stock_ids]

        async with self.get_session() as session:
            # 載入股票基本資訊
            info_result = await session.execute(
                text(
                    """
                    SELECT stock_id, capital, outstanding_shares, per, cash_dividend, stock_dividend
                    FROM stock_info
                    WHERE stock_id = ANY(:stock_ids)
                """
                ),
                {"stock_ids": stock_ids},
            )

            info_dict = {}
            for row in info_result:
                info_dict[row.stock_id] = {
                    "id": row.stock_id,
                    "capital": row.capital,
                    "outstanding_shares": row.outstanding_shares,
                    "PER": row.per,
                    "cash_dividend": row.cash_dividend,
                    "stock_dividend": row.stock_dividend,
                }

            # 載入 EPS 資料
            eps_result = await session.execute(
                text(
                    """
                    SELECT stock_id, year, quarter, eps
                    FROM stock_eps
                    WHERE stock_id = ANY(:stock_ids)
                    ORDER BY stock_id, year DESC, quarter DESC
                """
                ),
                {"stock_ids": stock_ids},
            )

            for row in eps_result:
                stock_id = row.stock_id
                if stock_id in info_dict:
                    key = f"{row.year}/{row.quarter}Q"
                    info_dict[stock_id][key] = row.eps

            # 轉換為 Series
            result_dict = {}
            for stock_id, info_data in info_dict.items():
                result_dict[stock_id] = pd.Series(info_data)

            return result_dict

    async def bulk_load_eps(
        self, stock_ids: List[str], quarters: int = 4
    ) -> Dict[str, pd.DataFrame]:
        """批次載入最近 N 季 EPS（使用單一 SQL 查詢）

        Args:
            stock_ids: 股票代碼列表
            quarters: 要載入的季度數量（預設 4 季）

        Returns:
            Dict[stock_id, DataFrame]: 每支股票的 EPS DataFrame
            DataFrame columns: [year, quarter, eps]
            已按 year DESC, quarter DESC 排序（最新季在前）
        """
        stock_ids = [sid.lower() for sid in stock_ids]

        async with self.get_session() as session:
            # 使用 Window Function 取得每支股票最近 N 季的 EPS
            eps_result = await session.execute(
                text(
                    """
                    WITH ranked_eps AS (
                        SELECT
                            stock_id,
                            year,
                            quarter,
                            eps,
                            ROW_NUMBER() OVER (
                                PARTITION BY stock_id
                                ORDER BY year DESC, quarter DESC
                            ) as rn
                        FROM stock_eps
                        WHERE stock_id = ANY(:stock_ids)
                    )
                    SELECT stock_id, year, quarter, eps
                    FROM ranked_eps
                    WHERE rn <= :quarters
                    ORDER BY stock_id, year DESC, quarter DESC
                """
                ),
                {"stock_ids": stock_ids, "quarters": quarters},
            )

            # 按 stock_id 分組建立 DataFrame
            from collections import defaultdict

            eps_dict = defaultdict(list)
            for row in eps_result:
                eps_dict[row.stock_id].append(
                    {"year": row.year, "quarter": row.quarter, "eps": row.eps}
                )

            # 轉換為 DataFrame
            result_dict = {}
            for stock_id, eps_list in eps_dict.items():
                if eps_list:
                    result_dict[stock_id] = pd.DataFrame(eps_list)
                else:
                    # 沒有資料時回傳空 DataFrame
                    result_dict[stock_id] = pd.DataFrame(
                        columns=["year", "quarter", "eps"]
                    )

            # 對於沒有 EPS 資料的股票，也回傳空 DataFrame
            for stock_id in stock_ids:
                if stock_id not in result_dict:
                    result_dict[stock_id] = pd.DataFrame(
                        columns=["year", "quarter", "eps"]
                    )

            return result_dict

    async def bulk_load_dividends(
        self, stock_ids: List[str], years: int = 5
    ) -> Dict[str, pd.DataFrame]:
        """批次載入最近 N 年歷史股利（使用單一 SQL 查詢）

        Args:
            stock_ids: 股票代碼列表
            years: 要載入的年數（預設 5 年）

        Returns:
            Dict[stock_id, DataFrame]: 每支股票的股利 DataFrame
            DataFrame columns: [year, cash_dividend, stock_dividend]
            已按 year DESC 排序（最新年度在前）
        """
        stock_ids = [sid.lower() for sid in stock_ids]

        async with self.get_session() as session:
            result = await session.execute(
                text(
                    """
                    WITH ranked_div AS (
                        SELECT
                            stock_id,
                            year,
                            cash_dividend,
                            stock_dividend,
                            ROW_NUMBER() OVER (
                                PARTITION BY stock_id
                                ORDER BY year DESC
                            ) AS rn
                        FROM stock_dividend
                        WHERE stock_id = ANY(:stock_ids)
                    )
                    SELECT stock_id, year, cash_dividend, stock_dividend
                    FROM ranked_div
                    WHERE rn <= :years
                    ORDER BY stock_id, year DESC
                """
                ),
                {"stock_ids": stock_ids, "years": years},
            )

            from collections import defaultdict

            div_dict = defaultdict(list)
            for row in result:
                div_dict[row.stock_id].append(
                    {
                        "year": row.year,
                        "cash_dividend": row.cash_dividend,
                        "stock_dividend": row.stock_dividend,
                    }
                )

            result_dict = {}
            for stock_id, div_list in div_dict.items():
                result_dict[stock_id] = pd.DataFrame(div_list)

            for stock_id in stock_ids:
                if stock_id not in result_dict:
                    result_dict[stock_id] = pd.DataFrame(
                        columns=["year", "cash_dividend", "stock_dividend"]
                    )

            return result_dict

    async def check_needs_update(self, stock_id: str, days_threshold: int = 1) -> bool:
        """檢查股票是否需要更新"""
        stock_id = stock_id.lower()  # 統一使用小寫 ID
        latest_date = await self.get_latest_data_date(stock_id)

        if latest_date is None:
            return True  # 沒有資料，需要更新

        # 轉換日期並檢查
        if isinstance(latest_date, str):
            try:
                latest_date = datetime.strptime(latest_date, "%Y-%m-%d").date()
            except ValueError:
                return True
        elif isinstance(latest_date, datetime):
            latest_date = latest_date.date()

        # 檢查追蹤狀態，如果最近已經檢查過且 API 沒有更新資料，就不重複更新
        tracker = await self.get_tracker(stock_id)
        if tracker:
            # 如果今天已經檢查過且最新日期沒變，就不需要重新更新
            last_checked = tracker.get("last_checked_at")
            daily_last_date = tracker.get("daily_last_date")

            if (
                last_checked
                and last_checked.date() >= date.today()
                and daily_last_date
                and daily_last_date.date() == latest_date
            ):
                # 今天已經檢查過且日期一致，不需要更新
                return False

        # 檢查是否需要更新（考慮交易日）
        return self._needs_update_considering_trading_days(latest_date, days_threshold)

    def _needs_update_considering_trading_days(
        self, latest_date: date, days_threshold: int = 1
    ) -> bool:
        """檢查是否需要更新，考慮交易日"""
        from datetime import timedelta

        today = date.today()

        # 獲取最後一個交易日
        last_trading_day = self._get_last_trading_day(today)

        # 如果最新資料日期 >= 最後交易日，則不需要更新
        if latest_date >= last_trading_day:
            return False

        # 計算距離最後交易日的工作天數
        trading_days_diff = self._count_trading_days(latest_date, last_trading_day)

        return trading_days_diff >= days_threshold

    def _get_last_trading_day(self, current_date: date) -> date:
        """獲取最後一個交易日（週一到週五）"""
        from datetime import timedelta

        # 如果是週末，找上一個週五
        if current_date.weekday() >= 5:  # 週六(5) 或週日(6)
            days_back = current_date.weekday() - 4  # 回到週五
            return current_date - timedelta(days=days_back)

        # 如果是週一到週五
        if current_date.weekday() == 0:  # 週一
            return current_date - timedelta(days=3)  # 上週五
        else:
            return current_date - timedelta(days=1)  # 前一個工作日

    def _count_trading_days(self, start_date: date, end_date: date) -> int:
        """計算兩個日期之間的交易日數量"""
        from datetime import timedelta

        if start_date >= end_date:
            return 0

        current = start_date + timedelta(days=1)
        trading_days = 0

        while current <= end_date:
            if current.weekday() < 5:  # 週一到週五
                trading_days += 1
            current += timedelta(days=1)

        return trading_days

    async def update_tracker(self, stock_id: str, **kwargs):
        """更新股票追蹤狀態

        支援的參數:
        - info_loaded: bool
        - info_updated_at: datetime
        - daily_loaded: bool
        - daily_updated_at: datetime
        - daily_first_date: datetime
        - daily_last_date: datetime
        - analysis_completed: bool (映射到 analysis_updated_at)
        - analysis_updated_at: datetime
        """
        async with self.get_session() as session:
            # 檢查是否存在
            result = await session.execute(
                text(
                    "SELECT stock_id FROM stock_update_tracker WHERE stock_id = :stock_id"
                ),
                {"stock_id": stock_id},
            )
            exists = result.fetchone() is not None

            if exists:
                # 更新現有記錄
                update_fields = [
                    "last_checked_at = :last_checked_at",
                    "updated_at = :updated_at",
                ]
                params = {
                    "stock_id": stock_id,
                    "last_checked_at": datetime.now(),
                    "updated_at": datetime.now(),
                }

                # 處理各種參數
                if "info_loaded" in kwargs:
                    update_fields.append("info_loaded = :info_loaded")
                    params["info_loaded"] = kwargs["info_loaded"]

                if "info_updated_at" in kwargs:
                    update_fields.append("info_updated_at = :info_updated_at")
                    params["info_updated_at"] = kwargs["info_updated_at"]

                if "daily_loaded" in kwargs:
                    update_fields.append("daily_loaded = :daily_loaded")
                    params["daily_loaded"] = kwargs["daily_loaded"]

                if "daily_updated_at" in kwargs:
                    update_fields.append("daily_updated_at = :daily_updated_at")
                    params["daily_updated_at"] = kwargs["daily_updated_at"]

                if "daily_first_date" in kwargs:
                    update_fields.append("daily_first_date = :daily_first_date")
                    params["daily_first_date"] = kwargs["daily_first_date"]

                if "daily_last_date" in kwargs:
                    update_fields.append("daily_last_date = :daily_last_date")
                    params["daily_last_date"] = kwargs["daily_last_date"]

                if "analysis_completed" in kwargs or "analysis_updated_at" in kwargs:
                    update_fields.append("analysis_updated_at = :analysis_updated_at")
                    params["analysis_updated_at"] = kwargs.get(
                        "analysis_updated_at", datetime.now()
                    )

                await session.execute(
                    text(
                        f"""
                        UPDATE stock_update_tracker
                        SET {', '.join(update_fields)}
                        WHERE stock_id = :stock_id
                    """
                    ),
                    params,
                )
            else:
                # 插入新記錄
                insert_params = {
                    "stock_id": stock_id,
                    "info_loaded": kwargs.get("info_loaded", False),
                    "daily_loaded": kwargs.get("daily_loaded", False),
                    "info_updated_at": kwargs.get("info_updated_at"),
                    "daily_updated_at": kwargs.get("daily_updated_at"),
                    "daily_first_date": kwargs.get("daily_first_date"),
                    "daily_last_date": kwargs.get("daily_last_date"),
                    "analysis_updated_at": kwargs.get("analysis_updated_at"),
                    "last_checked_at": datetime.now(),
                    "created_at": datetime.now(),
                    "updated_at": datetime.now(),
                }

                # 處理 analysis_completed
                if kwargs.get("analysis_completed"):
                    insert_params["analysis_updated_at"] = kwargs.get(
                        "analysis_updated_at", datetime.now()
                    )

                await session.execute(
                    text(
                        """
                        INSERT INTO stock_update_tracker 
                        (stock_id, info_loaded, daily_loaded, info_updated_at, daily_updated_at, 
                         daily_first_date, daily_last_date, analysis_updated_at,
                         last_checked_at, created_at, updated_at)
                        VALUES (:stock_id, :info_loaded, :daily_loaded, :info_updated_at, :daily_updated_at,
                                :daily_first_date, :daily_last_date, :analysis_updated_at,
                                :last_checked_at, :created_at, :updated_at)
                    """
                    ),
                    insert_params,
                )

    async def bulk_update_alert_status(
        self,
        attention_stocks: List[Dict],
        disposal_stocks: List[Dict],
    ):
        """
        批次更新警示狀態（UPSERT）

        Args:
            attention_stocks: 注意股清單 [{'stock_id': '1471', 'count': 3, 'reason': '...', 'date': '2026-01-31'}, ...]
            disposal_stocks: 處置股清單 [{'stock_id': '1471', 'type': '第一次處置', ...}, ...]
        """
        async with self._db_semaphore:
            async with self.get_session() as session:
                # 1. 更新注意股
                for stock in attention_stocks:
                    stock_id = stock['stock_id'].lower()
                    await session.execute(
                        text("""
                            INSERT INTO stock_alert_status
                            (stock_id, is_attention_stock, attention_count, attention_reason, attention_date,
                             last_checked_at, updated_at, created_at)
                            VALUES (:stock_id, true, :count, :reason, :date, :now, :now, :now)
                            ON CONFLICT (stock_id) DO UPDATE SET
                                is_attention_stock = true,
                                attention_count = :count,
                                attention_reason = :reason,
                                attention_date = :date,
                                last_checked_at = :now,
                                updated_at = :now
                        """),
                        {
                            "stock_id": stock_id,
                            "count": stock.get('count'),
                            "reason": stock.get('reason'),
                            "date": stock.get('date'),
                            "now": datetime.now(),
                        },
                    )

                # 2. 更新處置股
                for stock in disposal_stocks:
                    stock_id = stock['stock_id'].lower()
                    await session.execute(
                        text("""
                            INSERT INTO stock_alert_status
                            (stock_id, is_disposal_stock, disposal_announced_date, disposal_start_date,
                             disposal_end_date, disposal_type, disposal_condition, disposal_measure,
                             disposal_content, last_checked_at, updated_at, created_at)
                            VALUES (:stock_id, true, :announced_date, :start_date, :end_date, :type,
                                    :condition, :measure, :content, :now, :now, :now)
                            ON CONFLICT (stock_id) DO UPDATE SET
                                is_disposal_stock = true,
                                disposal_announced_date = :announced_date,
                                disposal_start_date = :start_date,
                                disposal_end_date = :end_date,
                                disposal_type = :type,
                                disposal_condition = :condition,
                                disposal_measure = :measure,
                                disposal_content = :content,
                                last_checked_at = :now,
                                updated_at = :now
                        """),
                        {
                            "stock_id": stock_id,
                            "announced_date": stock.get('announced_date'),
                            "start_date": stock.get('start_date'),
                            "end_date": stock.get('end_date'),
                            "type": stock.get('type'),
                            "condition": stock.get('condition'),
                            "measure": stock.get('measure'),
                            "content": stock.get('content'),
                            "now": datetime.now(),
                        },
                    )

                # 3. 清除已移除的警示（將不在清單中的股票標記為 false）
                attention_ids = [s['stock_id'].lower() for s in attention_stocks]
                disposal_ids = [s['stock_id'].lower() for s in disposal_stocks]

                if attention_ids or disposal_ids:
                    # 清除不在注意股清單中的記錄
                    if attention_ids:
                        await session.execute(
                            text("""
                                UPDATE stock_alert_status
                                SET is_attention_stock = false,
                                    attention_count = NULL,
                                    attention_reason = NULL,
                                    attention_date = NULL,
                                    updated_at = :now
                                WHERE is_attention_stock = true
                                  AND stock_id <> ALL(:attention_ids)
                            """),
                            {"attention_ids": attention_ids, "now": datetime.now()},
                        )

                    # 清除不在處置股清單中的記錄
                    if disposal_ids:
                        await session.execute(
                            text("""
                                UPDATE stock_alert_status
                                SET is_disposal_stock = false,
                                    disposal_announced_date = NULL,
                                    disposal_start_date = NULL,
                                    disposal_end_date = NULL,
                                    disposal_type = NULL,
                                    disposal_condition = NULL,
                                    disposal_measure = NULL,
                                    disposal_content = NULL,
                                    updated_at = :now
                                WHERE is_disposal_stock = true
                                  AND stock_id <> ALL(:disposal_ids)
                            """),
                            {"disposal_ids": disposal_ids, "now": datetime.now()},
                        )

    async def bulk_update_system_warnings(
        self,
        warning_stocks: Dict[str, Dict],
    ):
        """
        批次更新系統預警

        Args:
            warning_stocks: {stock_id: {'warning_score': 60, 'warning_reasons': ['異常放量', ...]}, ...}
        """
        async with self._db_semaphore:
            async with self.get_session() as session:
                for stock_id, warning_data in warning_stocks.items():
                    stock_id = stock_id.lower()
                    await session.execute(
                        text("""
                            INSERT INTO stock_alert_status
                            (stock_id, system_warning, warning_score, warning_reasons,
                             last_checked_at, updated_at, created_at)
                            VALUES (:stock_id, true, :score, :reasons, :now, :now, :now)
                            ON CONFLICT (stock_id) DO UPDATE SET
                                system_warning = true,
                                warning_score = :score,
                                warning_reasons = :reasons,
                                last_checked_at = :now,
                                updated_at = :now
                        """),
                        {
                            "stock_id": stock_id,
                            "score": warning_data.get('warning_score', 0),
                            "reasons": warning_data.get('warning_reasons', []),
                            "now": datetime.now(),
                        },
                    )

    async def bulk_load_alert_status(
        self,
        stock_ids: List[str],
    ) -> Dict[str, Dict]:
        """
        批次載入警示狀態（避免 N+1 查詢）

        Args:
            stock_ids: 股票代號清單

        Returns:
            {stock_id: {
                'is_attention_stock': True,
                'attention_count': 3,
                'is_disposal_stock': False,
                ...
            }, ...}
        """
        if not stock_ids:
            return {}

        # 統一使用小寫 ID
        stock_ids = [sid.lower() for sid in stock_ids]

        async with self._db_semaphore:
            async with self.get_session() as session:
                result = await session.execute(
                    text("""
                        SELECT stock_id, is_attention_stock, attention_count, attention_reason,
                               attention_date, is_disposal_stock, disposal_announced_date,
                               disposal_start_date, disposal_end_date, disposal_type,
                               disposal_condition, disposal_measure, disposal_content,
                               system_warning, warning_score, warning_reasons
                        FROM stock_alert_status
                        WHERE stock_id = ANY(:stock_ids)
                          AND (is_attention_stock = true OR is_disposal_stock = true OR system_warning = true)
                    """),
                    {"stock_ids": stock_ids},
                )

                rows = result.fetchall()

                alert_dict = {}
                for row in rows:
                    stock_id = row.stock_id
                    alert_dict[stock_id] = {
                        "is_attention_stock": row.is_attention_stock or False,
                        "attention_count": row.attention_count,
                        "attention_reason": row.attention_reason,
                        "attention_date": row.attention_date.isoformat() if row.attention_date else None,
                        "is_disposal_stock": row.is_disposal_stock or False,
                        "disposal_announced_date": row.disposal_announced_date.isoformat() if row.disposal_announced_date else None,
                        "disposal_start_date": row.disposal_start_date.isoformat() if row.disposal_start_date else None,
                        "disposal_end_date": row.disposal_end_date.isoformat() if row.disposal_end_date else None,
                        "disposal_type": row.disposal_type,
                        "disposal_condition": row.disposal_condition,
                        "disposal_measure": row.disposal_measure,
                        "disposal_content": row.disposal_content,
                        "system_warning": row.system_warning or False,
                        "warning_score": row.warning_score or 0,
                        "warning_reasons": row.warning_reasons or [],
                    }

                return alert_dict

    async def get_tracker(self, stock_id: str) -> Optional[Dict[str, Any]]:
        """取得股票追蹤狀態"""
        stock_id = stock_id.lower()  # 統一使用小寫 ID
        async with self.get_session() as session:
            result = await session.execute(
                text("SELECT * FROM stock_update_tracker WHERE stock_id = :stock_id"),
                {"stock_id": stock_id},
            )
            row = result.fetchone()
            return dict(row._mapping) if row else None

    # ========== 可轉債批次操作 ==========

    async def save_convertible_bonds(self, bonds: list[dict]):
        """批次儲存可轉債基本資訊"""
        if not bonds:
            return
        async with self._db_semaphore:
            async with self.get_session() as session:
                await session.execute(
                    text("""
                        INSERT INTO convertible_bond
                        (bond_id, name, underlying_stock_id, conversion_price,
                         issue_date, maturity_date, put_date, put_price,
                         coupon_rate, issued_amount, outstanding_amount, is_active, updated_at)
                        VALUES (:bond_id, :name, :underlying_stock_id, :conversion_price,
                                :issue_date, :maturity_date, :put_date, :put_price,
                                :coupon_rate, :issued_amount, :outstanding_amount, :is_active, :updated_at)
                        ON CONFLICT (bond_id) DO UPDATE SET
                            name = EXCLUDED.name,
                            conversion_price = EXCLUDED.conversion_price,
                            outstanding_amount = EXCLUDED.outstanding_amount,
                            is_active = EXCLUDED.is_active,
                            updated_at = EXCLUDED.updated_at
                    """),
                    bonds,
                )

    async def save_convertible_bond_daily(self, records: list[dict]):
        """批次儲存可轉債每日交易資料"""
        if not records:
            return
        async with self._db_semaphore:
            async with self.get_session() as session:
                await session.execute(
                    text("""
                        INSERT INTO convertible_bond_daily
                        (bond_id, date, open, high, low, close, volume,
                         underlying_close, conversion_value, premium_rate, arbitrage_spread)
                        VALUES (:bond_id, :date, :open, :high, :low, :close, :volume,
                                :underlying_close, :conversion_value, :premium_rate, :arbitrage_spread)
                        ON CONFLICT (bond_id, date) DO UPDATE SET
                            close = EXCLUDED.close,
                            volume = EXCLUDED.volume,
                            underlying_close = EXCLUDED.underlying_close,
                            conversion_value = EXCLUDED.conversion_value,
                            premium_rate = EXCLUDED.premium_rate,
                            arbitrage_spread = EXCLUDED.arbitrage_spread
                    """),
                    records,
                )

    async def save_convertible_bond_signals(self, signals: list[dict]):
        """批次儲存可轉債套利訊號"""
        if not signals:
            return
        async with self._db_semaphore:
            async with self.get_session() as session:
                await session.execute(
                    text("""
                        INSERT INTO convertible_bond_signals
                        (bond_id, date, raw_score, normalized_score, signal_count,
                         risk_level, signals_json, underlying_stock_id,
                         premium_rate, conversion_value, updated_at)
                        VALUES (:bond_id, :date, :raw_score, :normalized_score, :signal_count,
                                :risk_level, :signals_json, :underlying_stock_id,
                                :premium_rate, :conversion_value, :updated_at)
                        ON CONFLICT (bond_id) DO UPDATE SET
                            date = EXCLUDED.date,
                            raw_score = EXCLUDED.raw_score,
                            normalized_score = EXCLUDED.normalized_score,
                            signal_count = EXCLUDED.signal_count,
                            risk_level = EXCLUDED.risk_level,
                            signals_json = EXCLUDED.signals_json,
                            premium_rate = EXCLUDED.premium_rate,
                            conversion_value = EXCLUDED.conversion_value,
                            updated_at = EXCLUDED.updated_at
                    """),
                    signals,
                )

    async def save_stock_list_cache(self, records: list[dict]):
        """批次儲存股票列表快取（UPSERT）"""
        if not records:
            return
        async with self._db_semaphore:
            async with self.get_session() as session:
                # 分批處理避免單次 SQL 過大
                batch_size = 500
                for i in range(0, len(records), batch_size):
                    batch = records[i : i + batch_size]
                    await session.execute(
                        text("""
                            INSERT INTO stock_list_cache
                            (stock_id, name, close_price, last_date,
                             chip_raw_score, chip_normalized, chip_signals_json,
                             expected_return, win_rate,
                             tech_raw_score, tech_normalized, tech_signals_json,
                             fund_raw_score, fund_normalized, fund_signals_json,
                             signal_count, risk_level,
                             alert_status_json, cb_arbitrage_score, cb_signals_json, updated_at)
                            VALUES
                            (:stock_id, :name, :close_price, :last_date,
                             :chip_raw_score, :chip_normalized, :chip_signals_json,
                             :expected_return, :win_rate,
                             :tech_raw_score, :tech_normalized, :tech_signals_json,
                             :fund_raw_score, :fund_normalized, :fund_signals_json,
                             :signal_count, :risk_level,
                             :alert_status_json, :cb_arbitrage_score, :cb_signals_json, :updated_at)
                            ON CONFLICT (stock_id) DO UPDATE SET
                                name = EXCLUDED.name,
                                close_price = EXCLUDED.close_price,
                                last_date = EXCLUDED.last_date,
                                chip_raw_score = EXCLUDED.chip_raw_score,
                                chip_normalized = EXCLUDED.chip_normalized,
                                chip_signals_json = EXCLUDED.chip_signals_json,
                                expected_return = EXCLUDED.expected_return,
                                win_rate = EXCLUDED.win_rate,
                                tech_raw_score = EXCLUDED.tech_raw_score,
                                tech_normalized = EXCLUDED.tech_normalized,
                                tech_signals_json = EXCLUDED.tech_signals_json,
                                fund_raw_score = EXCLUDED.fund_raw_score,
                                fund_normalized = EXCLUDED.fund_normalized,
                                fund_signals_json = EXCLUDED.fund_signals_json,
                                signal_count = EXCLUDED.signal_count,
                                risk_level = EXCLUDED.risk_level,
                                alert_status_json = EXCLUDED.alert_status_json,
                                cb_arbitrage_score = EXCLUDED.cb_arbitrage_score,
                                cb_signals_json = EXCLUDED.cb_signals_json,
                                updated_at = EXCLUDED.updated_at
                        """),
                        batch,
                    )

    async def bulk_load_convertible_bond_daily(
        self, bond_ids: list[str], days: int = 250
    ) -> dict[str, pd.DataFrame]:
        """批次載入可轉債每日交易資料"""
        if not bond_ids:
            return {}
        async with self.get_session() as session:
            result = await session.execute(
                text("""
                    SELECT bond_id, date, open, high, low, close, volume,
                           underlying_close, conversion_value, premium_rate, arbitrage_spread
                    FROM convertible_bond_daily
                    WHERE bond_id = ANY(:bond_ids)
                      AND date >= NOW() - INTERVAL ':days days'
                    ORDER BY bond_id, date
                """.replace(":days days", f"{days} days")),
                {"bond_ids": bond_ids},
            )
            rows = result.fetchall()

        data_map = {}
        for row in rows:
            row_dict = dict(row._mapping)
            bid = row_dict["bond_id"]
            if bid not in data_map:
                data_map[bid] = []
            data_map[bid].append(row_dict)

        return {bid: pd.DataFrame(records) for bid, records in data_map.items()}

    async def bulk_check_cb_needs_update(self, bond_ids: list[str]) -> dict[str, bool]:
        """批次檢查哪些可轉債需要更新（DB 最新日期 < 今天）"""
        if not bond_ids:
            return {}
        async with self.get_session() as session:
            result = await session.execute(
                text("""
                    SELECT bond_id, MAX(date) as latest_date
                    FROM convertible_bond_daily
                    WHERE bond_id = ANY(:bond_ids)
                    GROUP BY bond_id
                """),
                {"bond_ids": bond_ids},
            )
            rows = result.fetchall()

        latest_dates = {row._mapping["bond_id"]: row._mapping["latest_date"] for row in rows}
        today = date.today()
        needs_update = {}
        for bid in bond_ids:
            if bid not in latest_dates:
                needs_update[bid] = True
            else:
                db_date = latest_dates[bid]
                if hasattr(db_date, "date"):
                    db_date = db_date.date()
                needs_update[bid] = db_date < today
        return needs_update

    async def get_all_convertible_bonds(self, active_only: bool = True) -> list[dict]:
        """取得所有可轉債基本資訊"""
        async with self.get_session() as session:
            where = "WHERE is_active = true" if active_only else ""
            result = await session.execute(
                text(f"""
                    SELECT bond_id, name, underlying_stock_id, conversion_price,
                           issue_date, maturity_date, put_date, put_price,
                           coupon_rate, issued_amount, outstanding_amount, is_active
                    FROM convertible_bond {where}
                    ORDER BY bond_id
                """)
            )
            return [dict(row._mapping) for row in result.fetchall()]

    async def get_cb_signals_by_underlying(self) -> dict[str, dict]:
        """取得 underlying_stock_id → {score, signals_json} 映射（取最高分的 CB）"""
        async with self.get_session() as session:
            result = await session.execute(
                text("""
                    SELECT DISTINCT ON (underlying_stock_id)
                        underlying_stock_id, normalized_score as cb_arbitrage_score, signals_json
                    FROM convertible_bond_signals
                    ORDER BY underlying_stock_id, normalized_score DESC
                """)
            )
            return {
                row._mapping["underlying_stock_id"]: {
                    "score": row._mapping["cb_arbitrage_score"],
                    "signals_json": row._mapping["signals_json"],
                }
                for row in result.fetchall()
            }

    async def auto_cleanup_old_data(
        self, retention_days: int = 250, vacuum: bool = True
    ) -> Dict[str, Any]:
        """
        自動清理超過保留期限的歷史資料（爬蟲完成後調用）

        Args:
            retention_days: 保留天數（0 表示不清理）
            vacuum: 是否執行 VACUUM 釋放空間

        Returns:
            清理結果統計
        """
        import os
        from datetime import datetime, timedelta

        # 環境檢查：只在 production 環境執行
        environment = os.getenv("ENVIRONMENT", "development")
        if environment != "production":
            logger.info(
                f"⏭️  自動清理跳過（環境：{environment}，只在 production 執行）"
            )
            return {
                "status": "skipped",
                "reason": f"environment={environment} (只在 production 執行)",
                "retention_days": retention_days,
            }

        if retention_days <= 0:
            logger.info("⏭️  自動清理跳過（retention_days=0，永久保留）")
            return {
                "status": "skipped",
                "reason": "retention_days=0 (永久保留)",
                "retention_days": retention_days,
            }

        # 計算截止日期
        cutoff_date = datetime.now() - timedelta(days=retention_days)
        logger.info(
            f"🗑️  開始自動清理舊資料（保留 {retention_days} 天，截止日期：{cutoff_date.date()}）"
        )

        # 需要清理的表
        tables_to_cleanup = [
            "stock_daily",
            "institutional_investors",
            "major_investors",
            "margin_trading",
            "concentration_data",
            "stock_technical_indicators",
        ]

        result = {
            "status": "completed",
            "environment": environment,
            "retention_days": retention_days,
            "cutoff_date": cutoff_date.isoformat(),
            "deleted_counts": {},
            "vacuum": "pending",
        }

        async with self.async_session_factory() as session:
            total_deleted = 0

            for table in tables_to_cleanup:
                try:
                    # 刪除舊資料
                    delete_result = await session.execute(
                        text(
                            f"""
                        DELETE FROM {table}
                        WHERE date < :cutoff_date
                    """
                        ),
                        {"cutoff_date": cutoff_date},
                    )

                    deleted_count = delete_result.rowcount
                    result["deleted_counts"][table] = deleted_count
                    total_deleted += deleted_count

                    if deleted_count > 0:
                        logger.info(f"  ✅ {table}: 刪除 {deleted_count:,} 行")
                    else:
                        logger.debug(f"  ⏭️  {table}: 無需刪除")

                except Exception as e:
                    logger.error(f"  ❌ {table}: 刪除失敗 - {e}")
                    result["deleted_counts"][table] = f"error: {str(e)}"

            # 提交刪除
            await session.commit()
            logger.info(f"📊 總計刪除 {total_deleted:,} 行資料")

            # VACUUM 釋放磁碟空間
            if vacuum and total_deleted > 0:
                logger.info("🧹 執行 VACUUM 釋放磁碟空間...")
                try:
                    # 關閉當前 session
                    await session.close()

                    # 使用新連線執行 VACUUM（需要 AUTOCOMMIT）
                    async with self.async_engine.connect() as conn:
                        # 設定為 AUTOCOMMIT 模式（VACUUM 不能在交易中執行）
                        await conn.execution_options(isolation_level="AUTOCOMMIT")
                        for table in tables_to_cleanup:
                            await conn.execute(text(f"VACUUM {table}"))
                            logger.debug(f"  ✅ VACUUM {table}")

                    result["vacuum"] = "completed"
                    logger.info("✅ VACUUM 完成")

                except Exception as e:
                    logger.error(f"❌ VACUUM 失敗: {e}")
                    result["vacuum"] = f"error: {str(e)}"
            else:
                result["vacuum"] = "skipped"

        logger.info(f"✅ 自動清理完成（保留 {retention_days} 天資料）")
        return result


def safe_float(value):
    """安全轉換為浮點數"""
    if value is None or value == "" or pd.isna(value):
        return None
    try:
        return float(value)
    except (ValueError, TypeError):
        return None
