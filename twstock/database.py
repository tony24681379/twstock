import asyncio
import hashlib
import os
from contextlib import asynccontextmanager
from datetime import date, datetime
from typing import Any, Dict, List, Optional

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
    foreign = Column(Float, comment="外資買賣超")
    investment_trust = Column(Float, comment="投信買賣超")
    dealer = Column(Float, comment="自營商買賣超")
    sum_holding_rate = Column(Float, comment="三大法人持股比率合計")
    foreign_holding_rate = Column(Float, comment="外資持股比率")
    investment_trust_holding_rate = Column(Float, comment="投信持股比率")
    dealer_holding_rate = Column(Float, comment="自營商持股比率")

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
        """初始化資料庫表格"""
        async with self.async_engine.begin() as conn:
            await conn.run_sync(Base.metadata.create_all)

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
                        ON CONFLICT (stock_id) DO UPDATE SET
                            capital = EXCLUDED.capital,
                            outstanding_shares = EXCLUDED.outstanding_shares,
                            per = EXCLUDED.per,
                            cash_dividend = EXCLUDED.cash_dividend,
                            stock_dividend = EXCLUDED.stock_dividend,
                            updated_at = EXCLUDED.updated_at
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
                            ON CONFLICT (stock_id, year, quarter) DO UPDATE SET
                                eps = EXCLUDED.eps
                        """
                        ),
                        eps_records,
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
                    if "foreign" in row:
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
                                "foreign": safe_float(row.get("foreign")),
                                "investment_trust": safe_float(
                                    row.get("investment_trust")
                                ),
                                "dealer": safe_float(row.get("dealer")),
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
                            ON CONFLICT (stock_id, date) DO UPDATE SET
                                volume = EXCLUDED.volume,
                                open = EXCLUDED.open,
                                high = EXCLUDED.high,
                                low = EXCLUDED.low,
                                close = EXCLUDED.close
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
                            (id, stock_id, date, "foreign", investment_trust, dealer, sum_holding_rate,
                             foreign_holding_rate, investment_trust_holding_rate, dealer_holding_rate)
                            VALUES (:id, :stock_id, :date, :foreign, :investment_trust, :dealer, :sum_holding_rate,
                                    :foreign_holding_rate, :investment_trust_holding_rate, :dealer_holding_rate)
                            ON CONFLICT (stock_id, date) DO UPDATE SET
                                "foreign" = EXCLUDED."foreign",
                                investment_trust = EXCLUDED.investment_trust,
                                dealer = EXCLUDED.dealer,
                                sum_holding_rate = EXCLUDED.sum_holding_rate,
                                foreign_holding_rate = EXCLUDED.foreign_holding_rate,
                                investment_trust_holding_rate = EXCLUDED.investment_trust_holding_rate,
                                dealer_holding_rate = EXCLUDED.dealer_holding_rate
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
                            ON CONFLICT (stock_id, date) DO UPDATE SET
                                major_investors = EXCLUDED.major_investors,
                                agent_diff = EXCLUDED.agent_diff,
                                skp5 = EXCLUDED.skp5,
                                skp20 = EXCLUDED.skp20
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
                            ON CONFLICT (stock_id, date) DO UPDATE SET
                                lending_balance = EXCLUDED.lending_balance,
                                borrowing_balance = EXCLUDED.borrowing_balance,
                                balance_limit = EXCLUDED.balance_limit
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
                            "rate_of_ing_holding": safe_float(row.get("rateOfINGHolding")),
                            "rate_of_dealer_holding": safe_float(
                                row.get("rateOfDealerHolding")
                            ),
                        }
                    )

                # 批次 INSERT（使用 ON CONFLICT DO UPDATE）
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
                            ON CONFLICT (stock_id, date)
                            DO UPDATE SET
                                more_than_400 = EXCLUDED.more_than_400,
                                more_than_1000 = EXCLUDED.more_than_1000,
                                less_than_20 = EXCLUDED.less_than_20,
                                "close" = EXCLUDED."close",
                                director_ratio = EXCLUDED.director_ratio,
                                rate_of_foreign_holding = EXCLUDED.rate_of_foreign_holding,
                                rate_of_ing_holding = EXCLUDED.rate_of_ing_holding,
                                rate_of_dealer_holding = EXCLUDED.rate_of_dealer_holding
                        """
                        ),
                        concentration_records,
                    )
                    await session.commit()

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
                    ON CONFLICT (stock_id) DO UPDATE SET
                        name = EXCLUDED.name,
                        type = EXCLUDED.type,
                        country = EXCLUDED.country,
                        market = EXCLUDED.market,
                        is_active = EXCLUDED.is_active,
                        updated_at = EXCLUDED.updated_at
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

    async def bulk_load_daily_data(
        self, stock_ids: List[str], days: int = 490
    ) -> Dict[str, pd.DataFrame]:
        """批次載入多支股票的每日資料（使用單一 SQL 查詢）

        Args:
            stock_ids: 股票代碼列表
            days: 載入最近幾天的資料

        Returns:
            Dict[stock_id, DataFrame]: 每支股票的 DataFrame
        """
        stock_ids = [sid.lower() for sid in stock_ids]

        async with self.get_session() as session:
            # 使用單一 SQL 查詢取得所有股票的完整資料（包含 JOIN）
            # 注意：INTERVAL 不能用參數綁定，直接用 f-string 構建
            result = await session.execute(
                text(
                    f"""
                    SELECT
                        d.stock_id,
                        d.date,
                        d.volume,
                        d.open,
                        d.high,
                        d.low,
                        d.close,
                        i."foreign",
                        i.investment_trust,
                        i.dealer,
                        i.sum_holding_rate,
                        i.foreign_holding_rate,
                        i.investment_trust_holding_rate,
                        i.dealer_holding_rate,
                        m.major_investors,
                        m.agent_diff,
                        m.skp5,
                        m.skp20,
                        mt.lending_balance,
                        mt.borrowing_balance,
                        mt.balance_limit
                    FROM stock_daily d
                    LEFT JOIN institutional_investors i ON d.stock_id = i.stock_id AND d.date = i.date
                    LEFT JOIN major_investors m ON d.stock_id = m.stock_id AND d.date = m.date
                    LEFT JOIN margin_trading mt ON d.stock_id = mt.stock_id AND d.date = mt.date
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

                stock_data_dict[stock_id].append(
                    {
                        "date": row.date,
                        "volume": row.volume,
                        "open": row.open,
                        "high": row.high,
                        "low": row.low,
                        "close": row.close,
                        "foreign": row.foreign or 0,
                        "investment_trust": row.investment_trust or 0,
                        "dealer": row.dealer or 0,
                        "sum_holding_rate": row.sum_holding_rate or 0,
                        "foreign_holding_rate": row.foreign_holding_rate or 0,
                        "investment_trust_holding_rate": row.investment_trust_holding_rate
                        or 0,
                        "dealer_holding_rate": row.dealer_holding_rate or 0,
                        "major_investors": row.major_investors or 0,
                        "agent_diff": row.agent_diff or 0,
                        "skp5": row.skp5 or 0,
                        "skp20": row.skp20 or 0,
                        "lending_balance": row.lending_balance or 0,
                        "borrowing_balance": row.borrowing_balance or 0,
                        "balance_limit": row.balance_limit or 0,
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


def safe_float(value):
    """安全轉換為浮點數"""
    if value is None or value == "" or pd.isna(value):
        return None
    try:
        return float(value)
    except (ValueError, TypeError):
        return None
