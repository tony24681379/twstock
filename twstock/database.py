import os
import hashlib
from datetime import datetime, timedelta
from typing import Optional, List, Dict, Any
import asyncio
from contextlib import asynccontextmanager

import pandas as pd
from sqlalchemy import create_engine, Column, String, Float, Integer, DateTime, Boolean, BigInteger, Index, text
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker, Session
from sqlalchemy.ext.asyncio import AsyncSession, create_async_engine, async_sessionmaker
from sqlalchemy.pool import NullPool

try:
    from .db_utils import (
        to_python_datetime, safe_float, safe_int,
        escape_sql_identifier, prepare_dataframe_for_db
    )
except ImportError:
    from db_utils import (
        to_python_datetime, safe_float, safe_int,
        escape_sql_identifier, prepare_dataframe_for_db
    )

Base = declarative_base()


class StockInfo(Base):
    """股票基本資訊表"""
    __tablename__ = 'stock_info'
    
    id = Column(String(10), primary_key=True)
    capital = Column(Float)
    outstanding_shares = Column(Float)
    per = Column(Float)
    cash_dividend = Column(Float)
    stock_dividend = Column(Float)
    updated_at = Column(DateTime, default=datetime.now, onupdate=datetime.now)
    
    __table_args__ = (
        Index('idx_stock_info_updated', 'updated_at'),
    )


class StockEPS(Base):
    """每季 EPS 資料表"""
    __tablename__ = 'stock_eps'
    
    id = Column(Integer, primary_key=True, autoincrement=True)
    stock_id = Column(String(10), nullable=False)
    year = Column(Integer, nullable=False)
    quarter = Column(Integer, nullable=False)
    eps = Column(Float)
    created_at = Column(DateTime, default=datetime.now)
    
    __table_args__ = (
        Index('idx_stock_eps_stock_year_quarter', 'stock_id', 'year', 'quarter', unique=True),
    )


class StockDaily(Base):
    """每日交易資料表"""
    __tablename__ = 'stock_daily'
    
    id = Column(BigInteger, primary_key=True, autoincrement=True)
    stock_id = Column(String(10), nullable=False)
    date = Column(DateTime, nullable=False)
    volume = Column(Float)
    open = Column(Float)
    high = Column(Float)
    low = Column(Float)
    close = Column(Float)
    created_at = Column(DateTime, default=datetime.now)
    
    __table_args__ = (
        Index('idx_stock_daily_stock_date', 'stock_id', 'date', unique=True),
        Index('idx_stock_daily_date', 'date'),
    )


class InstitutionalInvestors(Base):
    """三大法人買賣超資料表"""
    __tablename__ = 'institutional_investors'
    
    id = Column(BigInteger, primary_key=True, autoincrement=True)
    stock_id = Column(String(10), nullable=False)
    date = Column(DateTime, nullable=False)
    foreign = Column('foreign', Float)  # 外資買賣超比例（SQLite 保留字）
    investment_trust = Column(Float)  # 投信買賣超比例
    dealer = Column(Float)  # 自營商買賣超比例
    sum_holding_rate = Column(Float)  # 三大法人持股比例
    foreign_holding_rate = Column(Float)
    investment_trust_holding_rate = Column(Float)
    dealer_holding_rate = Column(Float)
    created_at = Column(DateTime, default=datetime.now)
    
    __table_args__ = (
        Index('idx_institutional_stock_date', 'stock_id', 'date', unique=True),
        Index('idx_institutional_date', 'date'),
    )


class MajorInvestors(Base):
    """主力投資人資料表"""
    __tablename__ = 'major_investors'
    
    id = Column(BigInteger, primary_key=True, autoincrement=True)
    stock_id = Column(String(10), nullable=False)
    date = Column(DateTime, nullable=False)
    major_investors = Column(Float)  # 主力買賣力道
    agent_diff = Column(Float)  # 券商分點差
    skp5 = Column(Float)  # 5日累積
    skp20 = Column(Float)  # 20日累積
    created_at = Column(DateTime, default=datetime.now)
    
    __table_args__ = (
        Index('idx_major_stock_date', 'stock_id', 'date', unique=True),
        Index('idx_major_date', 'date'),
    )


class MarginTrading(Base):
    """融資融券資料表"""
    __tablename__ = 'margin_trading'
    
    id = Column(BigInteger, primary_key=True, autoincrement=True)
    stock_id = Column(String(10), nullable=False)
    date = Column(DateTime, nullable=False)
    lending_balance = Column(Float)  # 融資餘額
    borrowing_balance = Column(Float)  # 融券餘額
    balance_limit = Column(Float)  # 融資限額
    created_at = Column(DateTime, default=datetime.now)
    
    __table_args__ = (
        Index('idx_margin_stock_date', 'stock_id', 'date', unique=True),
        Index('idx_margin_date', 'date'),
    )


class StockList(Base):
    """股票清單資料表"""
    __tablename__ = 'stock_list'
    
    id = Column(String(10), primary_key=True)
    name = Column(String(50))
    type = Column(String(20))  # Stock, ETF, Index
    country = Column(String(10))
    market = Column(String(20))  # TWSE, OTC
    is_active = Column(Boolean, default=True)
    updated_at = Column(DateTime, default=datetime.now, onupdate=datetime.now)
    
    __table_args__ = (
        Index('idx_stock_list_type', 'type'),
        Index('idx_stock_list_active', 'is_active'),
    )


class StockUpdateTracker(Base):
    """股票資料更新追蹤表 (取代 loaded.csv 和檔案系統)"""
    __tablename__ = 'stock_update_tracker'
    
    stock_id = Column(String(10), primary_key=True)
    
    # 基本資訊追蹤
    info_loaded = Column(Boolean, default=False)
    info_updated_at = Column(DateTime)
    
    # 每日資料追蹤
    daily_loaded = Column(Boolean, default=False)
    daily_first_date = Column(DateTime)  # 最早的資料日期
    daily_last_date = Column(DateTime)   # 最新的資料日期
    daily_count = Column(Integer, default=0)  # 資料筆數
    daily_updated_at = Column(DateTime)  # 上次更新時間
    
    # 分析資料追蹤
    analysis_completed = Column(Boolean, default=False)
    analysis_updated_at = Column(DateTime)
    
    # 整體狀態
    created_at = Column(DateTime, default=datetime.now)
    last_checked_at = Column(DateTime, default=datetime.now, onupdate=datetime.now)
    
    __table_args__ = (
        Index('idx_tracker_loaded', 'info_loaded', 'daily_loaded'),
        Index('idx_tracker_updated', 'daily_updated_at'),
    )


class DatabaseManager:
    """資料庫管理器"""
    
    def __init__(self, database_url: Optional[str] = None):
        """
        初始化資料庫連線
        
        Args:
            database_url: 資料庫連線字串，預設使用 SQLite
                         例如: 'postgresql+asyncpg://user:pass@localhost/dbname'
                              'mysql+aiomysql://user:pass@localhost/dbname'
                              'sqlite+aiosqlite:///./twstock.db'
        """
        if database_url is None:
            # 預設使用 SQLite
            database_url = os.environ.get(
                'DATABASE_URL',
                'sqlite+aiosqlite:///./twstock.db'
            )
        
        # 儲存資料庫類型供 SQL 語法判斷使用
        self.database_url = database_url
        self.is_postgresql = 'postgresql' in database_url
        self.is_sqlite = database_url.startswith('sqlite')
        
        # 建立信號量以限制資料庫並發存取
        # PostgreSQL 可以處理更多並發
        if self.is_postgresql:
            self._db_semaphore = asyncio.Semaphore(20)  # PostgreSQL 可以處理多個並發寫入
        else:
            self._db_semaphore = asyncio.Semaphore(1)  # SQLite 只允許一個寫入者
        
        # 支援同步和非同步操作
        connect_args = {}
        if database_url.startswith('sqlite'):
            connect_args = {
                "check_same_thread": False,
                "timeout": 30  # 增加 SQLite timeout 到 30 秒
            }
        
        self.async_engine = create_async_engine(
            database_url,
            echo=False,
            poolclass=NullPool,
            future=True,
            pool_pre_ping=True,
            pool_recycle=300,
            connect_args=connect_args
        )
        self.async_session_factory = async_sessionmaker(
            self.async_engine,
            class_=AsyncSession,
            expire_on_commit=False
        )
        
        # 同步引擎（用於初始化）
        sync_url = database_url.replace('+aiosqlite', '').replace('+asyncpg', '').replace('+aiomysql', '')
        self.sync_engine = create_engine(sync_url, echo=False)
    
    async def init_database(self):
        """初始化資料庫表格"""
        async with self.async_engine.begin() as conn:
            await conn.run_sync(Base.metadata.create_all)
            
            # 對 SQLite 啟用 WAL 模式以改善並發效能
            if 'sqlite' in str(self.async_engine.url):
                await conn.execute(text("PRAGMA journal_mode=WAL"))
                await conn.execute(text("PRAGMA synchronous=NORMAL"))
                await conn.execute(text("PRAGMA cache_size=10000"))
                await conn.execute(text("PRAGMA temp_store=MEMORY"))
                print("SQLite WAL 模式已啟用")
    
    @asynccontextmanager
    async def get_session(self):
        """取得資料庫 session，包含重試機制"""
        max_retries = 5
        retry_delay = 0.5
        
        for attempt in range(max_retries):
            try:
                async with self.async_session_factory() as session:
                    try:
                        yield session
                        await session.commit()
                        break  # 成功則跳出重試循環
                    except Exception as e:
                        await session.rollback()
                        if "database is locked" in str(e) and attempt < max_retries - 1:
                            # SQLite 鎖定錯誤，等待後重試
                            await asyncio.sleep(retry_delay * (attempt + 1))
                            continue
                        raise
                    finally:
                        await session.close()
            except Exception as e:
                if "database is locked" in str(e) and attempt < max_retries - 1:
                    await asyncio.sleep(retry_delay * (attempt + 1))
                    continue
                raise
    
    async def save_stock_info(self, stock_id: str, info_data: pd.Series):
        """儲存股票基本資訊"""
        async with self._db_semaphore:  # 使用信號量限制並發
            async with self.get_session() as session:
                # 檢查是否已存在
                result = await session.execute(
                    text("SELECT id FROM stock_info WHERE id = :stock_id"),
                    {"stock_id": stock_id}
                )
                exists = result.first() is not None
                
                stock_info = {
                    'id': stock_id,
                    'capital': float(info_data.get('capital', 0)) if pd.notna(info_data.get('capital')) else None,
                    'outstanding_shares': float(info_data.get('outstanding_shares', 0)) if pd.notna(info_data.get('outstanding_shares')) else None,
                    'per': float(info_data.get('PER', 0)) if pd.notna(info_data.get('PER')) else None,
                    'cash_dividend': float(info_data.get('cash_dividend', 0)) if pd.notna(info_data.get('cash_dividend')) else None,
                    'stock_dividend': float(info_data.get('stock_dividend', 0)) if pd.notna(info_data.get('stock_dividend')) else None,
                }
                
                if exists:
                    await session.execute(
                        text("""
                            UPDATE stock_info 
                            SET capital = :capital, outstanding_shares = :outstanding_shares,
                                per = :per, cash_dividend = :cash_dividend, 
                                stock_dividend = :stock_dividend, updated_at = :updated_at
                            WHERE id = :id
                        """),
                        {**stock_info, 'updated_at': datetime.now()}
                    )
                else:
                    await session.execute(
                        text("""
                            INSERT INTO stock_info (id, capital, outstanding_shares, per, cash_dividend, stock_dividend)
                            VALUES (:id, :capital, :outstanding_shares, :per, :cash_dividend, :stock_dividend)
                        """),
                        stock_info
                    )
                
                # 儲存 EPS 資料
                for key in info_data.index:
                    if '/' in str(key) and 'Q' in str(key):
                        try:
                            year, quarter = key.strip().split('/')
                            quarter = int(quarter.replace('Q', ''))
                            year = int(year)
                            eps_value = float(info_data[key]) if pd.notna(info_data[key]) else None
                            
                            if eps_value is not None:
                                # 根據資料庫類型使用不同的 UPSERT 語法
                                if self.is_postgresql:
                                    await session.execute(
                                        text("""
                                            INSERT INTO stock_eps (stock_id, year, quarter, eps)
                                            VALUES (:stock_id, :year, :quarter, :eps)
                                            ON CONFLICT (stock_id, year, quarter) DO UPDATE SET
                                                eps = EXCLUDED.eps
                                        """),
                                        {"stock_id": stock_id, "year": year, "quarter": quarter, "eps": eps_value}
                                    )
                                else:
                                    # SQLite 使用 INSERT OR REPLACE
                                    await session.execute(
                                        text("""
                                            INSERT OR REPLACE INTO stock_eps (stock_id, year, quarter, eps)
                                            VALUES (:stock_id, :year, :quarter, :eps)
                                        """),
                                        {"stock_id": stock_id, "year": year, "quarter": quarter, "eps": eps_value}
                                    )
                        except Exception as e:
                            print(f"Error saving EPS for {key}: {e}")
    
    async def save_daily_data(self, stock_id: str, daily_data: pd.DataFrame):
        """儲存每日交易資料（批次處理）"""
        if daily_data.empty:
            print(f"Daily data for {stock_id} is empty, skipping save")
            return
        
        # 預處理 DataFrame
        daily_data = prepare_dataframe_for_db(daily_data)
        
        if daily_data.empty:
            print(f"ERROR: DataFrame became empty after prepare_dataframe_for_db for {stock_id}")
            return
        
        async with self._db_semaphore:  # 使用信號量限制並發
            async with self.get_session() as session:
                # 準備批次資料
                records = []
                for i, (_, row) in enumerate(daily_data.iterrows()):
                    # 使用工具函數轉換日期
                    date_value = to_python_datetime(row['date'])
                    
                    record = {
                        'stock_id': stock_id,
                        'date': date_value,
                        'volume': safe_float(row.get('volume')),
                        'open': safe_float(row.get('open')),
                        'high': safe_float(row.get('high')),
                        'low': safe_float(row.get('low')),
                        'close': safe_float(row.get('close')),
                    }
                    
                    # 同時儲存三大法人資料
                    if 'foreign' in row:
                        try:
                            inst_record = {
                                'stock_id': stock_id,
                                'date': date_value,
                                'foreign': safe_float(row.get('foreign')),
                                'investment_trust': safe_float(row.get('investment_trust')),
                                'dealer': safe_float(row.get('dealer')),
                                'sum_holding_rate': safe_float(row.get('sum_holding_rate')),
                                'foreign_holding_rate': safe_float(row.get('foreign_holding_rate')),
                                'investment_trust_holding_rate': safe_float(row.get('investment_trust_holding_rate')),
                                'dealer_holding_rate': safe_float(row.get('dealer_holding_rate')),
                            }
                            
                            # 生成手動 ID
                            unique_str = f"{stock_id}_inst_{date_value.strftime('%Y%m%d')}"
                            hash_obj = hashlib.md5(unique_str.encode())
                            inst_id = int(hash_obj.hexdigest()[:15], 16)
                            inst_record['id'] = inst_id
                            
                            # 根據資料庫類型使用不同的 UPSERT 語法
                            if self.is_postgresql:
                                await session.execute(
                                    text("""
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
                                    """),
                                    inst_record
                                )
                            else:
                                # SQLite 使用 INSERT OR IGNORE + UPDATE，避免 auto-increment id 問題
                                await session.execute(
                                text("""
                                    INSERT OR IGNORE INTO institutional_investors 
                                    (id, stock_id, date, "foreign", investment_trust, dealer, sum_holding_rate,
                                     foreign_holding_rate, investment_trust_holding_rate, dealer_holding_rate)
                                    VALUES (:id, :stock_id, :date, :foreign, :investment_trust, :dealer, :sum_holding_rate,
                                            :foreign_holding_rate, :investment_trust_holding_rate, :dealer_holding_rate)
                                """),
                                inst_record
                                )
                                
                                # 然後更新（如果記錄已存在）
                                await session.execute(
                                text("""
                                    UPDATE institutional_investors 
                                    SET "foreign" = :foreign, investment_trust = :investment_trust, 
                                        dealer = :dealer, sum_holding_rate = :sum_holding_rate,
                                        foreign_holding_rate = :foreign_holding_rate, 
                                        investment_trust_holding_rate = :investment_trust_holding_rate, 
                                        dealer_holding_rate = :dealer_holding_rate
                                    WHERE stock_id = :stock_id AND date = :date
                                """),
                                inst_record
                                )
                        except Exception as e:
                            print(f"Error saving institutional data for {record['date']}: {e}")
                    
                    # 儲存主力資料
                    if 'major_investors' in row:
                        try:
                            major_record = {
                                'stock_id': stock_id,
                                'date': date_value,
                                'major_investors': safe_float(row.get('major_investors')),
                                'agent_diff': safe_float(row.get('agent_diff')),
                                'skp5': safe_float(row.get('skp5')),
                                'skp20': safe_float(row.get('skp20')),
                            }
                            
                            # 生成手動 ID
                            unique_str = f"{stock_id}_major_{date_value.strftime('%Y%m%d')}"
                            hash_obj = hashlib.md5(unique_str.encode())
                            major_id = int(hash_obj.hexdigest()[:15], 16)
                            major_record['id'] = major_id
                            
                            # 根據資料庫類型使用不同的 UPSERT 語法
                            if self.is_postgresql:
                                await session.execute(
                                    text("""
                                        INSERT INTO major_investors 
                                        (id, stock_id, date, major_investors, agent_diff, skp5, skp20)
                                        VALUES (:id, :stock_id, :date, :major_investors, :agent_diff, :skp5, :skp20)
                                        ON CONFLICT (stock_id, date) DO UPDATE SET
                                            major_investors = EXCLUDED.major_investors,
                                            agent_diff = EXCLUDED.agent_diff,
                                            skp5 = EXCLUDED.skp5,
                                            skp20 = EXCLUDED.skp20
                                    """),
                                    major_record
                                )
                            else:
                                # SQLite INSERT OR IGNORE + UPDATE 模式
                                await session.execute(
                                text("""
                                    INSERT OR IGNORE INTO major_investors 
                                    (id, stock_id, date, major_investors, agent_diff, skp5, skp20)
                                    VALUES (:id, :stock_id, :date, :major_investors, :agent_diff, :skp5, :skp20)
                                """),
                                major_record
                                )
                                
                                await session.execute(
                                text("""
                                    UPDATE major_investors 
                                    SET major_investors = :major_investors, agent_diff = :agent_diff, 
                                        skp5 = :skp5, skp20 = :skp20
                                    WHERE stock_id = :stock_id AND date = :date
                                """),
                                major_record
                                )
                        except Exception as e:
                            print(f"Error saving major investor data for {record['date']}: {e}")
                
                    # 儲存融資融券資料
                    if 'lending_balance' in row:
                        try:
                            margin_record = {
                                'stock_id': stock_id,
                                'date': date_value,
                                'lending_balance': safe_float(row.get('lending_balance')),
                                'borrowing_balance': safe_float(row.get('borrowing_balance')),
                                'balance_limit': safe_float(row.get('balance_limit')),
                            }
                            
                            # 生成手動 ID
                            unique_str = f"{stock_id}_margin_{date_value.strftime('%Y%m%d')}"
                            hash_obj = hashlib.md5(unique_str.encode())
                            margin_id = int(hash_obj.hexdigest()[:15], 16)
                            margin_record['id'] = margin_id
                            
                            # 根據資料庫類型使用不同的 UPSERT 語法
                            if self.is_postgresql:
                                await session.execute(
                                    text("""
                                        INSERT INTO margin_trading 
                                        (id, stock_id, date, lending_balance, borrowing_balance, balance_limit)
                                        VALUES (:id, :stock_id, :date, :lending_balance, :borrowing_balance, :balance_limit)
                                        ON CONFLICT (stock_id, date) DO UPDATE SET
                                            lending_balance = EXCLUDED.lending_balance,
                                            borrowing_balance = EXCLUDED.borrowing_balance,
                                            balance_limit = EXCLUDED.balance_limit
                                    """),
                                    margin_record
                                )
                            else:
                                # SQLite INSERT OR IGNORE + UPDATE 模式
                                await session.execute(
                                text("""
                                    INSERT OR IGNORE INTO margin_trading 
                                    (id, stock_id, date, lending_balance, borrowing_balance, balance_limit)
                                    VALUES (:id, :stock_id, :date, :lending_balance, :borrowing_balance, :balance_limit)
                                """),
                                margin_record
                                )
                                
                                await session.execute(
                                    text("""
                                        UPDATE margin_trading 
                                        SET lending_balance = :lending_balance, borrowing_balance = :borrowing_balance, 
                                            balance_limit = :balance_limit
                                        WHERE stock_id = :stock_id AND date = :date
                                    """),
                                    margin_record
                                )
                        except Exception as e:
                            print(f"Error saving margin trading data for {record['date']}: {e}")
                
                # 儲存每日價格資料 - 使用 INSERT OR REPLACE
                try:
                    # 使用更可靠的 ID 生成方式：結合日期和股票代碼的 hash
                    # 創建一個唯一的字串組合
                    unique_str = f"{stock_id}_{date_value.strftime('%Y%m%d')}"
                    # 使用 MD5 hash 並取前 15 位數字作為 ID
                    hash_obj = hashlib.md5(unique_str.encode())
                    simple_id = int(hash_obj.hexdigest()[:15], 16)
                    
                    record_with_id = record.copy()
                    record_with_id['id'] = simple_id
                    
                    # 根據資料庫類型使用不同的 UPSERT 語法
                    if self.is_postgresql:
                        await session.execute(
                            text("""
                                INSERT INTO stock_daily 
                                (id, stock_id, date, volume, open, high, low, close)
                                VALUES (:id, :stock_id, :date, :volume, :open, :high, :low, :close)
                                ON CONFLICT (stock_id, date) DO UPDATE SET
                                    volume = EXCLUDED.volume,
                                    open = EXCLUDED.open,
                                    high = EXCLUDED.high,
                                    low = EXCLUDED.low,
                                    close = EXCLUDED.close
                            """),
                            record_with_id
                        )
                    else:
                        # SQLite 使用 INSERT OR REPLACE
                        await session.execute(
                            text("""
                                INSERT OR REPLACE INTO stock_daily 
                                (id, stock_id, date, volume, open, high, low, close)
                                VALUES (:id, :stock_id, :date, :volume, :open, :high, :low, :close)
                            """),
                            record_with_id
                        )
                except Exception as e:
                    print(f"Error saving daily record: {e}")
                    raise
    
    async def save_stock_list(self, stocks: List[Dict[str, Any]]):
        """儲存股票清單"""
        async with self.get_session() as session:
            for stock in stocks:
                # 根據資料庫類型使用不同的 UPSERT 語法
                if self.is_postgresql:
                    await session.execute(
                        text("""
                            INSERT INTO stock_list 
                            (id, name, type, country, market, is_active, updated_at)
                            VALUES (:id, :name, :type, :country, :market, :is_active, :updated_at)
                            ON CONFLICT (id) DO UPDATE SET
                                name = EXCLUDED.name,
                                type = EXCLUDED.type,
                                country = EXCLUDED.country,
                                market = EXCLUDED.market,
                                is_active = EXCLUDED.is_active,
                                updated_at = EXCLUDED.updated_at
                        """),
                        {
                            'id': stock.get('id'),
                            'name': stock.get('name'),
                            'type': stock.get('type'),
                            'country': stock.get('country', 'TW'),
                            'market': stock.get('market', 'TWSE'),
                            'is_active': stock.get('is_active', True),
                            'updated_at': datetime.now()
                        }
                    )
                else:
                    # SQLite 使用 INSERT OR REPLACE
                    await session.execute(
                        text("""
                            INSERT OR REPLACE INTO stock_list 
                            (id, name, type, country, market, is_active, updated_at)
                            VALUES (:id, :name, :type, :country, :market, :is_active, :updated_at)
                        """),
                        {
                            'id': stock.get('id'),
                            'name': stock.get('name'),
                            'type': stock.get('type'),
                            'country': stock.get('country', 'TW'),
                            'market': stock.get('market', 'TWSE'),
                            'is_active': stock.get('is_active', True),
                            'updated_at': datetime.now()
                        }
                    )
    
    async def get_latest_data_date(self, stock_id: str) -> Optional[datetime]:
        """取得某支股票最新的資料日期"""
        async with self.get_session() as session:
            result = await session.execute(
                text("SELECT MAX(date) as latest_date FROM stock_daily WHERE stock_id = :stock_id"),
                {"stock_id": stock_id}
            )
            row = result.first()
            return row.latest_date if row and row.latest_date else None
    
    async def get_tracker(self, stock_id: str) -> dict:
        """取得股票的更新追蹤狀態"""
        async with self.get_session() as session:
            result = await session.execute(
                text("""SELECT * FROM stock_update_tracker WHERE stock_id = :stock_id"""),
                {"stock_id": stock_id}
            )
            row = result.first()
            if row:
                return {
                    'stock_id': row.stock_id,
                    'info_loaded': row.info_loaded,
                    'info_updated_at': row.info_updated_at,
                    'daily_loaded': row.daily_loaded,
                    'daily_first_date': row.daily_first_date,
                    'daily_last_date': row.daily_last_date,
                    'daily_count': row.daily_count,
                    'daily_updated_at': row.daily_updated_at,
                    'analysis_completed': row.analysis_completed,
                }
            return None
    
    async def update_tracker(self, stock_id: str, **kwargs):
        """更新股票的追蹤狀態"""
        async with self.get_session() as session:
            # 檢查是否存在
            result = await session.execute(
                text("SELECT stock_id FROM stock_update_tracker WHERE stock_id = :stock_id"),
                {"stock_id": stock_id}
            )
            exists = result.first() is not None
            
            # 準備更新資料，並轉換所有日期欄位
            update_data = {'stock_id': stock_id, 'last_checked_at': datetime.now()}
            
            # 轉換所有日期相關欄位
            for key, value in kwargs.items():
                if 'date' in key.lower() or 'time' in key.lower() or 'at' in key.lower():
                    # 轉換日期欄位
                    update_data[key] = to_python_datetime(value)
                else:
                    update_data[key] = value
            
            if exists:
                # 更新
                set_clause = ", ".join([f"{k} = :{k}" for k in kwargs.keys()])
                set_clause += ", last_checked_at = :last_checked_at"
                await session.execute(
                    text(f"""UPDATE stock_update_tracker SET {set_clause} 
                            WHERE stock_id = :stock_id"""),
                    update_data
                )
            else:
                # 新增
                columns = list(update_data.keys())
                values = [f":{col}" for col in columns]
                await session.execute(
                    text(f"""INSERT INTO stock_update_tracker ({', '.join(columns)})
                            VALUES ({', '.join(values)})"""),
                    update_data
                )
    
    async def get_loaded_stocks(self) -> List[str]:
        """取得所有已載入的股票清單 (取代 loaded.csv)"""
        async with self.get_session() as session:
            result = await session.execute(
                text("""SELECT stock_id FROM stock_update_tracker 
                       WHERE daily_loaded = true OR info_loaded = true
                       ORDER BY stock_id""")
            )
            return [row.stock_id for row in result]
    
    async def check_needs_update(self, stock_id: str, days_threshold: int = 1) -> bool:
        """檢查股票是否需要更新"""
        tracker = await self.get_tracker(stock_id)
        if not tracker or not tracker['daily_loaded']:
            return True  # 沒有資料，需要更新
        
        # 檢查資料庫中最新的資料日期
        latest_db_date = await self.get_latest_data_date(stock_id)
        if latest_db_date is None:
            return True  # 沒有資料，需要更新
        
        # 使用工具函數轉換日期
        latest_datetime = to_python_datetime(latest_db_date)
        if latest_datetime is None:
            return True  # 無法解析日期，需要更新
        
        # 檢查是否是今天或昨天的資料
        today = datetime.now().date()
        latest_date = latest_datetime.date()
        
        # 如果最新資料是今天的，不需要更新
        if latest_date >= today:
            return False
        
        # 如果是週末，檢查是否有上週五的資料
        if today.weekday() >= 5:  # 週末 (週六=5, 週日=6)
            # 計算上週五
            days_back = today.weekday() - 4  # 到週五的天數
            last_friday = today - timedelta(days=days_back)
            if latest_date >= last_friday:
                return False
        
        # 檢查最後更新時間，避免頻繁更新
        if tracker['daily_updated_at']:
            last_update = to_python_datetime(tracker['daily_updated_at'])
            if last_update:
                time_since_update = (datetime.now() - last_update).total_seconds() / 3600  # 小時
                if time_since_update < 1:  # 1小時內已更新過
                    return False
        
        # 其他情況需要更新
        return True
    
    async def close(self):
        """關閉資料庫連線"""
        await self.async_engine.dispose()


# 使用範例
async def example_usage():
    # 初始化資料庫管理器
    db = DatabaseManager()  # 使用預設 SQLite
    # db = DatabaseManager('postgresql+asyncpg://user:password@localhost/twstock')  # PostgreSQL
    
    # 初始化資料庫表格
    await db.init_database()
    
    # 儲存股票資訊範例
    info_data = pd.Series({
        'capital': 1000000,
        'outstanding_shares': 500000,
        'PER': 15.5,
        'cash_dividend': 3.0,
        'stock_dividend': 0.5,
        '2024/1Q': 2.5,
        '2024/2Q': 3.0
    })
    await db.save_stock_info('2330', info_data)
    
    # 儲存每日資料範例
    daily_data = pd.DataFrame({
        'date': [datetime(2024, 1, 1), datetime(2024, 1, 2)],
        'volume': [10000, 12000],
        'open': [100, 101],
        'high': [102, 103],
        'low': [99, 100],
        'close': [101, 102],
        'foreign': [0.5, -0.3],
        'investment_trust': [0.2, 0.1],
        'dealer': [-0.1, 0.2],
        'sum_holding_rate': [40.5, 40.3],
        'foreign_holding_rate': [30.0, 29.8],
        'investment_trust_holding_rate': [5.5, 5.4],
        'dealer_holding_rate': [5.0, 5.1],
        'major_investors': [100, 120],
        'agent_diff': [50, 55],
        'skp5': [200, 210],
        'skp20': [800, 820],
        'lending_balance': [50000, 51000],
        'borrowing_balance': [1000, 1100],
        'balance_limit': [100000, 100000]
    })
    await db.save_daily_data('2330', daily_data)
    
    # 關閉連線
    await db.close()


if __name__ == "__main__":
    asyncio.run(example_usage())