import asyncio
import os

from .database import DatabaseManager
from .header_manager import HeaderManager
from .wantgoo import WantgooFetcher

_global_fetcher = None
_header_manager = None
_db_manager = None
_init_lock = asyncio.Lock()
_initialized = False


async def get_global_fetcher() -> WantgooFetcher:
    """獲取全域共享的 fetcher 實例（不含 DB）"""
    global _global_fetcher, _header_manager, _initialized

    if _global_fetcher is None:
        async with _init_lock:
            if _global_fetcher is None:
                print("Creating global fetcher instance...")
                _header_manager = HeaderManager()
                _global_fetcher = WantgooFetcher(header_manager=_header_manager)

    if not _initialized:
        async with _init_lock:
            if not _initialized:
                print("Initializing global fetcher...")
                await _header_manager.get_headers()
                _initialized = True
                print("Global fetcher initialized successfully")

    return _global_fetcher


async def get_db_manager() -> DatabaseManager:
    """獲取資料庫管理器（獨立於 fetcher）"""
    global _db_manager, _header_manager

    if _db_manager is None:
        async with _init_lock:
            if _db_manager is None:
                db_url = os.environ.get(
                    "DATABASE_URL",
                    "postgresql+asyncpg://twstock_user:twstock_password123@localhost:5432/twstock",
                )
                _db_manager = DatabaseManager(database_url=db_url)
                await _db_manager.init_database()
                print(f"Database initialized: {db_url}")

                # 從 HeaderManager 取得 api_latest_date
                if _header_manager and _header_manager.api_latest_date:
                    _db_manager.api_latest_date = _header_manager.api_latest_date

    return _db_manager


async def close_global_resources():
    """關閉全域資源（資料庫連線等）"""
    global _db_manager

    if _db_manager:
        await _db_manager.close()
        _db_manager = None


def reset_global_fetcher():
    """重置全域 fetcher（測試用）"""
    global _global_fetcher, _header_manager, _initialized
    _global_fetcher = None
    _header_manager = None
    _initialized = False
