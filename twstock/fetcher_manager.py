# -*- coding: utf-8 -*-
"""
Fetcher 管理器
確保整個應用程式只有一個 fetcher 實例，避免重複初始化
同時管理資料庫連線
"""

import asyncio
import os
from typing import Optional

from .wantgoo import WantgooFetcher
from .database import DatabaseManager

# 全域單例 fetcher 和資料庫管理器
_global_fetcher = None
_db_manager = None
_init_lock = asyncio.Lock()
_initialized = False


async def get_global_fetcher(use_database: bool = True):
    """
    獲取全域共享的 fetcher 實例
    確保只初始化一次
    
    Args:
        use_database: 是否使用資料庫儲存資料，預設為 True
    """
    global _global_fetcher, _db_manager, _initialized
    
    if _global_fetcher is None:
        async with _init_lock:
            if _global_fetcher is None:  # 雙重檢查
                print("Creating global fetcher instance...")
                
                # 如果啟用資料庫，初始化資料庫管理器
                if use_database and _db_manager is None:
                    # 從環境變數讀取資料庫連線字串，預設使用 SQLite
                    db_url = os.environ.get('DATABASE_URL', 'sqlite+aiosqlite:///./twstock.db')
                    _db_manager = DatabaseManager(db_url)
                    # 初始化資料庫表格
                    await _db_manager.init_database()
                    print(f"Database initialized: {db_url}")
                
                _global_fetcher = WantgooFetcher(db_manager=_db_manager)
    
    # 確保初始化（只執行一次）
    if not _initialized:
        async with _init_lock:
            if not _initialized:  # 雙重檢查
                print("Initializing global fetcher...")
                # 觸發一次初始化
                await _global_fetcher._initialize_headers()
                _initialized = True
                print("Global fetcher initialized successfully")
    
    return _global_fetcher


async def get_db_manager():
    """獲取資料庫管理器"""
    global _db_manager
    
    if _db_manager is None:
        async with _init_lock:
            if _db_manager is None:
                db_url = os.environ.get('DATABASE_URL', 'sqlite+aiosqlite:///./twstock.db')
                _db_manager = DatabaseManager(db_url)
                await _db_manager.init_database()
    
    return _db_manager


async def close_global_resources():
    """關閉全域資源（資料庫連線等）"""
    global _db_manager
    
    if _db_manager:
        await _db_manager.close()
        _db_manager = None


def reset_global_fetcher():
    """重置全域 fetcher（測試用）"""
    global _global_fetcher, _initialized
    _global_fetcher = None
    _initialized = False