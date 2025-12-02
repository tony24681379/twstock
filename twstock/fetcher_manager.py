# -*- coding: utf-8 -*-
"""
Fetcher 管理器
確保整個應用程式只有一個 fetcher 實例，避免重複初始化
"""

import asyncio

from .wantgoo import WantgooFetcher

# 全域單例 fetcher
_global_fetcher = None
_init_lock = asyncio.Lock()
_initialized = False


async def get_global_fetcher():
    """
    獲取全域共享的 fetcher 實例
    確保只初始化一次
    """
    global _global_fetcher, _initialized
    
    if _global_fetcher is None:
        async with _init_lock:
            if _global_fetcher is None:  # 雙重檢查
                print("Creating global fetcher instance...")
                _global_fetcher = WantgooFetcher()
    
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


def reset_global_fetcher():
    """重置全域 fetcher（測試用）"""
    global _global_fetcher, _initialized
    _global_fetcher = None
    _initialized = False