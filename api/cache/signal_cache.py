"""訊號快取管理器"""

import threading
from collections import OrderedDict
from dataclasses import dataclass
from datetime import datetime
from typing import Optional


@dataclass
class CacheEntry:
    """快取項目"""

    data: dict  # 計算結果
    db_last_date: datetime  # DB 最新日期
    cached_at: datetime  # 快取時間
    hit_count: int = 0  # 命中次數


class SignalCacheManager:
    """
    訊號快取管理器（Singleton）

    特性：
    - Singleton 模式：全域唯一實例
    - LRU 淘汰策略：使用 OrderedDict 實現
    - 智能失效：根據 DB 最新日期自動判斷快取是否有效
    - TTL 保護：1 小時過期機制
    - 線程安全：使用 threading.Lock
    - 統計監控：追蹤命中率、快取大小、淘汰次數
    """

    _instance = None
    _lock = threading.Lock()

    def __new__(cls):
        if cls._instance is None:
            with cls._lock:
                if cls._instance is None:
                    cls._instance = super().__new__(cls)
                    cls._instance._initialized = False
        return cls._instance

    def __init__(self):
        if self._initialized:
            return

        self._cache: OrderedDict[str, CacheEntry] = OrderedDict()
        self._max_size = 3000  # 最多快取 3000 支股票
        self._ttl_seconds = 3600  # 1 小時 TTL
        self._cache_lock = threading.Lock()  # 快取專用鎖
        self._initialized = True

        # 統計資訊
        self.stats = {"hits": 0, "misses": 0, "evictions": 0, "total_requests": 0}

        print(f"✅ SignalCacheManager 已初始化（max_size={self._max_size}, TTL={self._ttl_seconds}s）")

    def get(self, stock_id: str, db_last_date: datetime) -> Optional[dict]:
        """
        取得快取資料

        Args:
            stock_id: 股票代碼
            db_last_date: 該股票在 DB 中的最新日期

        Returns:
            快取的計算結果，如果無效則返回 None
        """
        with self._cache_lock:
            self.stats["total_requests"] += 1

            # 檢查是否存在
            if stock_id not in self._cache:
                self.stats["misses"] += 1
                return None

            entry = self._cache[stock_id]

            # 檢查 DB 資料是否更新
            if entry.db_last_date < db_last_date:
                # DB 有新資料，快取失效
                del self._cache[stock_id]
                self.stats["misses"] += 1
                return None

            # 檢查 TTL
            age = (datetime.now() - entry.cached_at).total_seconds()
            if age > self._ttl_seconds:
                # 超過 TTL，快取失效
                del self._cache[stock_id]
                self.stats["misses"] += 1
                return None

            # Cache hit
            entry.hit_count += 1
            self.stats["hits"] += 1

            # LRU: 移到最後（最近使用）
            self._cache.move_to_end(stock_id)

            return entry.data

    def set(self, stock_id: str, data: dict, db_last_date: datetime):
        """
        設置快取

        Args:
            stock_id: 股票代碼
            data: 計算結果（包含技術和基本面訊號）
            db_last_date: DB 最新日期
        """
        with self._cache_lock:
            # 檢查是否需要淘汰舊項目
            if len(self._cache) >= self._max_size:
                # LRU: 移除最久未使用的項目
                oldest_key = next(iter(self._cache))
                del self._cache[oldest_key]
                self.stats["evictions"] += 1

            # 加入新項目
            self._cache[stock_id] = CacheEntry(
                data=data, db_last_date=db_last_date, cached_at=datetime.now()
            )

    def invalidate(self, stock_id: str):
        """使特定股票的快取失效"""
        with self._cache_lock:
            if stock_id in self._cache:
                del self._cache[stock_id]

    def invalidate_all(self):
        """清空所有快取"""
        with self._cache_lock:
            count = len(self._cache)
            self._cache.clear()
            print(f"🗑️  已清空所有快取（{count} 個項目）")

    def cleanup_expired(self):
        """清理過期項目"""
        with self._cache_lock:
            now = datetime.now()
            expired_keys = [
                key
                for key, entry in self._cache.items()
                if (now - entry.cached_at).total_seconds() > self._ttl_seconds
            ]

            for key in expired_keys:
                del self._cache[key]

            if expired_keys:
                print(f"🗑️  已清理 {len(expired_keys)} 個過期快取項目")

    def get_stats(self) -> dict:
        """取得快取統計資訊"""
        with self._cache_lock:
            total = self.stats["total_requests"]
            hit_rate = (self.stats["hits"] / total * 100) if total > 0 else 0

            return {
                **self.stats,
                "hit_rate": f"{hit_rate:.2f}%",
                "cache_size": len(self._cache),
                "max_size": self._max_size,
                "ttl_seconds": self._ttl_seconds,
            }


# 全域實例
signal_cache = SignalCacheManager()
