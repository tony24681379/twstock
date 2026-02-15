"""
Redis 快取客戶端

使用 Singleton 模式管理 Redis 連線，支援 Upstash Redis。
提供基本的快取操作和統計功能。
"""

import json
import logging
import os
from typing import Any, Optional, List
from datetime import timedelta

import redis.asyncio as redis
from redis.asyncio import Redis
from redis.exceptions import RedisError, ConnectionError

logger = logging.getLogger(__name__)


class RedisClient:
    """Redis 快取客戶端 (Singleton)"""

    _instance: Optional['RedisClient'] = None
    _redis: Optional[Redis] = None
    _is_connected: bool = False

    # 統計資訊
    _hits: int = 0
    _misses: int = 0
    _errors: int = 0

    def __new__(cls):
        if cls._instance is None:
            cls._instance = super().__new__(cls)
        return cls._instance

    @classmethod
    async def connect(cls) -> 'RedisClient':
        """
        建立 Redis 連線

        從環境變數 REDIS_URL 讀取連線字串
        支援 redis:// 和 rediss:// (TLS) 協議
        """
        if cls._redis is not None and cls._is_connected:
            return cls._instance

        redis_url = os.getenv('REDIS_URL')
        if not redis_url:
            logger.warning("REDIS_URL 環境變數未設定，Redis 快取已停用")
            cls._is_connected = False
            return cls._instance

        try:
            # 解析 Redis URL
            # 支援 Upstash Redis: rediss://default:password@host:port
            cls._redis = redis.from_url(
                redis_url,
                encoding="utf-8",
                decode_responses=True,
                socket_connect_timeout=5,
                socket_timeout=5,
                retry_on_timeout=True,
                health_check_interval=30,
            )

            # 測試連線
            await cls._redis.ping()
            cls._is_connected = True
            logger.info(f"Redis 連線成功: {redis_url.split('@')[-1]}")  # 隱藏密碼

        except (RedisError, ConnectionError) as e:
            logger.error(f"Redis 連線失敗: {e}")
            cls._is_connected = False
            cls._redis = None

        return cls._instance

    @classmethod
    async def disconnect(cls):
        """關閉 Redis 連線"""
        if cls._redis:
            await cls._redis.aclose()
            cls._redis = None
            cls._is_connected = False
            logger.info("Redis 連線已關閉")

    @classmethod
    async def health_check(cls) -> bool:
        """健康檢查"""
        if not cls._is_connected or cls._redis is None:
            return False

        try:
            await cls._redis.ping()
            return True
        except (RedisError, ConnectionError):
            cls._is_connected = False
            return False

    @classmethod
    async def get(cls, key: str) -> Optional[Any]:
        """
        取得快取值

        Args:
            key: 快取鍵

        Returns:
            快取值（自動反序列化），不存在則返回 None
        """
        if not cls._is_connected or cls._redis is None:
            return None

        try:
            value = await cls._redis.get(key)
            if value is None:
                cls._misses += 1
                return None

            cls._hits += 1
            # 嘗試反序列化 JSON
            try:
                return json.loads(value)
            except (json.JSONDecodeError, TypeError):
                # 如果不是 JSON，直接返回字串
                return value

        except (RedisError, ConnectionError) as e:
            logger.error(f"Redis GET 錯誤 [{key}]: {e}")
            cls._errors += 1
            return None

    @classmethod
    async def set(
        cls,
        key: str,
        value: Any,
        ttl: Optional[int] = None
    ) -> bool:
        """
        設定快取值

        Args:
            key: 快取鍵
            value: 快取值（自動序列化）
            ttl: 過期時間（秒），None 表示永久

        Returns:
            是否設定成功
        """
        if not cls._is_connected or cls._redis is None:
            return False

        try:
            # 自動序列化為 JSON
            if not isinstance(value, str):
                value = json.dumps(value, ensure_ascii=False)

            if ttl:
                await cls._redis.setex(key, ttl, value)
            else:
                await cls._redis.set(key, value)

            return True

        except (RedisError, ConnectionError, TypeError) as e:
            logger.error(f"Redis SET 錯誤 [{key}]: {e}")
            cls._errors += 1
            return False

    @classmethod
    async def delete(cls, key: str) -> bool:
        """
        刪除快取

        Args:
            key: 快取鍵

        Returns:
            是否刪除成功
        """
        if not cls._is_connected or cls._redis is None:
            return False

        try:
            await cls._redis.delete(key)
            return True

        except (RedisError, ConnectionError) as e:
            logger.error(f"Redis DELETE 錯誤 [{key}]: {e}")
            cls._errors += 1
            return False

    @classmethod
    async def delete_pattern(cls, pattern: str) -> int:
        """
        刪除符合模式的所有快取

        Args:
            pattern: 快取鍵模式（支援 * 萬用字元）

        Returns:
            刪除的數量
        """
        if not cls._is_connected or cls._redis is None:
            return 0

        try:
            keys = await cls._redis.keys(pattern)
            if not keys:
                return 0

            deleted = await cls._redis.delete(*keys)
            return deleted

        except (RedisError, ConnectionError) as e:
            logger.error(f"Redis DELETE_PATTERN 錯誤 [{pattern}]: {e}")
            cls._errors += 1
            return 0

    @classmethod
    async def exists(cls, key: str) -> bool:
        """檢查快取是否存在"""
        if not cls._is_connected or cls._redis is None:
            return False

        try:
            return await cls._redis.exists(key) > 0
        except (RedisError, ConnectionError) as e:
            logger.error(f"Redis EXISTS 錯誤 [{key}]: {e}")
            cls._errors += 1
            return False

    @classmethod
    async def keys(cls, pattern: str = "*") -> List[str]:
        """
        取得符合模式的所有快取鍵

        Args:
            pattern: 快取鍵模式（預設 "*" 表示所有）

        Returns:
            快取鍵列表
        """
        if not cls._is_connected or cls._redis is None:
            return []

        try:
            keys = await cls._redis.keys(pattern)
            return keys
        except (RedisError, ConnectionError) as e:
            logger.error(f"Redis KEYS 錯誤 [{pattern}]: {e}")
            cls._errors += 1
            return []

    @classmethod
    async def flushall(cls) -> bool:
        """
        清空所有快取

        Returns:
            是否清空成功
        """
        if not cls._is_connected or cls._redis is None:
            return False

        try:
            await cls._redis.flushall()
            # 重置統計
            cls._hits = 0
            cls._misses = 0
            cls._errors = 0
            return True

        except (RedisError, ConnectionError) as e:
            logger.error(f"Redis FLUSHALL 錯誤: {e}")
            cls._errors += 1
            return False

    @classmethod
    async def get_stats(cls) -> dict:
        """
        取得快取統計資訊

        Returns:
            統計資訊字典
        """
        total_requests = cls._hits + cls._misses
        hit_rate = (cls._hits / total_requests * 100) if total_requests > 0 else 0

        stats = {
            "connected": cls._is_connected,
            "hits": cls._hits,
            "misses": cls._misses,
            "errors": cls._errors,
            "total_requests": total_requests,
            "hit_rate": f"{hit_rate:.2f}%",
        }

        # 如果已連線，取得 Redis 伺服器資訊
        if cls._is_connected and cls._redis:
            try:
                info = await cls._redis.info()
                stats["memory_used"] = info.get("used_memory_human", "N/A")
                stats["connected_clients"] = info.get("connected_clients", 0)
                stats["total_keys"] = await cls._redis.dbsize()
            except (RedisError, ConnectionError) as e:
                logger.error(f"Redis INFO 錯誤: {e}")

        return stats

    @classmethod
    def reset_stats(cls):
        """重置統計資訊"""
        cls._hits = 0
        cls._misses = 0
        cls._errors = 0
