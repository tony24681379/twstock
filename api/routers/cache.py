"""快取管理 API"""

from fastapi import APIRouter

from api.cache.signal_cache import signal_cache
from api.cache import RedisClient, invalidate_stock_cache

router = APIRouter(prefix="/api/cache", tags=["cache"])


@router.get("/stats")
async def get_cache_stats():
    """
    取得快取統計資訊

    Returns:
        dict: 包含以下資訊
            - hits: 命中次數
            - misses: 未命中次數
            - evictions: 淘汰次數
            - total_requests: 總請求次數
            - hit_rate: 命中率（百分比）
            - cache_size: 當前快取大小
            - max_size: 最大快取大小
            - ttl_seconds: TTL 設定（秒）
    """
    return signal_cache.get_stats()


@router.post("/clear")
async def clear_cache():
    """
    清空所有快取

    Returns:
        dict: 操作結果訊息
    """
    signal_cache.invalidate_all()
    return {"message": "快取已清空", "success": True}


@router.post("/clear/{stock_id}")
async def clear_stock_cache(stock_id: str):
    """
    清空特定股票的快取

    Args:
        stock_id: 股票代碼

    Returns:
        dict: 操作結果訊息
    """
    signal_cache.invalidate(stock_id)
    return {"message": f"股票 {stock_id} 的快取已清空", "success": True}


@router.post("/cleanup")
async def cleanup_expired():
    """
    手動清理過期的快取項目

    Returns:
        dict: 操作結果訊息
    """
    signal_cache.cleanup_expired()
    return {"message": "過期快取已清理", "success": True}


# ============================================================
# Redis 快取管理端點
# ============================================================


@router.get("/redis/stats")
async def get_redis_stats():
    """
    取得 Redis 快取統計資訊

    Returns:
        dict: 包含以下資訊
            - connected: Redis 連線狀態
            - hits: 命中次數
            - misses: 未命中次數
            - errors: 錯誤次數
            - total_requests: 總請求次數
            - hit_rate: 命中率（百分比）
            - memory_used: 記憶體使用量（如果已連線）
            - connected_clients: 連線客戶端數（如果已連線）
            - total_keys: 快取鍵總數（如果已連線）
    """
    return await RedisClient.get_stats()


@router.get("/redis/keys")
async def get_redis_keys(pattern: str = "*"):
    """
    取得所有快取鍵

    Args:
        pattern: 快取鍵模式（預設 "*" 表示所有）

    Returns:
        dict: 包含快取鍵列表
    """
    keys = await RedisClient.keys(pattern)
    return {"pattern": pattern, "count": len(keys), "keys": keys}


@router.post("/redis/clear")
async def clear_redis_cache():
    """
    清空所有 Redis 快取

    Returns:
        dict: 操作結果訊息
    """
    success = await RedisClient.flushall()
    if success:
        return {"message": "Redis 快取已清空", "success": True}
    else:
        return {"message": "Redis 快取清空失敗（未連線）", "success": False}


@router.post("/redis/clear/{stock_id}")
async def clear_redis_stock_cache(stock_id: str):
    """
    清空特定股票的所有 Redis 快取

    Args:
        stock_id: 股票代碼

    Returns:
        dict: 操作結果訊息
    """
    deleted = await invalidate_stock_cache(stock_id)
    return {
        "message": f"股票 {stock_id} 的 Redis 快取已清空",
        "success": True,
        "deleted_keys": deleted,
    }


@router.post("/redis/clear-pattern")
async def clear_redis_pattern(pattern: str):
    """
    清空符合模式的 Redis 快取

    Args:
        pattern: 快取鍵模式（例如 "chart:*", "chips:2330*"）

    Returns:
        dict: 操作結果訊息
    """
    deleted = await RedisClient.delete_pattern(pattern)
    return {
        "message": f"符合模式 '{pattern}' 的快取已清空",
        "success": True,
        "deleted_keys": deleted,
    }
