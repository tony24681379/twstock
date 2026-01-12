"""快取管理 API"""

from fastapi import APIRouter

from api.cache.signal_cache import signal_cache

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
