"""
Redis 快取裝飾器

提供 @cache_query_result 裝飾器，自動快取資料庫查詢結果。
支援自訂 TTL、快取鍵前綴、跳過快取等功能。
"""

import hashlib
import json
import logging
from functools import wraps
from typing import Callable, Optional, Any

from .redis_client import RedisClient

logger = logging.getLogger(__name__)


def cache_query_result(
    prefix: str,
    ttl: int = 3600,
    key_params: Optional[list] = None
):
    """
    快取查詢結果的裝飾器

    Args:
        prefix: 快取鍵前綴（例如："chart", "chips", "stock"）
        ttl: 過期時間（秒），預設 1 小時
        key_params: 用於生成快取鍵的參數名稱列表，None 表示使用所有參數

    使用範例:
        @cache_query_result(prefix="chart", ttl=7200, key_params=["stock_id", "days"])
        async def get_chart_data(stock_id: str, days: int = 490):
            # 資料庫查詢邏輯
            return data

    快取鍵格式: {prefix}:{param1}:{param2}:...
    例如: chart:2330:490, chips:2330, stock:2330
    """

    def decorator(func: Callable) -> Callable:
        @wraps(func)
        async def wrapper(*args, **kwargs) -> Any:
            # 檢查是否跳過快取
            force_refresh = kwargs.pop("force_refresh", False)

            # 生成快取鍵
            cache_key = _generate_cache_key(
                prefix=prefix,
                func=func,
                args=args,
                kwargs=kwargs,
                key_params=key_params
            )

            # 如果不強制刷新，先嘗試從快取取得
            if not force_refresh:
                cached_value = await RedisClient.get(cache_key)
                if cached_value is not None:
                    logger.debug(f"快取命中: {cache_key}")
                    return cached_value

            # 執行原函數
            logger.debug(f"快取未命中，執行查詢: {cache_key}")
            result = await func(*args, **kwargs)

            # 儲存到快取（非同步，不阻塞返回）
            if result is not None:
                await RedisClient.set(cache_key, result, ttl=ttl)

            return result

        return wrapper

    return decorator


def _generate_cache_key(
    prefix: str,
    func: Callable,
    args: tuple,
    kwargs: dict,
    key_params: Optional[list] = None
) -> str:
    """
    生成快取鍵

    Args:
        prefix: 快取鍵前綴
        func: 被裝飾的函數
        args: 位置參數
        kwargs: 關鍵字參數
        key_params: 用於生成快取鍵的參數名稱列表

    Returns:
        快取鍵字串，格式: {prefix}:{param1}:{param2}:...
    """
    import inspect

    # 取得函數簽名
    sig = inspect.signature(func)
    bound_args = sig.bind_partial(*args, **kwargs)
    bound_args.apply_defaults()

    # 如果指定了 key_params，只使用這些參數
    if key_params:
        key_values = []
        for param_name in key_params:
            value = bound_args.arguments.get(param_name)
            if value is not None:
                key_values.append(str(value))
    else:
        # 使用所有參數（排除 self, cls, force_refresh）
        key_values = []
        for param_name, value in bound_args.arguments.items():
            if param_name in ("self", "cls", "force_refresh"):
                continue
            if value is not None:
                # 處理複雜物件（序列化為 JSON 再雜湊）
                if isinstance(value, (dict, list, tuple)):
                    value_str = _hash_complex_object(value)
                else:
                    value_str = str(value)
                key_values.append(value_str)

    # 組合快取鍵
    cache_key = f"{prefix}:{':'.join(key_values)}"

    return cache_key


def _hash_complex_object(obj: Any) -> str:
    """
    為複雜物件生成雜湊值

    Args:
        obj: 複雜物件（dict, list, tuple 等）

    Returns:
        MD5 雜湊值（前 8 位）
    """
    try:
        # 序列化為 JSON 字串
        json_str = json.dumps(obj, sort_keys=True, ensure_ascii=False)
        # 計算 MD5 雜湊
        hash_obj = hashlib.md5(json_str.encode())
        # 返回前 8 位（節省空間）
        return hash_obj.hexdigest()[:8]
    except (TypeError, ValueError):
        # 如果無法序列化，使用物件的字串表示
        return str(hash(str(obj)))[:8]


def invalidate_cache(prefix: str, **params):
    """
    清除特定快取

    Args:
        prefix: 快取鍵前綴
        **params: 快取鍵參數

    使用範例:
        await invalidate_cache(prefix="chart", stock_id="2330")
        await invalidate_cache(prefix="chips", stock_id="2330")
    """
    async def _invalidate():
        if not params:
            # 清除所有符合前綴的快取
            pattern = f"{prefix}:*"
        else:
            # 清除特定快取
            key_parts = [prefix]
            key_parts.extend(str(v) for v in params.values())
            pattern = ":".join(key_parts)

        deleted = await RedisClient.delete_pattern(pattern)
        logger.info(f"清除快取: {pattern}, 刪除 {deleted} 個鍵")
        return deleted

    return _invalidate()


def invalidate_stock_cache(stock_id: str):
    """
    清除特定股票的所有快取

    Args:
        stock_id: 股票代碼

    使用範例:
        await invalidate_stock_cache("2330")
    """
    async def _invalidate():
        patterns = [
            f"chart:{stock_id}:*",
            f"chips:{stock_id}",
            f"history:{stock_id}:*",
            f"fundamental:{stock_id}",
            f"stock:{stock_id}",
        ]

        total_deleted = 0
        for pattern in patterns:
            deleted = await RedisClient.delete_pattern(pattern)
            total_deleted += deleted

        logger.info(f"清除股票快取 [{stock_id}]: 刪除 {total_deleted} 個鍵")
        return total_deleted

    return _invalidate()
