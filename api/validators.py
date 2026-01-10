"""
API 輸入驗證工具
提供統一的驗證邏輯，避免重複代碼
"""
from fastapi import HTTPException, Path, Query

from api.constants import ValidationRules


def validate_stock_id(stock_id: str) -> str:
    """
    驗證股票代碼格式

    Args:
        stock_id: 股票代碼

    Returns:
        驗證通過的股票代碼

    Raises:
        HTTPException: 格式不正確時拋出 400 錯誤
    """
    if not stock_id or len(stock_id) < ValidationRules.STOCK_ID_MIN_LENGTH or len(stock_id) > ValidationRules.STOCK_ID_MAX_LENGTH:
        raise HTTPException(status_code=400, detail="Invalid stock ID format")
    return stock_id


def validate_period(period: str) -> str:
    """
    驗證週期參數

    Args:
        period: 週期字符串（1M, 3M, 6M, 1Y）

    Returns:
        驗證通過的週期

    Raises:
        HTTPException: 週期不正確時拋出 400 錯誤
    """
    if period not in ValidationRules.VALID_PERIODS:
        raise HTTPException(
            status_code=400,
            detail=f"Invalid period. Must be one of {', '.join(ValidationRules.VALID_PERIODS)}"
        )
    return period


def validate_sort_field(field: str) -> str:
    """
    驗證排序欄位

    Args:
        field: 排序欄位名稱

    Returns:
        驗證通過的欄位名稱

    Raises:
        HTTPException: 欄位不正確時拋出 400 錯誤
    """
    if field not in ValidationRules.VALID_SORT_FIELDS:
        raise HTTPException(
            status_code=400,
            detail=f"Invalid sort field. Must be one of {', '.join(ValidationRules.VALID_SORT_FIELDS)}"
        )
    return field


def validate_sort_order(order: str) -> str:
    """
    驗證排序方向

    Args:
        order: 排序方向（asc, desc）

    Returns:
        驗證通過的排序方向

    Raises:
        HTTPException: 排序方向不正確時拋出 400 錯誤
    """
    if order not in ValidationRules.VALID_SORT_ORDERS:
        raise HTTPException(
            status_code=400,
            detail=f"Invalid sort order. Must be one of {', '.join(ValidationRules.VALID_SORT_ORDERS)}"
        )
    return order


def validate_weeks(weeks: int) -> int:
    """
    驗證週數參數

    Args:
        weeks: 週數

    Returns:
        驗證通過的週數

    Raises:
        HTTPException: 週數超出範圍時拋出 400 錯誤
    """
    if weeks < ValidationRules.MIN_WEEKS or weeks > ValidationRules.MAX_WEEKS:
        raise HTTPException(
            status_code=400,
            detail=f"Weeks must be between {ValidationRules.MIN_WEEKS} and {ValidationRules.MAX_WEEKS}"
        )
    return weeks


def validate_days(days: int) -> int:
    """
    驗證天數參數

    Args:
        days: 天數

    Returns:
        驗證通過的天數

    Raises:
        HTTPException: 天數超出範圍時拋出 400 錯誤
    """
    if days < ValidationRules.MIN_DAYS or days > ValidationRules.MAX_DAYS:
        raise HTTPException(
            status_code=400,
            detail=f"Days must be between {ValidationRules.MIN_DAYS} and {ValidationRules.MAX_DAYS}"
        )
    return days


# ============================================================
# FastAPI Path/Query 參數驗證器（可直接在路由中使用）
# ============================================================

StockIdPath = Path(
    ...,
    min_length=ValidationRules.STOCK_ID_MIN_LENGTH,
    max_length=ValidationRules.STOCK_ID_MAX_LENGTH,
    description="股票代碼（3-6 字元）"
)

WeeksQuery = Query(
    default=12,
    ge=ValidationRules.MIN_WEEKS,
    le=ValidationRules.MAX_WEEKS,
    description="歷史週數（1-52）"
)

DaysQuery = Query(
    default=30,
    ge=ValidationRules.MIN_DAYS,
    le=ValidationRules.MAX_DAYS,
    description="歷史天數（1-90）"
)
)
