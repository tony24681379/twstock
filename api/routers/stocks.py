"""股票 API 路由"""

from typing import Optional

from fastapi import APIRouter, Depends, HTTPException, Query, Response
from sqlalchemy.ext.asyncio import AsyncSession

from api.auth import AuthUser, get_current_user
from api.config import DEFAULT_PAGE_SIZE, MAX_PAGE_SIZE
from api.models.chart import ChartDataResponse
from api.models.stock import (
    ChipsData,
    FundamentalInfo,
    StockDetail,
    StockHistory,
    StockListResponse,
)
from api.services.stock_service import StockService

router = APIRouter(prefix="/api/stocks", tags=["stocks"])


async def get_db_session():
    """取得資料庫 session（dependency）"""
    from api.main import db_manager

    if not db_manager:
        raise HTTPException(status_code=503, detail="Database not initialized")

    async with db_manager.async_session_factory() as session:
        yield session


@router.get("", response_model=StockListResponse)
async def list_stocks(
    response: Response,
    sort_by: str = Query("overall_strength", description="排序欄位"),
    order: str = Query("desc", description="排序方向 (asc/desc)"),
    limit: int = Query(DEFAULT_PAGE_SIZE, le=MAX_PAGE_SIZE, description="每頁筆數"),
    offset: int = Query(0, ge=0, description="偏移量"),
    # 自訂權重參數（籌碼 + 技術 + 基本面）
    chip_weight: float = Query(0.4, ge=0, le=1, description="籌碼權重"),
    tech_weight: float = Query(0.3, ge=0, le=1, description="技術權重"),
    fund_weight: float = Query(0.3, ge=0, le=1, description="基本面權重"),
    session: AsyncSession = Depends(get_db_session),
    user: Optional[AuthUser] = Depends(get_current_user),
):
    """
    取得股票列表（支援自訂權重）

    - **sort_by**: 排序欄位（stock_id, chip_strength, technical_strength, fundamental_strength, overall_strength, expected_return, win_rate, signal_count）
    - **order**: 排序方向（asc, desc）
    - **limit**: 每頁筆數（最大 500）
    - **offset**: 偏移量
    - **chip_weight**: 籌碼權重（0-1，預設 0.4）
    - **tech_weight**: 技術權重（0-1，預設 0.3）
    - **fund_weight**: 基本面權重（0-1，預設 0.3）

    三種權重總和必須為 1.0
    """
    # 驗證權重總和
    total = chip_weight + tech_weight + fund_weight
    if abs(total - 1.0) > 0.01:
        raise HTTPException(
            status_code=400, detail=f"權重總和必須為 1.0（當前: {total:.2f}）"
        )

    # 驗證參數
    allowed_sort_fields = [
        "stock_id",
        "chip_strength",
        "technical_strength",
        "fundamental_strength",
        "overall_strength",
        "signal_strength",  # 向後相容
        "expected_return",
        "win_rate",
        "signal_count",
        "cb_arbitrage_score",
    ]
    if sort_by not in allowed_sort_fields:
        raise HTTPException(
            status_code=400,
            detail=f"Invalid sort field. Allowed: {', '.join(allowed_sort_fields)}",
        )

    if order not in ["asc", "desc"]:
        raise HTTPException(status_code=400, detail="Invalid order direction")

    try:
        items, pagination = await StockService.get_stock_list(
            session,
            sort_by,
            order,
            limit,
            offset,
            chip_weight,
            tech_weight,
            fund_weight,  # 傳遞權重（籌碼 + 技術 + 基本面）
        )

        # 設定快取標頭（資料每小時更新一次）
        response.headers["Cache-Control"] = "public, max-age=3600"

        return StockListResponse(data=items, pagination=pagination)

    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Internal server error: {str(e)}")


# 注意：具體路徑（/chart, /history）必須在通用路徑（/{stock_id}）之前定義
@router.get("/{stock_id}/chart", response_model=ChartDataResponse)
async def get_stock_chart(
    stock_id: str,
    response: Response,
    period: str = Query("3M", description="時間範圍 (1M/3M/6M/1Y)"),
    session: AsyncSession = Depends(get_db_session),
    user: Optional[AuthUser] = Depends(get_current_user),
):
    """
    取得股票 K 線圖表資料（OHLCV）

    - **stock_id**: 股票代碼
    - **period**: 時間範圍（1M=1個月, 3M=3個月, 6M=6個月, 1Y=1年）

    注意：技術指標在前端即時計算，不由後端提供
    """
    # 驗證股票代碼（允許字母、破折號、插入符號等特殊字符）
    if not stock_id or len(stock_id) < 3 or len(stock_id) > 6:
        raise HTTPException(status_code=400, detail="Invalid stock ID format")

    # 驗證 period
    if period not in ["1M", "3M", "6M", "1Y"]:
        raise HTTPException(
            status_code=400, detail="Invalid period. Use 1M, 3M, 6M, or 1Y"
        )

    try:
        # 不傳 indicators 參數，後端不計算指標
        chart_data = await StockService.get_chart_data(session, stock_id, period, "")

        if not chart_data:
            raise HTTPException(
                status_code=404, detail=f"No chart data found for stock {stock_id}"
            )

        # 設定快取標頭（歷史 K線資料不常變動）
        response.headers["Cache-Control"] = "public, max-age=7200"  # 2 小時

        return chart_data

    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Internal server error: {str(e)}")


@router.get("/{stock_id}/history", response_model=StockHistory)
async def get_stock_history(
    stock_id: str,
    weeks: int = Query(12, ge=1, le=52, description="週數"),
    session: AsyncSession = Depends(get_db_session),
    user: Optional[AuthUser] = Depends(get_current_user),
):
    """
    取得股票籌碼集中度歷史資料

    - **stock_id**: 股票代碼
    - **weeks**: 週數（預設 12，最大 52）
    """
    # 驗證股票代碼（允許字母、破折號、插入符號等特殊字符）
    if not stock_id or len(stock_id) < 3 or len(stock_id) > 6:
        raise HTTPException(status_code=400, detail="Invalid stock ID format")

    try:
        history = await StockService.get_stock_history(session, stock_id, weeks)

        if not history:
            raise HTTPException(
                status_code=404,
                detail=f"Stock ID {stock_id} not found or no history data",
            )

        return history

    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Internal server error: {str(e)}")


@router.get("/{stock_id}/chips", response_model=ChipsData)
async def get_stock_chips(
    stock_id: str,
    days: int = Query(30, ge=1, le=90, description="查詢天數"),
    session: AsyncSession = Depends(get_db_session),
    user: Optional[AuthUser] = Depends(get_current_user),
):
    """
    取得股票籌碼資料（三大法人、主力、融資融券）

    - **stock_id**: 股票代碼
    - **days**: 查詢天數（預設 30 天，最大 90 天）
    """
    # 驗證股票代碼（允許字母、破折號、插入符號等特殊字符）
    if not stock_id or len(stock_id) < 3 or len(stock_id) > 6:
        raise HTTPException(status_code=400, detail="Invalid stock ID format")

    try:
        chips_data = await StockService.get_chips_data(session, stock_id, days)

        if not chips_data:
            raise HTTPException(
                status_code=404, detail=f"No chips data found for stock {stock_id}"
            )

        return chips_data

    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Internal server error: {str(e)}")


@router.get("/{stock_id}/fundamental", response_model=FundamentalInfo)
async def get_stock_fundamental(
    stock_id: str,
    session: AsyncSession = Depends(get_db_session),
    user: Optional[AuthUser] = Depends(get_current_user),
):
    """
    取得股票基本面資訊（詳細頁專用）

    - **stock_id**: 股票代碼
    """
    # 驗證股票代碼格式
    if not stock_id or len(stock_id) < 3 or len(stock_id) > 6:
        raise HTTPException(status_code=400, detail="Invalid stock ID format")

    try:
        fundamental_info = await StockService.get_fundamental_info(session, stock_id)

        if not fundamental_info:
            raise HTTPException(
                status_code=404, detail=f"Stock ID {stock_id} not found"
            )

        return fundamental_info

    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Internal server error: {str(e)}")


@router.get("/{stock_id}/convertible")
async def get_stock_convertible_bonds(
    stock_id: str,
    session: AsyncSession = Depends(get_db_session),
    user: Optional[AuthUser] = Depends(get_current_user),
):
    """取得股票關聯的可轉債列表"""
    from sqlalchemy import text as sa_text

    result = await session.execute(
        sa_text("""
            SELECT cb.bond_id, cb.name,
                   COALESCE(s.normalized_score, 0) as normalized_score,
                   s.premium_rate, s.risk_level
            FROM convertible_bond cb
            LEFT JOIN convertible_bond_signals s ON cb.bond_id = s.bond_id
            WHERE cb.underlying_stock_id = :stock_id AND cb.is_active = true
            ORDER BY COALESCE(s.normalized_score, 0) DESC
        """),
        {"stock_id": stock_id.lower()},
    )
    return [dict(row._mapping) for row in result.fetchall()]


@router.get("/{stock_id}", response_model=StockDetail)
async def get_stock(
    stock_id: str,
    session: AsyncSession = Depends(get_db_session),
    user: Optional[AuthUser] = Depends(get_current_user),
):
    """
    取得特定股票的詳細資訊

    - **stock_id**: 股票代碼（4-6 位數字）
    """
    # 簡單驗證股票代碼格式（允許字母、破折號、插入符號等特殊字符）
    if not stock_id or len(stock_id) < 3 or len(stock_id) > 6:
        raise HTTPException(status_code=400, detail="Invalid stock ID format")

    try:
        stock_detail = await StockService.get_stock_detail(session, stock_id)

        if not stock_detail:
            raise HTTPException(
                status_code=404, detail=f"Stock ID {stock_id} not found"
            )

        return stock_detail

    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Internal server error: {str(e)}")
