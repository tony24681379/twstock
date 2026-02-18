"""可轉債 API 路由"""

import json
import math
from datetime import datetime

from fastapi import APIRouter, Query, Response
from sqlalchemy import text

from api.models.convertible import (
    CBSignal,
    ConvertibleBondDetail,
    ConvertibleBondHistoryPoint,
    ConvertibleBondListItem,
    ConvertibleBondListResponse,
    PaginationMetadata,
)

router = APIRouter(prefix="/api/convertible", tags=["convertible"])

# 允許的排序欄位
ALLOWED_SORT_FIELDS = {
    "bond_id", "normalized_score", "premium_rate",
    "arbitrage_spread", "volume", "maturity_date", "conversion_value",
}


def _get_db():
    """取得 DB session（延遲導入避免循環依賴）"""
    from api.main import db_manager
    return db_manager


@router.get("", response_model=ConvertibleBondListResponse)
async def list_convertible_bonds(
    response: Response,
    sort_by: str = Query("normalized_score", description="排序欄位"),
    order: str = Query("desc", description="排序方向"),
    limit: int = Query(200, le=500),
    offset: int = Query(0, ge=0),
    active_only: bool = Query(True, description="僅顯示活躍 CB"),
):
    """取得可轉債列表"""
    if sort_by not in ALLOWED_SORT_FIELDS:
        from fastapi import HTTPException
        raise HTTPException(status_code=400, detail="Invalid sort field")

    order_dir = "DESC" if order == "desc" else "ASC"
    db = _get_db()

    async with db.get_session() as session:
        # 查詢 CB + signals + stock name
        where = "WHERE cb.is_active = true" if active_only else ""
        count_where = "WHERE is_active = true" if active_only else ""

        # 計算總數
        count_result = await session.execute(
            text(f"SELECT COUNT(*) FROM convertible_bond {count_where}")
        )
        total = count_result.scalar() or 0

        # 主查詢
        # 決定排序來源表
        sort_table = "s" if sort_by in ("normalized_score", "premium_rate", "arbitrage_spread") else "cb"
        if sort_by == "volume":
            sort_expr = "d.volume"
        elif sort_by in ("normalized_score", "premium_rate", "arbitrage_spread"):
            sort_expr = f"s.{sort_by}"
        else:
            sort_expr = f"cb.{sort_by}"

        result = await session.execute(
            text(f"""
                SELECT
                    cb.bond_id, cb.name, cb.underlying_stock_id,
                    cb.conversion_price, cb.maturity_date,
                    COALESCE(s.normalized_score, 0) as normalized_score,
                    COALESCE(s.signal_count, 0) as signal_count,
                    COALESCE(s.risk_level, '觀望') as risk_level,
                    s.signals_json,
                    s.premium_rate, s.conversion_value, s.updated_at,
                    sl.name as stock_name,
                    d_latest.close as cb_close,
                    d_latest.volume as cb_volume,
                    d_latest.arbitrage_spread
                FROM convertible_bond cb
                LEFT JOIN convertible_bond_signals s ON cb.bond_id = s.bond_id
                LEFT JOIN stock_list sl ON cb.underlying_stock_id = sl.stock_id
                LEFT JOIN LATERAL (
                    SELECT close, volume, arbitrage_spread
                    FROM convertible_bond_daily
                    WHERE bond_id = cb.bond_id
                    ORDER BY date DESC LIMIT 1
                ) d_latest ON true
                {where}
                ORDER BY {sort_expr} {order_dir} NULLS LAST
                LIMIT :limit OFFSET :offset
            """),
            {"limit": limit, "offset": offset},
        )
        rows = result.fetchall()

    page_size = limit
    page = offset // page_size + 1 if page_size > 0 else 1
    total_pages = math.ceil(total / page_size) if page_size > 0 else 1

    items = []
    for row in rows:
        r = row._mapping
        premium = r.get("premium_rate")
        arb_spread = r.get("arbitrage_spread")
        if arb_spread is None and premium is not None:
            arb_spread = round(-float(premium) - 0.6, 2)

        signals = []
        if r.get("signals_json"):
            raw = r["signals_json"]
            sig_list = json.loads(raw) if isinstance(raw, str) else raw
            signals = [CBSignal(**s) for s in sig_list]

        items.append(ConvertibleBondListItem(
            bond_id=r["bond_id"],
            name=r["name"] or "",
            underlying_stock_id=r["underlying_stock_id"] or "",
            underlying_stock_name=r.get("stock_name") or "",
            close=r.get("cb_close"),
            conversion_price=r.get("conversion_price"),
            conversion_value=r.get("conversion_value"),
            premium_rate=premium,
            arbitrage_spread=arb_spread,
            normalized_score=r.get("normalized_score") or 0,
            signal_count=r.get("signal_count") or 0,
            signals=signals,
            risk_level=r.get("risk_level") or "觀望",
            maturity_date=r.get("maturity_date"),
            volume=r.get("cb_volume"),
            last_updated=r.get("updated_at"),
        ))

    response.headers["Cache-Control"] = "public, max-age=3600"
    return ConvertibleBondListResponse(
        data=items,
        pagination=PaginationMetadata(
            total=total, page=page, page_size=page_size,
            total_pages=total_pages,
            has_next=page < total_pages, has_prev=page > 1,
        ),
    )


@router.get("/{bond_id}", response_model=ConvertibleBondDetail)
async def get_convertible_bond_detail(bond_id: str, response: Response):
    """取得可轉債詳情"""
    from fastapi import HTTPException

    db = _get_db()
    async with db.get_session() as session:
        result = await session.execute(
            text("""
                SELECT
                    cb.*,
                    s.raw_score, s.normalized_score, s.signal_count,
                    s.risk_level, s.signals_json, s.premium_rate as sig_premium,
                    s.conversion_value as sig_cv,
                    sl.name as stock_name,
                    d.close as cb_close, d.volume as cb_volume,
                    d.underlying_close, d.premium_rate as daily_premium,
                    d.conversion_value as daily_cv, d.arbitrage_spread,
                    ts.normalized_score as stock_strength
                FROM convertible_bond cb
                LEFT JOIN convertible_bond_signals s ON cb.bond_id = s.bond_id
                LEFT JOIN stock_list sl ON cb.underlying_stock_id = sl.stock_id
                LEFT JOIN stock_technical_signals ts ON cb.underlying_stock_id = ts.stock_id
                LEFT JOIN LATERAL (
                    SELECT close, volume, underlying_close, premium_rate,
                           conversion_value, arbitrage_spread
                    FROM convertible_bond_daily
                    WHERE bond_id = cb.bond_id
                    ORDER BY date DESC LIMIT 1
                ) d ON true
                WHERE cb.bond_id = :bond_id
            """),
            {"bond_id": bond_id},
        )
        row = result.fetchone()

    if not row:
        raise HTTPException(status_code=404, detail="Bond not found")

    r = row._mapping
    signals = []
    if r.get("signals_json"):
        raw = r["signals_json"]
        sig_list = json.loads(raw) if isinstance(raw, str) else raw
        signals = [CBSignal(**s) for s in sig_list]

    response.headers["Cache-Control"] = "public, max-age=3600"
    return ConvertibleBondDetail(
        bond_id=r["bond_id"],
        name=r["name"] or "",
        underlying_stock_id=r["underlying_stock_id"] or "",
        underlying_stock_name=r.get("stock_name") or "",
        issue_date=r.get("issue_date"),
        maturity_date=r.get("maturity_date"),
        put_date=r.get("put_date"),
        put_price=r.get("put_price"),
        coupon_rate=r.get("coupon_rate"),
        issued_amount=r.get("issued_amount"),
        outstanding_amount=r.get("outstanding_amount"),
        close=r.get("cb_close"),
        volume=r.get("cb_volume"),
        conversion_price=r.get("conversion_price"),
        conversion_value=r.get("daily_cv") or r.get("sig_cv"),
        premium_rate=r.get("daily_premium") or r.get("sig_premium"),
        arbitrage_spread=r.get("arbitrage_spread"),
        underlying_close=r.get("underlying_close"),
        underlying_overall_strength=r.get("stock_strength") or 0,
        normalized_score=r.get("normalized_score") or 0,
        raw_score=r.get("raw_score") or 0,
        signal_count=r.get("signal_count") or 0,
        risk_level=r.get("risk_level") or "觀望",
        signals=signals,
    )


@router.get("/{bond_id}/history")
async def get_convertible_bond_history(
    bond_id: str,
    response: Response,
    days: int = Query(90, ge=7, le=365),
):
    """取得可轉債歷史趨勢"""
    db = _get_db()
    async with db.get_session() as session:
        result = await session.execute(
            text("""
                SELECT date, close, volume, conversion_value, premium_rate, underlying_close
                FROM convertible_bond_daily
                WHERE bond_id = :bond_id
                  AND date >= NOW() - INTERVAL ':days days'
                ORDER BY date ASC
            """.replace(":days days", f"{days} days")),
            {"bond_id": bond_id},
        )
        rows = result.fetchall()

    response.headers["Cache-Control"] = "public, max-age=3600"
    return [
        ConvertibleBondHistoryPoint(
            date=r._mapping["date"],
            close=r._mapping.get("close"),
            volume=r._mapping.get("volume"),
            conversion_value=r._mapping.get("conversion_value"),
            premium_rate=r._mapping.get("premium_rate"),
            underlying_close=r._mapping.get("underlying_close"),
        )
        for r in rows
    ]
