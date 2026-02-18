"""股票資料服務層"""

import asyncio
import json
import logging
from datetime import datetime, timedelta
from typing import List, Optional, Tuple

from sqlalchemy import asc, desc, func, select
from sqlalchemy.ext.asyncio import AsyncSession

from api.models.chart import ChartDataResponse, OHLCVData
from api.models.stock import (
    BasicInfo,
    CapitalInfo,
    ChipSignal,
    ChipsData,
    ConcentrationDataPoint,
    ConcentrationSummary,
    EPSDetail,
    FundamentalInfo,
    HoldingInfo,
    InstitutionalData,
    MajorInvestorData,
    MarginTradingData,
    MonthlyRevenueDetail,
    PaginationMetadata,
    PriceInfo,
    StockDetail,
    StockHistory,
    StockListItem,
)
from api.cache import cache_query_result
from api.services.fundamental_signal_service import FundamentalSignalService
from api.services.indicator_service import IndicatorService
from api.services.signal_service import SignalService
from twstock.database import (
    ConcentrationData,
    DatabaseManager,
    InstitutionalInvestors,
    MajorInvestors,
    MarginTrading,
    StockDaily,
    StockInfo,
    StockList,
    StockTechnicalSignals,
)
from twstock.vectorized_signals import (
    SIGNAL_TYPE_CROSSOVER,
    SIGNAL_TYPE_STATE,
    CROSSOVER_DISPLAY_DAYS,
    EXTREME_EVENT_SIGNALS,
)

logger = logging.getLogger(__name__)


async def calculate_all_strengths_for_stock(
    stock_id: str,
    bulk_info: dict,
    bulk_eps: dict,
    current_price: float,  # 🆕 直接傳入 close_price
    all_processor,
    db_last_date: datetime,
) -> tuple:
    """
    計算單一股票的技術和基本面評分（支援快取）

    Args:
        stock_id: 股票代碼
        bulk_info: 批次股票資訊字典
        bulk_eps: 批次 EPS 資料字典
        current_price: 當前股價（從 SQL 查詢取得）
        all_processor: All 處理器實例
        db_last_date: 該股票在 DB 中的最新日期

    Returns:
        (stock_id, score_dict) 包含 technical 和 fundamental 兩組分數
    """
    # 🆕 嘗試從快取取得
    from api.cache.signal_cache import signal_cache

    cached_result = signal_cache.get(stock_id, db_last_date)
    if cached_result is not None:
        return stock_id, cached_result

    # 快取未命中，執行計算
    result = {
        "fundamental": {"signals": [], "strength": 0, "raw_score": 0},
    }

    # 計算基本面強度
    if stock_id in bulk_info:
        try:
            stock_info_dict = bulk_info[stock_id]
            eps_df = bulk_eps.get(stock_id)

            # 🆕 優化：直接使用傳入的 current_price，不需要 fallback
            # current_price 已從 SQL 查詢的 latest_price_subquery 取得

            # 計算基本面訊號
            fund_signals, fund_raw_score = FundamentalSignalService.calculate_signals(
                stock_info_dict, eps_df, current_price
            )
            fund_strength = FundamentalSignalService.normalize_score(fund_raw_score)

            result["fundamental"] = {
                "signals": [
                    s if isinstance(s, dict) else s.dict() for s in fund_signals
                ],  # 🔧 轉換為字典以便快取
                "strength": fund_strength,
                "raw_score": fund_raw_score,
            }
        except Exception as e:
            logger.warning(f"計算 {stock_id} 基本面評分失敗: {e}")

    # 🆕 計算完成後存入快取
    signal_cache.set(stock_id, result, db_last_date)

    return stock_id, result


class StockService:
    """股票資料服務"""

    @staticmethod
    def normalize_technical_score(raw_score: int) -> int:
        """
        標準化技術訊號分數（-172~+174 → 0~100）

        Args:
            raw_score: 原始分數（-172~+174）

        Returns:
            標準化分數（0~100）
        """
        # 將 -172~+174 映射到 0~100
        # 公式：((raw_score + 172) / 346) * 100
        normalized = ((raw_score + 172) / 346) * 100
        return int(max(0, min(100, normalized)))

    @staticmethod
    async def get_stock_list(
        session: AsyncSession,
        sort_by: str = "stock_id",
        order: str = "asc",
        limit: int = 100,
        offset: int = 0,
        chip_weight: float = 0.4,
        tech_weight: float = 0.3,
        fund_weight: float = 0.3,
    ) -> Tuple[List[StockListItem], PaginationMetadata]:
        """從預計算快取表取得股票列表（SQL 排序分頁，零記憶體計算）"""
        from sqlalchemy import text as sa_text
        from api.models.stock import AlertStatus

        # 排序欄位白名單（防 SQL injection）
        sort_col_map = {
            "stock_id": "stock_id",
            "chip_strength": "chip_raw_score",
            "technical_strength": "tech_raw_score",
            "fundamental_strength": "fund_raw_score",
            "overall_strength": "overall_strength",
            "signal_strength": "overall_strength",
            "expected_return": "expected_return",
            "win_rate": "win_rate",
            "signal_count": "signal_count",
            "cb_arbitrage_score": "COALESCE(cb_arbitrage_score, -1)",
        }
        order_col = sort_col_map.get(sort_by, "stock_id")
        order_dir = "DESC" if order == "desc" else "ASC"

        # COUNT
        count_result = await session.execute(
            sa_text("SELECT COUNT(*) FROM stock_list_cache")
        )
        total = count_result.scalar() or 0

        if total == 0:
            return [], PaginationMetadata(
                total=0, page=1, page_size=limit,
                total_pages=0, has_next=False, has_prev=False,
            )

        # 單一 SQL：動態加權 overall_strength + 排序 + 分頁
        query = sa_text(f"""
            SELECT *,
                CAST(chip_normalized * CAST(:cw AS float)
                   + tech_normalized * CAST(:tw AS float)
                   + fund_normalized * CAST(:fw AS float) AS int)
                    AS overall_strength
            FROM stock_list_cache
            ORDER BY {order_col} {order_dir}
            LIMIT :lim OFFSET :off
        """)

        result = await session.execute(query, {
            "cw": chip_weight, "tw": tech_weight, "fw": fund_weight,
            "lim": limit, "off": offset,
        })
        rows = result.fetchall()

        # 組裝 StockListItem
        items = []
        for row in rows:
            m = row._mapping

            chip_sigs = [ChipSignal(**s) for s in json.loads(m["chip_signals_json"] or "[]")]
            tech_sigs_raw = json.loads(m["tech_signals_json"] or "[]")
            tech_sigs = [ChipSignal(**s) for s in tech_sigs_raw if not s.get("is_event") and s.get("name", "") not in EXTREME_EVENT_SIGNALS]
            event_sigs = [ChipSignal(**s) for s in tech_sigs_raw if s.get("is_event") or s.get("name", "") in EXTREME_EVENT_SIGNALS]
            fund_sigs = [ChipSignal(**s) for s in json.loads(m["fund_signals_json"] or "[]")]
            all_sigs = chip_sigs + tech_sigs + fund_sigs

            alert_status = None
            if m["alert_status_json"]:
                alert_status = AlertStatus(**json.loads(m["alert_status_json"]))

            overall = m["overall_strength"]

            items.append(StockListItem(
                stock_id=m["stock_id"],
                name=m["name"] or m["stock_id"],
                close_price=m["close_price"] or 0.0,
                alert_status=alert_status,
                chip_strength=m["chip_raw_score"] or 0,
                technical_strength=m["tech_raw_score"] or 0,
                fundamental_strength=m["fund_raw_score"] or 0,
                overall_strength=overall,
                weights={"chip": chip_weight, "technical": tech_weight, "fundamental": fund_weight},
                chip_signals=chip_sigs,
                technical_signals=tech_sigs,
                fundamental_signals=fund_sigs,
                recent_events=event_sigs,
                signals=all_sigs,
                signal_count=m["signal_count"] or 0,
                expected_return=m["expected_return"] or 0.0,
                win_rate=m["win_rate"] or 0.0,
                risk_level=m["risk_level"] or "中",
                major_signals=[s.name for s in all_sigs[:3]],
                cb_arbitrage_score=m["cb_arbitrage_score"],
                cb_signals=[
                    ChipSignal(name=s["name"], score=s.get("score", 0), triggered=s.get("triggered", True))
                    for s in json.loads(m.get("cb_signals_json") or "[]")
                ],
                signal_strength=overall,
                last_updated=m["last_date"] or m["updated_at"] or datetime.now(),
            ))

        total_pages = (total + limit - 1) // limit
        current_page = offset // limit + 1

        pagination = PaginationMetadata(
            total=total,
            page=current_page,
            page_size=limit,
            total_pages=total_pages,
            has_next=offset + limit < total,
            has_prev=offset > 0,
        )

        return items, pagination

    @staticmethod
    @cache_query_result(prefix="stock", ttl=3600, key_params=["stock_id"])
    async def get_stock_detail(
        session: AsyncSession, stock_id: str
    ) -> Optional[StockDetail]:
        """
        取得股票詳細資訊

        Args:
            session: 資料庫 session
            stock_id: 股票代碼

        Returns:
            股票詳細資訊或 None
        """
        # 查詢股票名稱與基本資訊
        stock_list_query = select(StockList).where(StockList.stock_id == stock_id)
        stock_list_result = await session.execute(stock_list_query)
        stock_list = stock_list_result.scalar_one_or_none()

        if not stock_list:
            return None

        # 查詢股票財務資訊
        stock_info_query = select(StockInfo).where(StockInfo.stock_id == stock_id)
        stock_info_result = await session.execute(stock_info_query)
        stock_info = stock_info_result.scalar_one_or_none()

        # 查詢最新價格
        stock_daily_query = (
            select(StockDaily)
            .where(StockDaily.stock_id == stock_id)
            .order_by(desc(StockDaily.date))
            .limit(1)
        )
        stock_daily_result = await session.execute(stock_daily_query)
        stock_daily = stock_daily_result.scalar_one_or_none()

        if not stock_daily:
            return None

        # 查詢最新籌碼集中度（至少 10 週用於訊號計算）
        concentration_query = (
            select(ConcentrationData)
            .where(ConcentrationData.stock_id == stock_id)
            .order_by(desc(ConcentrationData.date))
            .limit(10)
        )
        concentration_result = await session.execute(concentration_query)
        concentration_list = concentration_result.scalars().all()

        # 組裝回應
        basic_info = BasicInfo(
            stock_id=stock_list.stock_id,
            name=stock_list.name or stock_list.stock_id,
            industry=None,  # StockInfo 沒有 industry 欄位
            outstanding_shares=stock_info.outstanding_shares if stock_info else None,
        )

        price_info = PriceInfo(
            close=stock_daily.close,
            open=stock_daily.open,
            high=stock_daily.high,
            low=stock_daily.low,
            volume=stock_daily.volume,
            date=stock_daily.date,
            change=0.0,  # TODO: 計算漲跌
            change_percent=0.0,  # TODO: 計算漲跌幅
        )

        concentration_summary = ConcentrationSummary(
            large_holders_pct=(
                concentration_list[0].more_than_400 if concentration_list else 0.0
            ),
            super_large_holders_pct=(
                concentration_list[0].more_than_1000 if concentration_list else 0.0
            ),
            retail_pct=(
                concentration_list[0].less_than_20 if concentration_list else 0.0
            ),
            latest_date=(
                concentration_list[0].date if concentration_list else datetime.now()
            ),
        )

        # 計算籌碼集中度訊號
        chip_signals = []
        chip_strength = 0
        expected_return = 0.0
        win_rate = 0.0
        if concentration_list and len(concentration_list) >= 2:
            chip_signals, raw_score, expected_return, win_rate = (
                SignalService.calculate_signals(concentration_list)
            )
            chip_strength = SignalService.normalize_score(raw_score)

        # 查詢技術訊號（含時效性過濾 + 極端事件分離）
        tech_signals = []
        recent_events = []
        tech_signals_query = (
            select(StockTechnicalSignals)
            .where(StockTechnicalSignals.stock_id == stock_id)
        )
        tech_signals_result = await session.execute(tech_signals_query)
        tech_signal_data = tech_signals_result.scalar_one_or_none()

        if tech_signal_data and tech_signal_data.signals_detail:
            try:
                signals_detail = tech_signal_data.signals_detail
                today = stock_daily.date.date() if isinstance(stock_daily.date, datetime) else stock_daily.date

                for sig in signals_detail:
                    trigger_date = datetime.fromisoformat(sig["trigger_date"]).date()
                    signal_type = sig.get("type", SIGNAL_TYPE_STATE)

                    is_valid = False
                    if signal_type == SIGNAL_TYPE_CROSSOVER:
                        days_since = (today - trigger_date).days
                        is_valid = (days_since <= CROSSOVER_DISPLAY_DAYS)
                    else:
                        is_valid = sig.get("is_valid", False)

                    if is_valid:
                        chip_signal = ChipSignal(
                            name=sig["name"],
                            triggered=True,
                            score=sig["score"],
                            description=None
                        )
                        # 極端事件分離到 recent_events
                        if sig.get("is_event") or sig["name"] in EXTREME_EVENT_SIGNALS:
                            recent_events.append(chip_signal)
                        else:
                            tech_signals.append(chip_signal)

            except (json.JSONDecodeError, KeyError, ValueError) as e:
                logger.warning(f"解析 {stock_id} 詳情頁 signals_detail 失敗: {e}")

        # 🆕 載入警示狀態
        from api.main import db_manager as api_db_manager
        alert_status = None
        if api_db_manager:
            alert_status_dict = await api_db_manager.bulk_load_alert_status([stock_id])
            if stock_id in alert_status_dict:
                from api.models.stock import AlertStatus
                alert_data = alert_status_dict[stock_id]
                alert_status = AlertStatus(**alert_data)

        return StockDetail(
            basic_info=basic_info,
            price_info=price_info,
            alert_status=alert_status,
            chip_signals=chip_signals,
            technical_signals=tech_signals,
            recent_events=recent_events,
            expected_return=expected_return,
            win_rate=win_rate,
            concentration_summary=concentration_summary,
            signal_strength=chip_strength,
            signals=chip_signals,
        )

    @staticmethod
    @cache_query_result(prefix="history", ttl=3600, key_params=["stock_id", "weeks"])
    async def get_stock_history(
        session: AsyncSession, stock_id: str, weeks: int = 12
    ) -> Optional[StockHistory]:
        """
        取得股票籌碼集中度歷史

        Args:
            session: 資料庫 session
            stock_id: 股票代碼
            weeks: 週數

        Returns:
            歷史籌碼集中度資料或 None
        """
        query = (
            select(ConcentrationData)
            .where(ConcentrationData.stock_id == stock_id)
            .order_by(desc(ConcentrationData.date))
            .limit(weeks)
        )

        result = await session.execute(query)
        concentration_list = result.scalars().all()

        if not concentration_list:
            return None

        # 組裝資料點
        # concentration_list 已按 desc(date) 排序：[最新, 次新, ..., 最舊]
        data_points = []

        for i, concentration in enumerate(concentration_list):
            # i=0 → 最新（2026-01-02）
            # i=1 → 次新（2025-12-26）
            # i=11 → 最舊（2025-10-17）

            # 前一週 = 下一個 index（因為是 desc 排序）
            prev_concentration = (
                concentration_list[i + 1] if i + 1 < len(concentration_list) else None
            )

            moreThan400_change = 0.0
            moreThan1000_change = 0.0
            lessThan20_change = 0.0

            if prev_concentration:
                # 當前週 - 前一週（更舊的）
                moreThan400_change = (
                    concentration.more_than_400 - prev_concentration.more_than_400
                )
                moreThan1000_change = (
                    concentration.more_than_1000 - prev_concentration.more_than_1000
                )
                lessThan20_change = (
                    concentration.less_than_20 - prev_concentration.less_than_20
                )

            # 組裝資料點（原始順序：最新到最舊）
            data_points.append(
                ConcentrationDataPoint(
                    week_label=f"W{i + 1}",  # W1=最新, W2=次新, ..., W12=最舊
                    date=concentration.date,
                    moreThan400_pct=concentration.more_than_400,
                    moreThan1000_pct=concentration.more_than_1000,
                    lessThan20_pct=concentration.less_than_20,
                    moreThan400_change=moreThan400_change,
                    moreThan1000_change=moreThan1000_change,
                    lessThan20_change=lessThan20_change,
                )
            )

        # 反轉順序：變成 [最舊...最新]（W1 在最後，W12 在最前）
        # 這樣前端用 reverse() 後就會變成 [最新...最舊]（W1 在最前）
        return StockHistory(stock_id=stock_id, data=list(reversed(data_points)))

    @staticmethod
    @cache_query_result(prefix="chart", ttl=7200, key_params=["stock_id", "period", "indicators"])
    async def get_chart_data(
        session: AsyncSession, stock_id: str, period: str = "3M", indicators: str = "MA"
    ) -> Optional[ChartDataResponse]:
        """
        取得 K 線圖表資料與技術指標

        Args:
            session: 資料庫 session
            stock_id: 股票代碼
            period: 時間範圍（1M/3M/6M/1Y）
            indicators: 指標列表（逗號分隔，例如 "MA,MACD,RSI"）

        Returns:
            圖表資料或 None
        """
        # 計算日期範圍
        from datetime import datetime, timedelta

        period_days = {"1M": 30, "3M": 90, "6M": 180, "1Y": 365}

        days = period_days.get(period, 90)  # 預設 3 個月
        start_date = datetime.now() - timedelta(days=days)

        # 查詢 OHLCV 資料
        query = (
            select(StockDaily)
            .where(StockDaily.stock_id == stock_id)
            .where(StockDaily.date >= start_date)
            .order_by(asc(StockDaily.date))
        )

        result = await session.execute(query)
        stock_dailies = result.scalars().all()

        if not stock_dailies:
            return None

        # 組裝 OHLCV 資料
        ohlcv_list = []
        open_prices = []
        high_prices = []
        low_prices = []
        close_prices = []
        volumes = []

        for daily in stock_dailies:
            ohlcv_list.append(
                OHLCVData(
                    time=daily.date.strftime("%Y-%m-%d"),
                    open=daily.open,
                    high=daily.high,
                    low=daily.low,
                    close=daily.close,
                    volume=daily.volume,
                )
            )
            open_prices.append(daily.open)
            high_prices.append(daily.high)
            low_prices.append(daily.low)
            close_prices.append(daily.close)
            volumes.append(daily.volume)

        # 計算技術指標
        indicator_list = [ind.strip() for ind in indicators.split(",") if ind.strip()]
        calculated_indicators = {}

        if indicator_list:
            calculated_indicators = IndicatorService.calculate_indicators(
                open_prices,
                high_prices,
                low_prices,
                close_prices,
                volumes,
                indicator_list,
            )

        return ChartDataResponse(
            stock_id=stock_id,
            period=period,
            ohlcv=ohlcv_list,
            indicators=calculated_indicators,
        )

    @staticmethod
    @cache_query_result(prefix="chips", ttl=3600, key_params=["stock_id", "days"])
    async def get_chips_data(
        session: AsyncSession, stock_id: str, days: int = 30
    ) -> Optional[ChipsData]:
        """
        取得股票籌碼資料（三大法人、主力、融資融券）

        Args:
            session: 資料庫 session
            stock_id: 股票代碼
            days: 查詢天數（預設 30 天）

        Returns:
            籌碼資料或 None
        """
        # 計算日期範圍
        start_date = datetime.now() - timedelta(days=days)

        # 查詢三大法人資料
        institutional_query = (
            select(InstitutionalInvestors)
            .where(
                InstitutionalInvestors.stock_id == stock_id,
                InstitutionalInvestors.date >= start_date,
            )
            .order_by(desc(InstitutionalInvestors.date))
        )
        institutional_result = await session.execute(institutional_query)
        institutional_records = institutional_result.scalars().all()

        # 查詢 StockInfo 獲取 outstanding_shares（用於計算百分比）
        stock_info_query = select(StockInfo).where(StockInfo.stock_id == stock_id)
        stock_info_result = await session.execute(stock_info_query)
        stock_info = stock_info_result.scalar_one_or_none()

        # 計算 total_stock（張數單位）
        total_stock = None
        if stock_info and stock_info.outstanding_shares:
            total_stock = stock_info.outstanding_shares / 1000  # 股 → 千股（張）

        # 查詢主力資料
        major_query = (
            select(MajorInvestors)
            .where(
                MajorInvestors.stock_id == stock_id, MajorInvestors.date >= start_date
            )
            .order_by(desc(MajorInvestors.date))
        )
        major_result = await session.execute(major_query)
        major_records = major_result.scalars().all()

        # 查詢融資融券資料
        margin_query = (
            select(MarginTrading)
            .where(MarginTrading.stock_id == stock_id, MarginTrading.date >= start_date)
            .order_by(desc(MarginTrading.date))
        )
        margin_result = await session.execute(margin_query)
        margin_records = margin_result.scalars().all()

        # 查詢最新的籌碼集中度資料
        concentration_query = (
            select(ConcentrationData)
            .where(ConcentrationData.stock_id == stock_id)
            .order_by(desc(ConcentrationData.date))
            .limit(1)
        )
        concentration_result = await session.execute(concentration_query)
        concentration = concentration_result.scalar_one_or_none()

        # 如果三種資料都沒有，返回 None
        if not institutional_records and not major_records and not margin_records:
            return None

        # 組裝三大法人資料
        institutional_data = []
        for record in institutional_records:
            # 從張數計算百分比（API 層即時計算）
            foreign_pct = None
            investment_trust_pct = None
            dealer_pct = None

            if total_stock is not None and total_stock > 0:
                if record.foreign_shares is not None:
                    foreign_pct = round(record.foreign_shares / total_stock * 100, 2)
                if record.investment_trust_shares is not None:
                    investment_trust_pct = round(
                        record.investment_trust_shares / total_stock * 100, 2
                    )
                if record.dealer_shares is not None:
                    dealer_pct = round(record.dealer_shares / total_stock * 100, 2)

            institutional_data.append(
                InstitutionalData(
                    date=record.date,
                    # 張數欄位（來自資料庫）
                    foreign_shares=record.foreign_shares,
                    investment_trust_shares=record.investment_trust_shares,
                    dealer_shares=record.dealer_shares,
                    # 百分比欄位（即時計算）
                    foreign=foreign_pct,
                    investment_trust=investment_trust_pct,
                    dealer=dealer_pct,
                    # 持股比率（來自資料庫）
                    sum_holding_rate=record.sum_holding_rate,
                    foreign_holding_rate=record.foreign_holding_rate,
                    investment_trust_holding_rate=record.investment_trust_holding_rate,
                    dealer_holding_rate=record.dealer_holding_rate,
                )
            )

        major_data = [
            MajorInvestorData(
                date=record.date,
                major_investors=record.major_investors,
                agent_diff=record.agent_diff,
                skp20=record.skp20,
            )
            for record in major_records
        ]

        # 組裝融資融券資料並計算每日差額
        # margin_records 已按 desc(date) 排序：[最新, 次新, ..., 最舊]
        margin_data = []

        for i, record in enumerate(margin_records):
            # 前一日 = 下一個 index（因為是 desc 排序）
            prev_record = margin_records[i + 1] if i + 1 < len(margin_records) else None

            # 計算差額
            lending_change = 0.0
            borrowing_change = 0.0

            if prev_record:
                # 當前日 - 前一日
                if record.lending_balance is not None and prev_record.lending_balance is not None:
                    lending_change = record.lending_balance - prev_record.lending_balance
                if record.borrowing_balance is not None and prev_record.borrowing_balance is not None:
                    borrowing_change = record.borrowing_balance - prev_record.borrowing_balance

            margin_data.append(
                MarginTradingData(
                    date=record.date,
                    lending_balance=record.lending_balance,
                    borrowing_balance=record.borrowing_balance,
                    balance_limit=record.balance_limit,
                    lending_change=lending_change,
                    borrowing_change=borrowing_change,
                )
            )

        # 籌碼集中度摘要
        concentration_summary = None
        if concentration:
            concentration_summary = ConcentrationSummary(
                large_holders_pct=concentration.more_than_400,
                super_large_holders_pct=concentration.more_than_1000,
                retail_pct=concentration.less_than_20,
                latest_date=concentration.date,
            )

        return ChipsData(
            stock_id=stock_id,
            institutional=institutional_data,
            major=major_data,
            margin=margin_data,
            concentration=concentration_summary,
        )

    @staticmethod
    @cache_query_result(prefix="fundamental", ttl=21600, key_params=["stock_id"])
    async def get_fundamental_info(
        session: AsyncSession, stock_id: str
    ) -> Optional[FundamentalInfo]:
        """
        取得股票基本面資訊（詳細頁專用）

        Args:
            session: 資料庫 session
            stock_id: 股票代碼

        Returns:
            基本面資訊或 None
        """
        # 查詢股票基本資訊
        stock_info_query = select(StockInfo).where(StockInfo.stock_id == stock_id)
        stock_info_result = await session.execute(stock_info_query)
        stock_info = stock_info_result.scalar_one_or_none()

        if not stock_info:
            return None

        # 查詢最新股價
        stock_daily_query = (
            select(StockDaily)
            .where(StockDaily.stock_id == stock_id)
            .order_by(desc(StockDaily.date))
            .limit(1)
        )
        stock_daily_result = await session.execute(stock_daily_query)
        stock_daily = stock_daily_result.scalar_one_or_none()

        current_price = stock_daily.close if stock_daily else 0.0

        # 初始化 DatabaseManager
        from api.main import db_manager as api_db_manager

        if not api_db_manager:
            api_db_manager = DatabaseManager()

        # 載入最近 4 季 EPS
        eps_dict = await api_db_manager.bulk_load_eps([stock_id], quarters=4)
        eps_df = eps_dict.get(stock_id)

        # 準備 stock_info_dict
        stock_info_dict = {
            "per": stock_info.per,
            "cash_dividend": stock_info.cash_dividend,
            "stock_dividend": stock_info.stock_dividend,
            "capital": stock_info.capital,
            "outstanding_shares": stock_info.outstanding_shares,
        }

        # 計算基本面訊號
        fund_signals, fund_raw_score = FundamentalSignalService.calculate_signals(
            stock_info_dict, eps_df, current_price
        )
        fund_strength = FundamentalSignalService.normalize_score(fund_raw_score)

        # 計算 EPS 指標
        eps_recent_4q = []
        eps_trend = "持平"
        eps_stability = 0.0
        eps_avg = 0.0

        if eps_df is not None and not eps_df.empty:
            eps_recent_4q = eps_df["eps"].tolist()
            eps_avg = eps_df["eps"].mean()
            eps_stability = eps_df["eps"].std()

            # 判斷趨勢
            if len(eps_recent_4q) >= 2:
                if eps_recent_4q[0] > eps_recent_4q[-1]:
                    eps_trend = "上升"
                elif eps_recent_4q[0] < eps_recent_4q[-1]:
                    eps_trend = "下降"

        # 計算股利殖利率
        dividend_yield = 0.0
        if current_price > 0 and stock_info.cash_dividend:
            dividend_yield = (stock_info.cash_dividend / current_price) * 100

        # 計算配息率
        payout_ratio = 0.0
        if eps_avg > 0 and stock_info.cash_dividend:
            payout_ratio = (stock_info.cash_dividend / eps_avg) * 100

        # 🆕 查詢最新籌碼集中度資料（用於持股結構）
        concentration_query = (
            select(ConcentrationData)
            .where(ConcentrationData.stock_id == stock_id)
            .order_by(desc(ConcentrationData.date))
            .limit(1)
        )
        concentration_result = await session.execute(concentration_query)
        concentration = concentration_result.scalar_one_or_none()

        # 🆕 建立 EPSDetail 列表（用於詳細表格）
        eps_details = []
        if eps_df is not None and not eps_df.empty:
            for _, row in eps_df.iterrows():
                eps_details.append(
                    EPSDetail(
                        year=int(row["year"]),
                        quarter=int(row["quarter"]),
                        eps=float(row["eps"]),
                    )
                )

        # 🆕 建立 CapitalInfo
        capital_info = CapitalInfo(
            capital=stock_info.capital,
            outstanding_shares=stock_info.outstanding_shares,
            stock_dividend=stock_info.stock_dividend,
        )

        # 🆕 建立 HoldingInfo（如果有籌碼資料）
        holding_info = None
        if concentration:
            holding_info = HoldingInfo(
                director_ratio=concentration.director_ratio,
                foreign_holding_rate=concentration.rate_of_foreign_holding,
                investment_trust_holding_rate=concentration.rate_of_ing_holding,
                dealer_holding_rate=concentration.rate_of_dealer_holding,
                latest_date=concentration.date,
            )

        # 🆕 載入月營收資料
        revenue_dict = await api_db_manager.bulk_load_monthly_revenue(
            [stock_id], months=12
        )
        revenue_df = revenue_dict.get(stock_id)

        # 計算營收指標
        revenue_recent_12m = []
        revenue_yoy_avg = 0.0
        revenue_trend = "持平"
        revenue_details = []

        if revenue_df is not None and not revenue_df.empty:
            # 營收列表（千元）
            revenue_recent_12m = revenue_df["revenue"].tolist()

            # 計算平均年增率
            yoy_changes = revenue_df["yoy_change"].dropna()
            if not yoy_changes.empty:
                revenue_yoy_avg = float(yoy_changes.mean())

            # 判斷營收趨勢（比較最近 3 個月和前 3 個月）
            if len(revenue_recent_12m) >= 6:
                recent_avg = sum(revenue_recent_12m[:3]) / 3
                previous_avg = sum(revenue_recent_12m[3:6]) / 3
                if recent_avg > previous_avg * 1.05:  # 增長超過 5%
                    revenue_trend = "上升"
                elif recent_avg < previous_avg * 0.95:  # 下降超過 5%
                    revenue_trend = "下降"

            # 建立詳細資料列表
            for _, row in revenue_df.iterrows():
                revenue_details.append(
                    MonthlyRevenueDetail(
                        year=int(row["year"]),
                        month=int(row["month"]),
                        revenue=float(row["revenue"]),
                        mom_change=float(row["mom_change"])
                        if row["mom_change"] is not None
                        else None,
                        yoy_change=float(row["yoy_change"])
                        if row["yoy_change"] is not None
                        else None,
                        cumulative_revenue=float(row["cumulative_revenue"])
                        if row["cumulative_revenue"] is not None
                        else None,
                        cumulative_yoy_change=float(row["cumulative_yoy_change"])
                        if row["cumulative_yoy_change"] is not None
                        else None,
                    )
                )

        return FundamentalInfo(
            eps_recent_4q=eps_recent_4q,
            eps_trend=eps_trend,
            eps_stability=eps_stability,
            eps_avg=eps_avg,
            per=stock_info.per,
            dividend_yield=dividend_yield,
            payout_ratio=payout_ratio,
            cash_dividend=stock_info.cash_dividend or 0.0,
            fundamental_signals=[
                ChipSignal(
                    name=signal["name"],
                    triggered=signal["triggered"],
                    score=signal["score"],
                    description=signal.get("description"),
                )
                for signal in fund_signals
            ],
            fundamental_strength=fund_strength,
            # 🆕 營收成長
            revenue_recent_12m=revenue_recent_12m,
            revenue_yoy_avg=revenue_yoy_avg,
            revenue_trend=revenue_trend,
            # 🆕 詳細數據
            eps_details=eps_details,
            capital_info=capital_info,
            holding_info=holding_info,
            revenue_details=revenue_details,
        )
