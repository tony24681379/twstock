"""股票資料服務層"""

import asyncio
import logging
from datetime import datetime, timedelta
from typing import List, Optional, Tuple

from sqlalchemy import asc, desc, func, select
from sqlalchemy.ext.asyncio import AsyncSession

from api.models.chart import ChartDataResponse, OHLCVData
from api.models.stock import (
    BasicInfo,
    ChipsData,
    ConcentrationDataPoint,
    ConcentrationSummary,
    InstitutionalData,
    MajorInvestorData,
    MarginTradingData,
    PaginationMetadata,
    PriceInfo,
    StockDetail,
    StockHistory,
    StockListItem,
)
from api.services.indicator_service import IndicatorService
from api.services.signal_service import SignalService
from api.services.technical_signal_service import TechnicalSignalService
from twstock.database import (
    ConcentrationData,
    DatabaseManager,
    InstitutionalInvestors,
    MajorInvestors,
    MarginTrading,
    StockDaily,
    StockInfo,
    StockList,
)

logger = logging.getLogger(__name__)


async def calculate_technical_score_for_stock(
    stock_id: str, bulk_info: dict, bulk_daily: dict, all_processor
) -> tuple:
    """
    計算單一股票的技術評分（可並行執行）

    Args:
        stock_id: 股票代碼
        bulk_info: 批次股票資訊字典
        bulk_daily: 批次日線資料字典
        all_processor: All 處理器實例

    Returns:
        (stock_id, score_dict) 或 (stock_id, None) 如果失敗/無資料
    """
    if stock_id not in bulk_info or stock_id not in bulk_daily:
        # 沒有資料，返回預設值
        return stock_id, {"signals": [], "strength": 0, "raw_score": 0}

    try:
        # 計算技術指標
        _, skill_series, _, _ = await all_processor.get_stock_from_bulk_data(
            stock_id, bulk_info[stock_id], bulk_daily[stock_id]
        )

        # 計算技術訊號分數
        tech_signals, tech_raw_score = TechnicalSignalService.calculate_signals(
            skill_series
        )
        tech_strength = TechnicalSignalService.normalize_score(tech_raw_score)

        return stock_id, {
            "signals": tech_signals,
            "strength": tech_strength,
            "raw_score": tech_raw_score,
        }
    except Exception as e:
        # 如果計算失敗，使用預設值
        logger.warning(f"計算 {stock_id} 技術評分失敗: {e}")
        return stock_id, {"signals": [], "strength": 0, "raw_score": 0}


class StockService:
    """股票資料服務"""

    @staticmethod
    async def get_stock_list(
        session: AsyncSession,
        sort_by: str = "stock_id",
        order: str = "asc",
        limit: int = 100,
        offset: int = 0,
    ) -> Tuple[List[StockListItem], PaginationMetadata]:
        """
        取得股票列表

        Args:
            session: 資料庫 session
            sort_by: 排序欄位
            order: 排序方向（asc/desc）
            limit: 每頁筆數
            offset: 偏移量

        Returns:
            股票列表與分頁資訊
        """
        # 先實作簡單版本：從 stock_info 和 stock_daily 取得基本資料
        # TODO: 完整版本需要計算訊號分數

        # 計算總數（排除指數）
        # 過濾條件：排除 ^ 開頭的產業指數、0000 大盤指數、000- 等特殊指數
        from sqlalchemy import and_, not_, or_

        filter_conditions = and_(
            StockList.is_active == True,
            not_(StockList.stock_id.like("^%")),  # 排除產業指數
            StockList.stock_id != "0000",  # 排除加權指數
            StockList.stock_id != "000-",  # 排除大盤扣除台積電
        )

        count_query = (
            select(func.count()).select_from(StockList).where(filter_conditions)
        )
        total_result = await session.execute(count_query)
        total = total_result.scalar() or 0

        # 查詢股票基本資訊、名稱與最新價格
        # 使用 subquery 取得每支股票的最新價格
        from sqlalchemy import and_

        latest_price_subquery = select(
            StockDaily.stock_id,
            StockDaily.close,
            StockDaily.date,
            func.row_number()
            .over(partition_by=StockDaily.stock_id, order_by=desc(StockDaily.date))
            .label("rn"),
        ).subquery()

        # 先取得所有股票（不分頁），用於計算訊號後排序
        query = (
            select(
                StockList,
                StockInfo,
                latest_price_subquery.c.close,
                latest_price_subquery.c.date,
            )
            .join(StockInfo, StockList.stock_id == StockInfo.stock_id, isouter=True)
            .join(
                latest_price_subquery,
                and_(
                    StockList.stock_id == latest_price_subquery.c.stock_id,
                    latest_price_subquery.c.rn == 1,
                ),
                isouter=True,
            )
            .where(filter_conditions)
        )

        # 只在按 stock_id 排序時使用 SQL 排序（效能較好）
        if sort_by == "stock_id":
            query = query.order_by(
                asc(StockList.stock_id) if order == "asc" else desc(StockList.stock_id)
            )
            # 直接在 SQL 層級分頁
            query = query.limit(limit).offset(offset)

        result = await session.execute(query)
        rows = result.all()

        # 組裝回應資料（批次查詢籌碼集中度資料以計算訊號）
        stock_ids = [row[0].stock_id for row in rows]

        # 批次查詢所有股票的籌碼集中度資料（使用窗口函數限制每股最多 10 週）
        # 使用 ROW_NUMBER() 窗口函數在 SQL 層級限制數據量，避免載入過多歷史數據
        concentration_ranked = (
            select(
                ConcentrationData.stock_id,
                ConcentrationData.date,
                ConcentrationData.more_than_400,
                ConcentrationData.more_than_1000,
                ConcentrationData.less_than_20,
                ConcentrationData.close,
                ConcentrationData.director_ratio,
                ConcentrationData.rate_of_foreign_holding,
                ConcentrationData.rate_of_ing_holding,
                ConcentrationData.rate_of_dealer_holding,
                func.row_number()
                .over(
                    partition_by=ConcentrationData.stock_id,
                    order_by=desc(ConcentrationData.date),
                )
                .label("rn"),
            )
            .where(ConcentrationData.stock_id.in_(stock_ids))
            .subquery()
        )

        # 只取每支股票的前 10 週數據（在 SQL 層級過濾，減少數據傳輸）
        concentration_result = await session.execute(
            select(concentration_ranked).where(concentration_ranked.c.rn <= 10)
        )
        all_concentration_rows = concentration_result.all()

        # 按 stock_id 分組（手動重建 ConcentrationData 對象）
        concentration_by_stock = {}
        for row in all_concentration_rows:
            stock_id = row.stock_id
            if stock_id not in concentration_by_stock:
                concentration_by_stock[stock_id] = []

            # 手動創建 ConcentrationData 對象（因為子查詢返回的是 row）
            conc = type(
                "ConcentrationData",
                (),
                {
                    "stock_id": row.stock_id,
                    "date": row.date,
                    "more_than_400": row.more_than_400,
                    "more_than_1000": row.more_than_1000,
                    "less_than_20": row.less_than_20,
                    "close": row.close,
                    "director_ratio": row.director_ratio,
                    "rate_of_foreign_holding": row.rate_of_foreign_holding,
                    "rate_of_ing_holding": row.rate_of_ing_holding,
                    "rate_of_dealer_holding": row.rate_of_dealer_holding,
                },
            )()
            concentration_by_stock[stock_id].append(conc)

        # 批次計算技術指標（使用 SQL JOIN，避免 N+1 查詢）
        print(f"📊 開始批次計算技術指標（{len(stock_ids)} 支股票）...")

        # 初始化 DatabaseManager（使用 SQL JOIN 批次載入）
        from api.main import db_manager as api_db_manager

        if not api_db_manager:
            # 如果 API 的 db_manager 未初始化，創建新的
            api_db_manager = DatabaseManager()

        # 批次載入股票資料（使用單一 SQL 查詢 + LEFT JOIN）
        # bulk_load_daily_data 已使用 LEFT JOIN 整合：
        # - stock_daily
        # - institutional_investors
        # - major_investors
        # - margin_trading
        bulk_info = await api_db_manager.bulk_load_stock_info(stock_ids)
        bulk_daily = await api_db_manager.bulk_load_daily_data(
            stock_ids, days=90
        )  # 優化：只載入 90 天數據（足夠所有技術指標）

        # 初始化 All 處理器（用於批次計算技術指標）
        from twstock import All

        all_processor = All()

        # ✅ 並行計算技術訊號（核心性能優化）
        # 使用 Semaphore 限制同時處理的股票數量，避免記憶體爆炸
        semaphore = asyncio.Semaphore(100)  # 限制同時處理 100 支股票

        async def limited_calculate(stock_id):
            """使用 Semaphore 限制並發的計算函數"""
            async with semaphore:
                return await calculate_technical_score_for_stock(
                    stock_id, bulk_info, bulk_daily, all_processor
                )

        # 並行執行所有股票的技術指標計算
        results = await asyncio.gather(
            *[limited_calculate(stock_id) for stock_id in stock_ids],
            return_exceptions=True,  # 避免單一股票失敗導致整體失敗
        )

        # 建立技術評分字典
        technical_scores = {}
        for result in results:
            if isinstance(result, Exception):
                logger.error(f"計算技術評分異常: {result}")
                continue
            stock_id, score = result
            if score is not None:
                technical_scores[stock_id] = score

        print(f"   ✓ 技術指標計算完成（並行處理 {len(stock_ids)} 支股票）")

        # 組裝回應資料（整合籌碼 + 技術）
        items = []
        for stock_list, stock_info, close_price, last_date in rows:
            stock_id = stock_list.stock_id

            # === 籌碼強度（現有邏輯）===
            concentration_data = concentration_by_stock.get(stock_id, [])
            chip_signals = []
            chip_raw_score = 0
            expected_return = 0.0
            win_rate = 0.0

            if concentration_data and len(concentration_data) >= 2:
                chip_signals, chip_raw_score, expected_return, win_rate = (
                    SignalService.calculate_signals(concentration_data)
                )

            chip_strength = SignalService.normalize_score(chip_raw_score)

            # === 技術強度（新增）===
            tech_data = technical_scores.get(
                stock_id, {"signals": [], "strength": 0, "raw_score": 0}
            )
            tech_strength = tech_data["strength"]
            tech_signals = tech_data["signals"]

            # === 綜合強度（加權平均：籌碼 60% + 技術 40%）===
            overall_strength = int(chip_strength * 0.6 + tech_strength * 0.4)

            # 風險等級（基於綜合強度）
            if overall_strength >= 70:
                risk_level = "低"
            elif overall_strength >= 40:
                risk_level = "中"
            else:
                risk_level = "高"

            # 合併訊號（用於向後相容）
            all_signals = chip_signals + tech_signals
            major_signals = [s.name for s in all_signals[:3]]

            items.append(
                StockListItem(
                    stock_id=stock_id,
                    name=stock_list.name or stock_id,
                    close_price=close_price if close_price else 0.0,
                    # 三種強度
                    chip_strength=chip_strength,
                    technical_strength=tech_strength,
                    overall_strength=overall_strength,
                    # 訊號列表
                    chip_signals=chip_signals,
                    technical_signals=tech_signals,
                    signals=all_signals,  # 合併（向後相容）
                    signal_count=len(all_signals),
                    expected_return=expected_return,
                    win_rate=win_rate,
                    risk_level=risk_level,
                    major_signals=major_signals,
                    # 向後相容
                    signal_strength=overall_strength,
                    last_updated=last_date if last_date else datetime.now(),
                )
            )

        # 如果按訊號相關欄位排序，需要在記憶體中排序和分頁
        if sort_by != "stock_id":
            # 定義排序鍵
            sort_key_map = {
                "chip_strength": lambda x: x.chip_strength,
                "technical_strength": lambda x: x.technical_strength,
                "overall_strength": lambda x: x.overall_strength,
                "signal_strength": lambda x: x.signal_strength,  # 向後相容
                "expected_return": lambda x: x.expected_return,
                "win_rate": lambda x: x.win_rate,
                "signal_count": lambda x: x.signal_count,
            }

            # 排序
            reverse = order == "desc"
            items.sort(
                key=sort_key_map.get(sort_by, lambda x: x.stock_id), reverse=reverse
            )

            # 分頁
            items = items[offset : offset + limit]

        # 分頁資訊
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

        return StockDetail(
            basic_info=basic_info,
            price_info=price_info,
            chip_signals=chip_signals,
            technical_signals=[],  # 詳情頁不計算技術訊號
            expected_return=expected_return,
            win_rate=win_rate,
            concentration_summary=concentration_summary,
            # 向後相容：詳情頁顯示的訊號強度 = 籌碼強度
            signal_strength=chip_strength,
            signals=chip_signals,
        )

    @staticmethod
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

        # 組裝回應資料
        institutional_data = [
            InstitutionalData(
                date=record.date,
                foreign=record.foreign,
                investment_trust=record.investment_trust,
                dealer=record.dealer,
                sum_holding_rate=record.sum_holding_rate,
                foreign_holding_rate=record.foreign_holding_rate,
                investment_trust_holding_rate=record.investment_trust_holding_rate,
                dealer_holding_rate=record.dealer_holding_rate,
            )
            for record in institutional_records
        ]

        major_data = [
            MajorInvestorData(
                date=record.date,
                major_investors=record.major_investors,
                agent_diff=record.agent_diff,
                skp20=record.skp20,
            )
            for record in major_records
        ]

        margin_data = [
            MarginTradingData(
                date=record.date,
                lending_balance=record.lending_balance,
                borrowing_balance=record.borrowing_balance,
                balance_limit=record.balance_limit,
            )
            for record in margin_records
        ]

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
