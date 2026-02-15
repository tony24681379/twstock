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
        標準化技術訊號分數（-120~+164 → 0~100）

        Args:
            raw_score: 原始分數（-120~+164）

        Returns:
            標準化分數（0~100）
        """
        # 將 -120~+164 映射到 0~100
        # 公式：((raw_score + 120) / 284) * 100
        normalized = ((raw_score + 120) / 284) * 100
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
        """
        取得股票列表

        Args:
            session: 資料庫 session
            sort_by: 排序欄位
            order: 排序方向（asc/desc）
            limit: 每頁筆數
            offset: 偏移量
            chip_weight: 籌碼權重（預設 0.4）
            tech_weight: 技術權重（預設 0.3）
            fund_weight: 基本面權重（預設 0.3）

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

        # 🆕 批次取得每支股票的 DB 最新日期（用於快取判斷）
        stock_last_dates = {}
        for stock_list, stock_info, close_price, last_date in rows:
            stock_last_dates[stock_list.stock_id] = last_date or datetime.now()

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

        # 批次查詢技術訊號資料
        technical_signals_query = (
            select(StockTechnicalSignals)
            .where(StockTechnicalSignals.stock_id.in_(stock_ids))
        )
        technical_signals_result = await session.execute(technical_signals_query)
        technical_signals_rows = technical_signals_result.scalars().all()

        # 建立技術訊號字典
        technical_signals_by_stock = {}
        for signal in technical_signals_rows:
            technical_signals_by_stock[signal.stock_id] = signal

        # 批次計算技術指標（使用 SQL JOIN，避免 N+1 查詢）
        import time
        import sys
        start_time = time.time()
        print(f"📊 開始批次計算（{len(stock_ids)} 支股票）", file=sys.stderr, flush=True)

        # 初始化 DatabaseManager（使用 SQL JOIN 批次載入）
        from api.main import db_manager as api_db_manager

        if not api_db_manager:
            # 如果 API 的 db_manager 未初始化，創建新的
            api_db_manager = DatabaseManager()

        # 批次載入股票資料（使用單一 SQL 查詢）
        t1 = time.time()
        bulk_info = await api_db_manager.bulk_load_stock_info(stock_ids)
        print(f"⏱️  bulk_load_stock_info: {time.time() - t1:.2f}s", file=sys.stderr, flush=True)

        # 🆕 優化：移除不必要的 bulk_load_daily_data（close_price 已從 SQL 取得）
        # t2 = time.time()
        # bulk_daily = await api_db_manager.bulk_load_daily_data(
        #     stock_ids, days=200
        # )
        # print(f"⏱️  bulk_load_daily_data: {time.time() - t2:.2f}s", file=sys.stderr, flush=True)

        # 批次載入 EPS 資料（用於基本面分析）
        t3 = time.time()
        bulk_eps = await api_db_manager.bulk_load_eps(stock_ids, quarters=4)
        print(f"⏱️  bulk_load_eps: {time.time() - t3:.2f}s", file=sys.stderr, flush=True)

        # 初始化 All 處理器（用於批次計算技術指標）
        from twstock import All

        all_processor = All()

        # 🆕 建立 stock_id -> close_price 的映射（從 SQL 查詢結果取得）
        close_prices = {
            row[0].stock_id: row[2] if row[2] else 0.0
            for row in rows
        }

        # ✅ 並行計算技術和基本面訊號（核心性能優化）
        # 使用 Semaphore 限制同時處理的股票數量，避免記憶體爆炸
        semaphore = asyncio.Semaphore(100)  # 限制同時處理 100 支股票

        async def limited_calculate(stock_id):
            """使用 Semaphore 限制並發的計算函數"""
            async with semaphore:
                return await calculate_all_strengths_for_stock(
                    stock_id,
                    bulk_info,
                    bulk_eps,
                    close_prices.get(stock_id, 0.0),  # 🆕 傳入 close_price
                    all_processor,
                    db_last_date=stock_last_dates[stock_id],
                )

        # 並行執行所有股票的技術和基本面指標計算
        t4 = time.time()
        results = await asyncio.gather(
            *[limited_calculate(stock_id) for stock_id in stock_ids],
            return_exceptions=True,  # 避免單一股票失敗導致整體失敗
        )
        print(f"⏱️  並行計算: {time.time() - t4:.2f}s", file=sys.stderr, flush=True)

        # 建立評分字典
        all_scores = {}
        for result in results:
            if isinstance(result, Exception):
                logger.error(f"計算評分異常: {result}")
                continue
            stock_id, scores = result
            if scores is not None:
                all_scores[stock_id] = scores

        print(f"✅ 完成（{len(stock_ids)} 支股票）", file=sys.stderr, flush=True)
        print(f"⏱️  總耗時: {time.time() - start_time:.2f}s", file=sys.stderr, flush=True)

        # 🆕 批次載入警示狀態（避免 N+1 查詢）
        t_alert = time.time()
        alert_status_dict = await api_db_manager.bulk_load_alert_status(stock_ids)
        print(f"⏱️  bulk_load_alert_status: {time.time() - t_alert:.2f}s", file=sys.stderr, flush=True)

        # 組裝回應資料（整合籌碼 + 技術 + 基本面）
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

            chip_strength = chip_raw_score  # 直接使用原始分數

            # === 技術強度（從預計算表取得，含時效性過濾）===
            tech_signal_data = technical_signals_by_stock.get(stock_id)
            tech_strength = 0
            tech_signals = []

            if tech_signal_data:
                # ✅ 優先使用 signals_detail（新欄位，包含訊號時效性資訊）
                if tech_signal_data.signals_detail:
                    try:
                        # signals_detail 已被 SQLAlchemy 自動解析為 list，不需要 json.loads()
                        signals_detail = tech_signal_data.signals_detail
                        today = datetime.now().date()

                        # 🔍 過濾過期訊號（雙重保險）
                        valid_signals = []
                        valid_raw_score = 0

                        for sig in signals_detail:
                            trigger_date = datetime.fromisoformat(sig["trigger_date"]).date()
                            signal_type = sig.get("type", SIGNAL_TYPE_STATE)

                            # 判斷訊號是否有效
                            is_valid = False
                            if signal_type == SIGNAL_TYPE_CROSSOVER:
                                # 交叉訊號：檢查是否在 5 天內觸發
                                days_since = (today - trigger_date).days
                                is_valid = (days_since <= CROSSOVER_DISPLAY_DAYS)
                            else:
                                # 狀態訊號：檢查 is_valid 標記
                                is_valid = sig.get("is_valid", False)

                            if is_valid:
                                valid_signals.append(sig)
                                valid_raw_score += sig["score"]

                        # 使用過濾後的有效訊號
                        tech_strength = valid_raw_score
                        tech_signals = [
                            ChipSignal(
                                name=sig["name"],
                                triggered=True,
                                score=sig["score"],
                                description=None
                            )
                            for sig in valid_signals
                        ]

                    except (json.JSONDecodeError, KeyError, ValueError) as e:
                        logger.warning(f"解析 {stock_id} signals_detail 失敗: {e}")
                        # Fallback 到 signals_json
                        tech_strength = tech_signal_data.raw_score
                        try:
                            signals_list = json.loads(tech_signal_data.signals_json) if tech_signal_data.signals_json else []
                            tech_signals = [
                                ChipSignal(
                                    name=s["signal_name"],
                                    triggered=s["triggered"],
                                    score=s["score"],
                                    description=None
                                )
                                for s in signals_list
                            ]
                        except (json.JSONDecodeError, KeyError) as e2:
                            logger.warning(f"解析 {stock_id} signals_json fallback 也失敗: {e2}")

                else:
                    # 如果沒有 signals_detail，fallback 到舊的 signals_json（向後相容）
                    tech_strength = tech_signal_data.raw_score
                    try:
                        signals_list = json.loads(tech_signal_data.signals_json) if tech_signal_data.signals_json else []
                        tech_signals = [
                            ChipSignal(
                                name=s["signal_name"],
                                triggered=s["triggered"],
                                score=s["score"],
                                description=None
                            )
                            for s in signals_list
                        ]
                    except (json.JSONDecodeError, KeyError) as e:
                        logger.warning(f"解析 {stock_id} 技術訊號 JSON 失敗: {e}")

            # === 基本面強度（從並行計算結果取得）===
            scores = all_scores.get(
                stock_id,
                {
                    "fundamental": {"signals": [], "strength": 0, "raw_score": 0},
                },
            )
            fund_strength = scores["fundamental"]["raw_score"]  # 使用原始分數
            # 🔧 從快取恢復時，將字典轉換回 ChipSignal 對象
            fund_signals = [
                ChipSignal(**s) if isinstance(s, dict) else s
                for s in scores["fundamental"]["signals"]
            ]

            # === 綜合強度（使用自訂權重，三維：籌碼 + 技術 + 基本面）===
            # 先標準化各項分數，再計算加權平均
            chip_normalized = SignalService.normalize_score(chip_strength)
            tech_normalized = StockService.normalize_technical_score(tech_strength)
            fund_normalized = FundamentalSignalService.normalize_score(fund_strength)

            overall_strength = int(
                chip_normalized * chip_weight +
                tech_normalized * tech_weight +
                fund_normalized * fund_weight
            )

            # 風險等級（基於綜合強度）
            if overall_strength >= 70:
                risk_level = "低"
            elif overall_strength >= 40:
                risk_level = "中"
            else:
                risk_level = "高"

            # 合併訊號（用於向後相容）
            all_signals = chip_signals + tech_signals + fund_signals
            major_signals = [s.name for s in all_signals[:3]]

            # 🆕 警示狀態
            alert_status = None
            if stock_id in alert_status_dict:
                from api.models.stock import AlertStatus
                alert_data = alert_status_dict[stock_id]
                alert_status = AlertStatus(**alert_data)

            items.append(
                StockListItem(
                    stock_id=stock_id,
                    name=stock_list.name or stock_id,
                    close_price=close_price if close_price else 0.0,
                    # 警示狀態（新增）
                    alert_status=alert_status,
                    # 三種強度（籌碼 + 技術 + 基本面）
                    chip_strength=chip_strength,
                    technical_strength=tech_strength,
                    fundamental_strength=fund_strength,
                    overall_strength=overall_strength,
                    # 權重資訊
                    weights={
                        "chip": chip_weight,
                        "technical": tech_weight,
                        "fundamental": fund_weight,
                    },
                    # 訊號列表
                    chip_signals=chip_signals,
                    technical_signals=tech_signals,
                    fundamental_signals=fund_signals,
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
                "fundamental_strength": lambda x: x.fundamental_strength,  # 新增
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

        # 查詢技術訊號（含時效性過濾）
        tech_signals = []
        tech_signals_query = (
            select(StockTechnicalSignals)
            .where(StockTechnicalSignals.stock_id == stock_id)
        )
        tech_signals_result = await session.execute(tech_signals_query)
        tech_signal_data = tech_signals_result.scalar_one_or_none()

        if tech_signal_data and tech_signal_data.signals_detail:
            try:
                # signals_detail 已被 SQLAlchemy 自動解析為 list，不需要 json.loads()
                signals_detail = tech_signal_data.signals_detail
                today = datetime.now().date()

                # 🔍 過濾過期訊號（雙重保險）
                for sig in signals_detail:
                    trigger_date = datetime.fromisoformat(sig["trigger_date"]).date()
                    signal_type = sig.get("type", SIGNAL_TYPE_STATE)

                    # 判斷訊號是否有效
                    is_valid = False
                    if signal_type == SIGNAL_TYPE_CROSSOVER:
                        # 交叉訊號：檢查是否在 5 天內觸發
                        days_since = (today - trigger_date).days
                        is_valid = (days_since <= CROSSOVER_DISPLAY_DAYS)
                    else:
                        # 狀態訊號：檢查 is_valid 標記
                        is_valid = sig.get("is_valid", False)

                    if is_valid:
                        tech_signals.append(
                            ChipSignal(
                                name=sig["name"],
                                triggered=True,
                                score=sig["score"],
                                description=None
                            )
                        )

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
            alert_status=alert_status,  # 🆕 警示狀態
            chip_signals=chip_signals,
            technical_signals=tech_signals,  # ✅ 返回過濾後的技術訊號
            expected_return=expected_return,
            win_rate=win_rate,
            concentration_summary=concentration_summary,
            # 向後相容：詳情頁顯示的訊號強度 = 籌碼強度
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
