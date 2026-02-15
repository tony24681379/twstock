"""
訊號預計算更新器

管理技術指標和訊號的增量更新，將計算結果儲存到 PostgreSQL 預計算表，
實現快速查詢（< 10ms）。

Performance:
- 首次執行: 14 秒（2700 支股票）
- 後續執行: 2 秒（增量更新）
- API 查詢: < 10ms（查詢預計算表）
"""

import json
from datetime import datetime, timedelta, date
from typing import Dict, List, Optional

import pandas as pd
from sqlalchemy import text, select, and_, or_

try:
    from .database import DatabaseManager, StockSignalHistory, StockTechnicalSignals
    from .vectorized_indicators import VectorizedIndicatorEngine
    from .vectorized_signals import (
        VectorizedSignalDetector,
        SIGNAL_TYPE_CROSSOVER,
        SIGNAL_TYPE_STATE,
        CROSSOVER_DISPLAY_DAYS,
    )
except ImportError:
    from database import DatabaseManager, StockSignalHistory, StockTechnicalSignals
    from vectorized_indicators import VectorizedIndicatorEngine
    from vectorized_signals import (
        VectorizedSignalDetector,
        SIGNAL_TYPE_CROSSOVER,
        SIGNAL_TYPE_STATE,
        CROSSOVER_DISPLAY_DAYS,
    )


class SignalUpdater:
    """訊號預計算更新器

    更新策略:
    1. 增量更新: 只計算有新資料的股票
    2. 批次寫入: 單一 SQL transaction
    3. 智能快取: 檢查 API 最新日期
    """

    def __init__(self, db_manager: DatabaseManager, batch_size: int = 500):
        """
        初始化更新器

        Args:
            db_manager: 資料庫管理器
            batch_size: 分批處理大小（預設 500 支股票/批）
        """
        self.db = db_manager
        self.batch_size = batch_size
        self.indicator_engine = VectorizedIndicatorEngine()
        self.signal_detector = VectorizedSignalDetector()

    async def update_signals(
        self, stock_ids: Optional[List[str]] = None, force_update: bool = False
    ) -> Dict[str, any]:
        """更新技術訊號（增量）

        Args:
            stock_ids: 要更新的股票列表（None = 全部）
            force_update: 強制更新所有股票（忽略增量檢查）

        Returns:
            Dict: {
                'updated_count': int,
                'skipped_count': int,
                'error_count': int,
                'elapsed_time': float
            }

        Example:
            >>> updater = SignalUpdater(db_manager)
            >>> result = await updater.update_signals(['2330', '2454'])
            >>> print(f"更新了 {result['updated_count']} 支股票")
        """
        start_time = datetime.now()

        # Step 1: 檢查哪些股票需要更新
        if force_update or stock_ids is None:
            # 強制更新或全部更新
            needs_update = await self._get_all_stock_ids()
            if stock_ids:
                needs_update = [sid for sid in needs_update if sid in stock_ids]
        else:
            needs_update = await self._check_needs_update(stock_ids)

        if not needs_update:
            return {
                "updated_count": 0,
                "skipped_count": len(stock_ids) if stock_ids else 0,
                "error_count": 0,
                "elapsed_time": 0.0,
            }

        print(f"需要更新 {len(needs_update)} 支股票的技術訊號")

        # Step 2: 分批處理（避免記憶體溢位）
        updated_count = 0
        error_count = 0

        for i in range(0, len(needs_update), self.batch_size):
            batch = needs_update[i : i + self.batch_size]
            print(
                f"處理批次 {i // self.batch_size + 1}/{(len(needs_update) + self.batch_size - 1) // self.batch_size} ({len(batch)} 支股票)"
            )

            try:
                # 批次處理這一批股票
                batch_result = await self._process_batch(batch)
                updated_count += batch_result["updated"]
                error_count += batch_result["errors"]
            except Exception as e:
                print(f"批次處理失敗: {e}")
                error_count += len(batch)

        elapsed_time = (datetime.now() - start_time).total_seconds()

        return {
            "updated_count": updated_count,
            "skipped_count": len(stock_ids) - updated_count if stock_ids else 0,
            "error_count": error_count,
            "elapsed_time": elapsed_time,
        }

    async def _process_batch(self, stock_ids: List[str]) -> Dict[str, int]:
        """處理單一批次的股票（支援訊號時效性管理）

        新架構：
        1. 載入歷史訊號記錄
        2. 檢測交叉訊號（5天過期）
        3. 驗證狀態訊號（持續驗證）
        4. 儲存訊號歷史
        5. 更新快取表

        Args:
            stock_ids: 這一批要處理的股票列表

        Returns:
            Dict: {'updated': int, 'errors': int}
        """
        try:
            # Step 1: 批次載入資料（150 曆日 ≈ 103 交易日，足夠計算所有指標含 MA60）
            bulk_data = await self.db.bulk_load_daily_data(stock_ids, days=150)

            if not bulk_data:
                return {"updated": 0, "errors": len(stock_ids)}

            # Step 2: 合併為單一 DataFrame
            all_data = pd.concat(
                [
                    df.assign(stock_id=sid)
                    for sid, df in bulk_data.items()
                    if not df.empty
                ],
                ignore_index=True,
            )

            if all_data.empty:
                return {"updated": 0, "errors": len(stock_ids)}

            # Step 3: 向量化計算技術指標
            all_data = self.indicator_engine.calculate_all_indicators(all_data)

            # Step 4: 載入現有訊號歷史（用於去重和狀態驗證）
            # 使用 60 天確保涵蓋所有可能的歷史訊號
            history_df = await self._load_signal_history(stock_ids, days=60)

            # Step 5: 檢測交叉訊號（新觸發的）
            new_crossover_signals = self.signal_detector.detect_crossover_signals(
                all_data, history_df
            )

            # Step 6: 驗證狀態訊號（更新有效性）
            validated_state_signals = self.signal_detector.validate_state_signals(
                all_data, history_df
            )

            # Step 7: 儲存訊號歷史（交叉+狀態）
            await self._save_signal_history(
                new_crossover_signals, validated_state_signals
            )

            # Step 8: 更新快取表（過濾過期訊號）
            await self._update_signal_cache(stock_ids)

            # Step 9: 批次寫入技術指標（保留原邏輯）
            await self._batch_save_indicators(all_data)

            return {"updated": len(stock_ids), "errors": 0}

        except Exception as e:
            print(f"批次處理錯誤: {e}")
            import traceback
            traceback.print_exc()
            return {"updated": 0, "errors": len(stock_ids)}

    async def _check_needs_update(self, stock_ids: List[str]) -> List[str]:
        """檢查哪些股票需要更新（增量更新）

        比較資料庫中的技術訊號更新時間與每日資料最新時間。

        Args:
            stock_ids: 要檢查的股票列表

        Returns:
            需要更新的股票列表
        """
        stock_ids = [sid.lower() for sid in stock_ids]

        async with self.db.get_session() as session:
            result = await session.execute(
                text(
                    """
                    SELECT
                        d.stock_id,
                        MAX(d.date) as latest_daily_date,
                        s.updated_at as signal_updated_at
                    FROM stock_daily d
                    LEFT JOIN stock_technical_signals s ON d.stock_id = s.stock_id
                    WHERE d.stock_id = ANY(:stock_ids)
                    GROUP BY d.stock_id, s.updated_at
                """
                ),
                {"stock_ids": stock_ids},
            )

            needs_update = []
            for row in result:
                # 如果沒有訊號記錄，或每日資料比訊號新，則需要更新
                if row.signal_updated_at is None:
                    needs_update.append(row.stock_id)
                elif row.latest_daily_date and row.signal_updated_at:
                    # 轉換為可比較的日期
                    if isinstance(row.latest_daily_date, str):
                        latest_daily = datetime.fromisoformat(
                            row.latest_daily_date
                        ).date()
                    else:
                        latest_daily = (
                            row.latest_daily_date.date()
                            if hasattr(row.latest_daily_date, "date")
                            else row.latest_daily_date
                        )

                    signal_updated = (
                        row.signal_updated_at.date()
                        if hasattr(row.signal_updated_at, "date")
                        else row.signal_updated_at
                    )

                    if latest_daily > signal_updated:
                        needs_update.append(row.stock_id)

            return needs_update

    async def _get_all_stock_ids(self) -> List[str]:
        """獲取所有啟用的股票代碼

        Returns:
            股票代碼列表
        """
        async with self.db.get_session() as session:
            result = await session.execute(
                text(
                    """
                    SELECT stock_id
                    FROM stock_list
                    WHERE is_active = true
                    ORDER BY stock_id
                """
                )
            )

            return [row.stock_id for row in result]

    async def _batch_save_indicators(self, indicators_df: pd.DataFrame) -> None:
        """批次儲存技術指標到資料庫

        使用 INSERT ... ON CONFLICT DO UPDATE 確保冪等性。

        Args:
            indicators_df: 包含技術指標的 DataFrame
        """
        # 只保留最新的每支股票資料
        latest_indicators = indicators_df.groupby("stock_id").last().reset_index()

        # 準備批次插入的資料
        records = []
        for _, row in latest_indicators.iterrows():
            records.append(
                {
                    "stock_id": row["stock_id"],
                    "date": row["date"],
                    "ma5": (
                        float(row.get("ma5", 0)) if pd.notna(row.get("ma5")) else None
                    ),
                    "ma10": (
                        float(row.get("ma10", 0)) if pd.notna(row.get("ma10")) else None
                    ),
                    "ma20": (
                        float(row.get("ma20", 0)) if pd.notna(row.get("ma20")) else None
                    ),
                    "ma60": (
                        float(row.get("ma60", 0)) if pd.notna(row.get("ma60")) else None
                    ),
                    "macd": (
                        float(row.get("macd", 0)) if pd.notna(row.get("macd")) else None
                    ),
                    "macd_signal": (
                        float(row.get("macd_signal", 0))
                        if pd.notna(row.get("macd_signal"))
                        else None
                    ),
                    "macd_hist": (
                        float(row.get("macd_hist", 0))
                        if pd.notna(row.get("macd_hist"))
                        else None
                    ),
                    "k9": float(row.get("k9", 0)) if pd.notna(row.get("k9")) else None,
                    "d9": float(row.get("d9", 0)) if pd.notna(row.get("d9")) else None,
                    "rsi": (
                        float(row.get("rsi", 0)) if pd.notna(row.get("rsi")) else None
                    ),
                    "adx": (
                        float(row.get("adx", 0)) if pd.notna(row.get("adx")) else None
                    ),
                    "bollinger_upper": (
                        float(row.get("bollinger_upper", 0))
                        if pd.notna(row.get("bollinger_upper"))
                        else None
                    ),
                    "bollinger_middle": (
                        float(row.get("bollinger_middle", 0))
                        if pd.notna(row.get("bollinger_middle"))
                        else None
                    ),
                    "bollinger_lower": (
                        float(row.get("bollinger_lower", 0))
                        if pd.notna(row.get("bollinger_lower"))
                        else None
                    ),
                    "updated_at": datetime.now(),
                }
            )

        if not records:
            return

        # 批次插入（使用 ON CONFLICT UPDATE）
        async with self.db.get_session() as session:
            await session.execute(
                text(
                    """
                    INSERT INTO stock_technical_indicators
                    (stock_id, date, ma5, ma10, ma20, ma60, macd, macd_signal, macd_hist,
                     k9, d9, rsi, adx, bollinger_upper, bollinger_middle, bollinger_lower, updated_at)
                    VALUES
                    (:stock_id, :date, :ma5, :ma10, :ma20, :ma60, :macd, :macd_signal, :macd_hist,
                     :k9, :d9, :rsi, :adx, :bollinger_upper, :bollinger_middle, :bollinger_lower, :updated_at)
                    ON CONFLICT (stock_id, date)
                    DO UPDATE SET
                        ma5 = EXCLUDED.ma5,
                        ma10 = EXCLUDED.ma10,
                        ma20 = EXCLUDED.ma20,
                        ma60 = EXCLUDED.ma60,
                        macd = EXCLUDED.macd,
                        macd_signal = EXCLUDED.macd_signal,
                        macd_hist = EXCLUDED.macd_hist,
                        k9 = EXCLUDED.k9,
                        d9 = EXCLUDED.d9,
                        rsi = EXCLUDED.rsi,
                        adx = EXCLUDED.adx,
                        bollinger_upper = EXCLUDED.bollinger_upper,
                        bollinger_middle = EXCLUDED.bollinger_middle,
                        bollinger_lower = EXCLUDED.bollinger_lower,
                        updated_at = EXCLUDED.updated_at
                """
                ),
                records,
            )

        print(f"已儲存 {len(records)} 筆技術指標記錄")

    async def _load_signal_history(
        self, stock_ids: List[str], days: int = 30
    ) -> pd.DataFrame:
        """載入最近的訊號歷史

        Args:
            stock_ids: 股票代碼列表
            days: 載入最近N天的歷史（預設30天）

        Returns:
            訊號歷史 DataFrame
            Columns: [stock_id, signal_name, signal_type, trigger_date,
                     last_valid_date, score]
        """
        cutoff_date = datetime.now() - timedelta(days=days)

        async with self.db.get_session() as session:
            result = await session.execute(
                select(StockSignalHistory).where(
                    and_(
                        StockSignalHistory.stock_id.in_(stock_ids),
                        StockSignalHistory.trigger_date >= cutoff_date,
                    )
                )
            )
            records = result.scalars().all()

            if not records:
                return pd.DataFrame()

            return pd.DataFrame(
                [
                    {
                        "stock_id": r.stock_id,
                        "signal_name": r.signal_name,
                        "signal_type": r.signal_type,
                        "trigger_date": r.trigger_date,
                        "last_valid_date": r.last_valid_date,
                        "score": r.score,
                    }
                    for r in records
                ]
            )

    async def _save_signal_history(
        self, crossover_df: pd.DataFrame, state_df: pd.DataFrame
    ) -> None:
        """儲存訊號歷史（交叉訊號插入新記錄，狀態訊號更新 last_valid_date）

        Args:
            crossover_df: 新的交叉訊號 DataFrame
            state_df: 狀態訊號驗證結果 DataFrame
        """
        async with self.db.get_session() as session:
            # 使用 no_autoflush 避免查詢時提前 flush 導致重複插入
            with session.no_autoflush:
                # 插入新的交叉訊號
                if not crossover_df.empty:
                    # 批次查詢已存在的記錄（避免重複插入）
                    conditions = [
                        and_(
                            StockSignalHistory.stock_id == row["stock_id"],
                            StockSignalHistory.signal_name == row["signal_name"],
                            StockSignalHistory.trigger_date == row["trigger_date"],
                        )
                        for _, row in crossover_df.iterrows()
                    ]
                    result = await session.execute(
                        select(
                            StockSignalHistory.stock_id,
                            StockSignalHistory.signal_name,
                            StockSignalHistory.trigger_date,
                        ).where(or_(*conditions))
                    )
                    existing_keys = {
                        (row.stock_id, row.signal_name, row.trigger_date)
                        for row in result.all()
                    }

                # 只插入不存在的記錄
                new_count = 0
                for _, row in crossover_df.iterrows():
                    key = (row["stock_id"], row["signal_name"], row["trigger_date"])
                    if key not in existing_keys:
                        signal = StockSignalHistory(
                            stock_id=row["stock_id"],
                            signal_name=row["signal_name"],
                            signal_type=row["signal_type"],
                            trigger_date=row["trigger_date"],
                            last_valid_date=row["trigger_date"],
                            score=row["score"],
                        )
                        session.add(signal)
                        new_count += 1
                    print(f"新增 {new_count} 個交叉訊號到歷史表（跳過 {len(crossover_df) - new_count} 個重複記錄）")

                # 更新或插入狀態訊號
                if not state_df.empty:
                    updated_count = 0
                    inserted_count = 0

                    for _, row in state_df.iterrows():
                        # 查詢現有記錄
                        result = await session.execute(
                            select(StockSignalHistory).where(
                                and_(
                                    StockSignalHistory.stock_id == row["stock_id"],
                                    StockSignalHistory.signal_name == row["signal_name"],
                                    StockSignalHistory.trigger_date == row["trigger_date"],
                                )
                            )
                        )
                        existing = result.scalar_one_or_none()

                        if row["is_valid"]:
                            if existing:
                                # 更新 last_valid_date
                                existing.last_valid_date = row["last_valid_date"]
                                existing.updated_at = datetime.now()
                                updated_count += 1
                            else:
                                # 新狀態訊號
                                signal = StockSignalHistory(
                                    stock_id=row["stock_id"],
                                    signal_name=row["signal_name"],
                                    signal_type=row["signal_type"],
                                    trigger_date=row["trigger_date"],
                                    last_valid_date=row["last_valid_date"],
                                    score=row["score"],
                                )
                                session.add(signal)
                                inserted_count += 1
                        else:
                            # 標記失效（設定 last_valid_date = NULL）
                            if existing:
                                existing.last_valid_date = None
                                existing.updated_at = datetime.now()
                                updated_count += 1

                    print(
                        f"狀態訊號：新增 {inserted_count} 個，更新 {updated_count} 個"
                    )

            await session.commit()

    async def _update_signal_cache(self, stock_ids: List[str]) -> None:
        """更新快取表（stock_technical_signals）用於快速查詢

        只包含有效訊號：
        - 交叉訊號：5天內觸發
        - 狀態訊號：last_valid_date 不為空

        Args:
            stock_ids: 需要更新的股票列表
        """
        today = datetime.now().date()
        cutoff_date_crossover = today - timedelta(days=CROSSOVER_DISPLAY_DAYS)

        async with self.db.get_session() as session:
            for stock_id in stock_ids:
                # 查詢該股票的有效訊號
                result = await session.execute(
                    select(StockSignalHistory).where(
                        and_(
                            StockSignalHistory.stock_id == stock_id,
                            or_(
                                # 交叉訊號：5天內觸發
                                and_(
                                    StockSignalHistory.signal_type
                                    == SIGNAL_TYPE_CROSSOVER,
                                    StockSignalHistory.trigger_date
                                    >= cutoff_date_crossover,
                                ),
                                # 狀態訊號：last_valid_date 不為空
                                and_(
                                    StockSignalHistory.signal_type == SIGNAL_TYPE_STATE,
                                    StockSignalHistory.last_valid_date.isnot(None),
                                ),
                            ),
                        )
                    )
                )
                valid_signals = result.scalars().all()

                if not valid_signals:
                    # 沒有有效訊號，刪除快取記錄
                    await session.execute(
                        text("DELETE FROM stock_technical_signals WHERE stock_id = :stock_id"),
                        {"stock_id": stock_id},
                    )
                    continue

                # 計算評分
                raw_score = sum(s.score for s in valid_signals)
                buy_signals = sum(1 for s in valid_signals if s.score > 0)
                sell_signals = sum(1 for s in valid_signals if s.score < 0)

                # 標準化分數（-120 ~ +164 → 0 ~ 100）
                normalized_score = int(((raw_score + 120) / 284) * 100)

                # 建立 signals_detail JSON
                signals_detail = []
                for s in valid_signals:
                    # 處理 trigger_date 可能是 datetime 或 date
                    trigger_date = s.trigger_date.date() if isinstance(s.trigger_date, datetime) else s.trigger_date
                    days_since = (today - trigger_date).days
                    signals_detail.append(
                        {
                            "name": s.signal_name,
                            "type": s.signal_type,
                            "score": s.score,
                            "trigger_date": s.trigger_date.isoformat(),
                            "days_since_trigger": days_since,
                            "is_valid": True,
                        }
                    )

                # 建立簡化的 signals_json（向後相容）
                signals_json = [
                    {"signal_name": s["name"], "score": s["score"], "triggered": True}
                    for s in signals_detail
                ]

                # 更新快取表
                await session.execute(
                    text(
                        """
                        INSERT INTO stock_technical_signals
                        (stock_id, date, raw_score, normalized_score, signal_count,
                         buy_signals, sell_signals, signals_json, signals_detail, updated_at)
                        VALUES
                        (:stock_id, :date, :raw_score, :normalized_score, :signal_count,
                         :buy_signals, :sell_signals, :signals_json, :signals_detail, :updated_at)
                        ON CONFLICT (stock_id)
                        DO UPDATE SET
                            date = EXCLUDED.date,
                            raw_score = EXCLUDED.raw_score,
                            normalized_score = EXCLUDED.normalized_score,
                            signal_count = EXCLUDED.signal_count,
                            buy_signals = EXCLUDED.buy_signals,
                            sell_signals = EXCLUDED.sell_signals,
                            signals_json = EXCLUDED.signals_json,
                            signals_detail = EXCLUDED.signals_detail,
                            updated_at = EXCLUDED.updated_at
                        """
                    ),
                    {
                        "stock_id": stock_id,
                        "date": today,
                        "raw_score": raw_score,
                        "normalized_score": normalized_score,
                        "signal_count": len(valid_signals),
                        "buy_signals": buy_signals,
                        "sell_signals": sell_signals,
                        "signals_json": json.dumps(signals_json, ensure_ascii=False),
                        "signals_detail": json.dumps(
                            signals_detail, ensure_ascii=False, default=str
                        ),
                        "updated_at": datetime.now(),
                    },
                )

            await session.commit()
            print(f"已更新 {len(stock_ids)} 支股票的快取表")

    async def _batch_save_signals(
        self, signals_df: pd.DataFrame, strengths_df: pd.DataFrame
    ) -> None:
        """批次儲存技術訊號到資料庫

        Args:
            signals_df: 訊號詳情 DataFrame（from detect_all_signals）
            strengths_df: 訊號強度 DataFrame（from calculate_signal_strength）
        """
        # 準備批次插入的資料
        records = []

        for _, row in strengths_df.iterrows():
            stock_id = row["stock_id"]

            # 獲取該股票的所有訊號
            stock_signals = signals_df[signals_df["stock_id"] == stock_id]

            # 轉換為 JSON 格式
            signals_json = stock_signals[
                ["signal_name", "score", "triggered", "date"]
            ].to_dict("records")

            # 取得最新日期
            latest_date = (
                stock_signals["date"].max()
                if not stock_signals.empty
                else datetime.now()
            )

            records.append(
                {
                    "stock_id": stock_id,
                    "date": latest_date,
                    "raw_score": int(row["raw_score"]),
                    "normalized_score": int(row["normalized_score"]),
                    "signal_count": int(row["signal_count"]),
                    "buy_signals": int(row["buy_signals"]),
                    "sell_signals": int(row["sell_signals"]),
                    "signals_json": json.dumps(
                        signals_json, ensure_ascii=False, default=str
                    ),
                    "updated_at": datetime.now(),
                }
            )

        if not records:
            return

        # 批次插入（使用 ON CONFLICT UPDATE）
        async with self.db.get_session() as session:
            await session.execute(
                text(
                    """
                    INSERT INTO stock_technical_signals
                    (stock_id, date, raw_score, normalized_score, signal_count,
                     buy_signals, sell_signals, signals_json, updated_at)
                    VALUES
                    (:stock_id, :date, :raw_score, :normalized_score, :signal_count,
                     :buy_signals, :sell_signals, :signals_json, :updated_at)
                    ON CONFLICT (stock_id)
                    DO UPDATE SET
                        date = EXCLUDED.date,
                        raw_score = EXCLUDED.raw_score,
                        normalized_score = EXCLUDED.normalized_score,
                        signal_count = EXCLUDED.signal_count,
                        buy_signals = EXCLUDED.buy_signals,
                        sell_signals = EXCLUDED.sell_signals,
                        signals_json = EXCLUDED.signals_json,
                        updated_at = EXCLUDED.updated_at
                """
                ),
                records,
            )

        print(f"已儲存 {len(records)} 筆技術訊號記錄")

    async def get_signal_statistics(self) -> Dict[str, any]:
        """獲取訊號統計資訊

        Returns:
            Dict: {
                'total_stocks': int,
                'updated_stocks': int,
                'average_score': float,
                'top_signals': List[Dict],  # 最常觸發的訊號
                'last_updated': datetime
            }
        """
        async with self.db.get_session() as session:
            # 總股票數
            total_result = await session.execute(
                text("SELECT COUNT(*) FROM stock_list WHERE is_active = true")
            )
            total_stocks = total_result.scalar()

            # 已更新股票數
            updated_result = await session.execute(
                text("SELECT COUNT(*) FROM stock_technical_signals")
            )
            updated_stocks = updated_result.scalar()

            # 平均分數
            avg_result = await session.execute(
                text("SELECT AVG(normalized_score) FROM stock_technical_signals")
            )
            average_score = avg_result.scalar() or 0

            # 最後更新時間
            last_updated_result = await session.execute(
                text("SELECT MAX(updated_at) FROM stock_technical_signals")
            )
            last_updated = last_updated_result.scalar()

            return {
                "total_stocks": total_stocks,
                "updated_stocks": updated_stocks,
                "average_score": round(average_score, 2),
                "last_updated": last_updated,
            }


class BatchedSignalUpdater(SignalUpdater):
    """分批訊號更新器（記憶體友善版）

    繼承自 SignalUpdater，使用更小的批次大小以節省記憶體。
    適合記憶體受限的環境。

    Memory:
    - 全量處理: 2700 stocks × 200 days × 30 cols × 8 bytes ≈ 1.3 GB
    - 分批處理: 500 stocks × 200 days × 30 cols × 8 bytes ≈ 240 MB
    """

    def __init__(self, db_manager: DatabaseManager):
        """
        初始化分批更新器（使用較小的批次大小）

        Args:
            db_manager: 資料庫管理器
        """
        super().__init__(db_manager, batch_size=500)
