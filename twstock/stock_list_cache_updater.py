"""股票列表快取更新器

在 main.py 完成所有分析後，將三維分數（籌碼/技術/基本面）預計算並寫入
stock_list_cache 表，讓 API /api/stocks 只需 SQL SELECT + 動態加權排序分頁。
"""

import json
import logging
from datetime import datetime

from sqlalchemy import text

from api.services.fundamental_signal_service import FundamentalSignalService
from api.services.signal_service import SignalService
from api.services.stock_service import StockService
from twstock.vectorized_signals import (
    CROSSOVER_DISPLAY_DAYS,
    SIGNAL_TYPE_CROSSOVER,
    SIGNAL_TYPE_STATE,
)

logger = logging.getLogger(__name__)


class StockListCacheUpdater:

    def __init__(self, db_manager):
        self.db = db_manager

    async def update_cache(self) -> dict:
        start = datetime.now()

        # 1. 取得 active 股票 + 名稱 + 最新價格
        stocks = await self._load_stock_basics()
        if not stocks:
            return {"count": 0, "elapsed": 0}

        stock_ids = [s["stock_id"] for s in stocks]
        stock_map = {s["stock_id"]: s for s in stocks}

        # 2. 載入各維度所需資料
        concentration_map = await self.db.bulk_load_concentration_data(stock_ids, weeks=10)
        tech_signals_map = await self._load_technical_signals(stock_ids)
        bulk_info = await self.db.bulk_load_stock_info(stock_ids)
        bulk_eps = await self.db.bulk_load_eps(stock_ids, quarters=4)
        alert_map = await self.db.bulk_load_alert_status(stock_ids)
        cb_map = await self.db.get_cb_signals_by_underlying()

        # 3. 逐股計算
        records = []
        now = datetime.now()
        # 使用最新交易日作為基準（而非 datetime.now()），確保不同天執行結果一致
        latest_trading_date = max(
            (s["last_date"].date() if isinstance(s["last_date"], datetime) else s["last_date"]
             for s in stocks if s["last_date"]),
            default=None,
        )
        today = latest_trading_date or now.date()

        for sid in stock_ids:
            s = stock_map[sid]

            # -- 籌碼 --
            chip_raw, chip_norm, chip_sigs_json = 0, 0, "[]"
            exp_ret, win_rt = 0.0, 0.0
            conc_df = concentration_map.get(sid)
            if conc_df is not None and len(conc_df) >= 2:
                conc_objects = self._df_to_concentration_objects(conc_df)
                chip_signals, chip_raw, exp_ret, win_rt = SignalService.calculate_signals(conc_objects)
                chip_norm = SignalService.normalize_score(chip_raw)
                chip_sigs_json = json.dumps(
                    [{"name": sig.name, "triggered": sig.triggered, "score": sig.score} for sig in chip_signals],
                    ensure_ascii=False,
                )

            # -- 技術 --
            tech_raw, tech_norm, tech_sigs_json = 0, 0, "[]"
            tech_data = tech_signals_map.get(sid)
            if tech_data:
                tech_raw, tech_norm, tech_sigs = self._process_tech_signals(tech_data, today)
                tech_sigs_json = json.dumps(tech_sigs, ensure_ascii=False)

            # -- 基本面 --
            fund_raw, fund_norm, fund_sigs_json = 0, 0, "[]"
            if sid in bulk_info:
                fund_sigs, fund_raw = FundamentalSignalService.calculate_signals(
                    bulk_info[sid], bulk_eps.get(sid), s["close_price"] or 0
                )
                fund_norm = FundamentalSignalService.normalize_score(fund_raw)
                fund_sigs_json = json.dumps(
                    [fs if isinstance(fs, dict) else {"name": fs.name, "triggered": fs.triggered, "score": fs.score} for fs in fund_sigs],
                    ensure_ascii=False,
                )

            # -- 聚合 --
            chip_count = len(json.loads(chip_sigs_json))
            tech_count = len(json.loads(tech_sigs_json))
            fund_count = len(json.loads(fund_sigs_json))
            signal_count = chip_count + tech_count + fund_count

            # 用預設權重算 overall 來決定 risk_level
            overall = int(chip_norm * 0.4 + tech_norm * 0.3 + fund_norm * 0.3)
            if overall >= 70:
                risk_level = "低"
            elif overall >= 40:
                risk_level = "中"
            else:
                risk_level = "高"

            # -- 警示 --
            alert_json = None
            if sid in alert_map:
                alert_json = json.dumps(alert_map[sid], ensure_ascii=False, default=str)

            records.append({
                "stock_id": sid,
                "name": s["name"],
                "close_price": s["close_price"],
                "last_date": s["last_date"],
                "chip_raw_score": chip_raw,
                "chip_normalized": chip_norm,
                "chip_signals_json": chip_sigs_json,
                "expected_return": exp_ret,
                "win_rate": win_rt,
                "tech_raw_score": tech_raw,
                "tech_normalized": tech_norm,
                "tech_signals_json": tech_sigs_json,
                "fund_raw_score": fund_raw,
                "fund_normalized": fund_norm,
                "fund_signals_json": fund_sigs_json,
                "signal_count": signal_count,
                "risk_level": risk_level,
                "alert_status_json": alert_json,
                "cb_arbitrage_score": cb_map[sid]["score"] if sid in cb_map else None,
                "cb_signals_json": self._serialize_cb_signals(cb_map.get(sid)),
                "updated_at": now,
            })

        # 4. 批次寫入
        await self.db.save_stock_list_cache(records)

        elapsed = (datetime.now() - start).total_seconds()
        return {"count": len(records), "elapsed": elapsed}

    async def _load_stock_basics(self) -> list[dict]:
        """查 active 股票 + 名稱 + 最新收盤價"""
        async with self.db.get_session() as session:
            result = await session.execute(text("""
                SELECT sl.stock_id, sl.name,
                       d.close AS close_price, d.date AS last_date
                FROM stock_list sl
                LEFT JOIN LATERAL (
                    SELECT close, date FROM stock_daily
                    WHERE stock_id = sl.stock_id
                    ORDER BY date DESC LIMIT 1
                ) d ON true
                WHERE sl.is_active = true
                  AND sl.stock_id NOT LIKE '^%%'
                  AND sl.stock_id != '0000'
                  AND sl.stock_id != '000-'
                ORDER BY sl.stock_id
            """))
            return [
                {
                    "stock_id": r.stock_id,
                    "name": r.name,
                    "close_price": float(r.close_price) if r.close_price else 0.0,
                    "last_date": r.last_date,
                }
                for r in result.fetchall()
            ]

    async def _load_technical_signals(self, stock_ids: list[str]) -> dict:
        """從 stock_technical_signals 批次讀取"""
        async with self.db.get_session() as session:
            result = await session.execute(
                text("""
                    SELECT stock_id, raw_score, normalized_score, signals_json, signals_detail
                    FROM stock_technical_signals
                    WHERE stock_id = ANY(:ids)
                """),
                {"ids": stock_ids},
            )
            return {
                r.stock_id: {
                    "raw_score": r.raw_score,
                    "normalized_score": r.normalized_score,
                    "signals_json": r.signals_json,
                    "signals_detail": r.signals_detail,
                }
                for r in result.fetchall()
            }

    def _process_tech_signals(self, tech_data: dict, today) -> tuple:
        """處理技術訊號，含時效性過濾（同 stock_service.py:424-461 邏輯）"""
        signals_detail = tech_data.get("signals_detail")

        if signals_detail:
            valid_signals = []
            valid_raw_score = 0

            for sig in signals_detail:
                trigger_date = datetime.fromisoformat(sig["trigger_date"]).date()
                signal_type = sig.get("type", SIGNAL_TYPE_STATE)

                is_valid = False
                if signal_type == SIGNAL_TYPE_CROSSOVER:
                    is_valid = (today - trigger_date).days <= CROSSOVER_DISPLAY_DAYS
                else:
                    is_valid = sig.get("is_valid", False)

                if is_valid:
                    valid_signals.append(sig)
                    valid_raw_score += sig["score"]

            tech_norm = StockService.normalize_technical_score(valid_raw_score)
            sigs = [
                {"name": s["name"], "triggered": True, "score": s["score"], "is_event": s.get("is_event", False)}
                for s in valid_signals
            ]
            return valid_raw_score, tech_norm, sigs

        # fallback: signals_json
        raw = tech_data.get("raw_score", 0)
        norm = StockService.normalize_technical_score(raw)
        sigs_json = tech_data.get("signals_json", "[]")
        try:
            parsed = json.loads(sigs_json) if isinstance(sigs_json, str) else (sigs_json or [])
            sigs = [{"name": s.get("signal_name", s.get("name", "")), "triggered": True, "score": s.get("score", 0)} for s in parsed]
        except (json.JSONDecodeError, TypeError):
            sigs = []
        return raw, norm, sigs

    @staticmethod
    def _serialize_cb_signals(cb_data: dict | None) -> str | None:
        """將 CB 訊號資料序列化為 JSON 字串（JSONB → String）"""
        if not cb_data or not cb_data.get("signals_json"):
            return None
        raw = cb_data["signals_json"]
        if isinstance(raw, str):
            return raw
        return json.dumps(raw, ensure_ascii=False)

    @staticmethod
    def _df_to_concentration_objects(df) -> list:
        """將 DataFrame 轉換為 SignalService.calculate_signals 期望的物件列表"""
        objects = []
        for _, row in df.iterrows():
            obj = type("ConcentrationData", (), {
                "stock_id": row.get("stock_id", ""),
                "date": row.get("date"),
                "more_than_400": row.get("moreThan400", 0),
                "more_than_1000": row.get("moreThan1000", 0),
                "less_than_20": row.get("lessThan20", 0),
                "close": row.get("close", 0),
                "director_ratio": row.get("directorRatio", 0),
                "rate_of_foreign_holding": row.get("rateOfForeignHolding", 0),
                "rate_of_ing_holding": row.get("rateOfINGHolding", 0),
                "rate_of_dealer_holding": row.get("rateOfDealerHolding", 0),
            })()
            objects.append(obj)
        return objects
