"""可轉債套利訊號計算服務"""

import json
import logging
from datetime import datetime
from typing import Optional

logger = logging.getLogger(__name__)

# 訊號定義
SIGNALS = {
    # === 套利訊號（正向） ===
    "discount_arbitrage": {
        "name": "折價套利",
        "score": 20,
        "description": "轉換價值 > CB 市價（含交易成本）",
    },
    "low_premium": {
        "name": "低溢價率",
        "score": 15,
        "description": "溢價率 < 5% 且標的股趨勢向上",
    },
    "forced_conversion": {
        "name": "催換在即",
        "score": 15,
        "description": "標的股價 > 轉換價 × 130%",
    },
    "put_protection": {
        "name": "到期賣回保護",
        "score": 12,
        "description": "接近賣回日且賣回價 > 市價",
    },
    "deep_in_money": {
        "name": "深度價內",
        "score": 10,
        "description": "標的股價 > 轉換價 × 150%",
    },
    "below_par": {
        "name": "低於面額",
        "score": 8,
        "description": "CB 市價 < 100（面額）",
    },
    # === 標的股連動訊號 ===
    "strong_underlying": {
        "name": "標的股強勢",
        "score": 12,
        "description": "標的股 overall_strength > 70",
    },
    "weak_underlying": {
        "name": "標的股弱勢",
        "score": -15,
        "description": "標的股 overall_strength < 30",
    },
    # === 風險訊號（負向） ===
    "high_premium_risk": {
        "name": "高溢價風險",
        "score": -15,
        "description": "溢價率 > 20%",
    },
    "maturity_risk": {
        "name": "到期逼近",
        "score": -10,
        "description": "距到期日 < 90 天且溢價率 > 10%",
    },
}

# 標準化參數：原始分數 -40 ~ +92 → 0-100
RAW_SCORE_MIN = -40
RAW_SCORE_RANGE = 140  # 92 - (-40) ≈ 132, 用 140 留緩衝


class ConvertibleSignalService:
    """可轉債套利訊號計算"""

    @classmethod
    def calculate_signals(
        cls,
        cb_close: float,
        conversion_price: float,
        underlying_close: float,
        premium_rate: Optional[float],
        conversion_value: Optional[float],
        maturity_date: Optional[datetime],
        put_date: Optional[datetime],
        put_price: Optional[float],
        underlying_strength: Optional[int] = None,
        today: Optional[datetime] = None,
    ) -> dict:
        """
        計算可轉債套利訊號

        Returns:
            dict with keys: signals, raw_score, normalized_score,
                           signal_count, risk_level
        """
        if today is None:
            today = datetime.now()

        triggered = []
        raw_score = 0

        # 1. 折價套利 (+20)
        if conversion_value and cb_close:
            if conversion_value > cb_close * 1.006:
                spread = round((conversion_value / cb_close - 1) * 100 - 0.6, 2)
                triggered.append(_make_signal(
                    "discount_arbitrage",
                    f"套利空間 {spread:.1f}%，轉換價值 {conversion_value:.1f} > CB價 {cb_close:.1f}"
                ))
                raw_score += SIGNALS["discount_arbitrage"]["score"]

        # 2. 低溢價率 (+15)
        if premium_rate is not None and premium_rate < 5 and underlying_strength is not None and underlying_strength > 50:
            triggered.append(_make_signal(
                "low_premium",
                f"溢價率 {premium_rate:.1f}%，標的股強度 {underlying_strength}"
            ))
            raw_score += SIGNALS["low_premium"]["score"]

        # 3. 催換在即 (+15)
        if underlying_close and conversion_price and conversion_price > 0:
            ratio = underlying_close / conversion_price
            if ratio > 1.3:
                triggered.append(_make_signal(
                    "forced_conversion",
                    f"股價/轉換價 = {ratio:.1%}，已超過 130% 催換門檻"
                ))
                raw_score += SIGNALS["forced_conversion"]["score"]

        # 4. 到期賣回保護 (+12)
        if put_date and put_price and cb_close:
            days_to_put = (put_date - today).days
            if 0 < days_to_put < 180 and put_price > cb_close:
                put_return = round((put_price / cb_close - 1) * 100, 2)
                triggered.append(_make_signal(
                    "put_protection",
                    f"距賣回日 {days_to_put} 天，賣回報酬 {put_return:.1f}%"
                ))
                raw_score += SIGNALS["put_protection"]["score"]

        # 5. 深度價內 (+10)
        if underlying_close and conversion_price and conversion_price > 0:
            ratio = underlying_close / conversion_price
            if ratio > 1.5:
                triggered.append(_make_signal(
                    "deep_in_money",
                    f"股價/轉換價 = {ratio:.1%}，深度價內"
                ))
                raw_score += SIGNALS["deep_in_money"]["score"]

        # 6. 低於面額 (+8)
        if cb_close and cb_close < 100:
            discount = round((1 - cb_close / 100) * 100, 1)
            triggered.append(_make_signal(
                "below_par",
                f"CB 價 {cb_close:.1f}，低於面額 {discount:.1f}%"
            ))
            raw_score += SIGNALS["below_par"]["score"]

        # 7. 標的股強勢 (+12)
        if underlying_strength is not None and underlying_strength > 70:
            triggered.append(_make_signal(
                "strong_underlying",
                f"標的股強度 {underlying_strength}"
            ))
            raw_score += SIGNALS["strong_underlying"]["score"]

        # 8. 標的股弱勢 (-15)
        if underlying_strength is not None and underlying_strength < 30:
            triggered.append(_make_signal(
                "weak_underlying",
                f"標的股強度 {underlying_strength}"
            ))
            raw_score += SIGNALS["weak_underlying"]["score"]

        # 9. 高溢價風險 (-15)
        if premium_rate is not None and premium_rate > 20:
            triggered.append(_make_signal(
                "high_premium_risk",
                f"溢價率 {premium_rate:.1f}%，遠高於合理水平"
            ))
            raw_score += SIGNALS["high_premium_risk"]["score"]

        # 10. 到期逼近 (-10)
        if maturity_date:
            days_to_maturity = (maturity_date - today).days
            if 0 < days_to_maturity < 90 and premium_rate is not None and premium_rate > 10:
                triggered.append(_make_signal(
                    "maturity_risk",
                    f"距到期 {days_to_maturity} 天，溢價率仍 {premium_rate:.1f}%"
                ))
                raw_score += SIGNALS["maturity_risk"]["score"]

        normalized = _normalize_score(raw_score)
        risk_level = _get_risk_level(normalized)

        return {
            "signals": triggered,
            "raw_score": raw_score,
            "normalized_score": normalized,
            "signal_count": len(triggered),
            "risk_level": risk_level,
        }

    @classmethod
    def build_signal_record(
        cls,
        bond_id: str,
        underlying_stock_id: str,
        result: dict,
        premium_rate: Optional[float],
        conversion_value: Optional[float],
    ) -> dict:
        """將訊號計算結果轉為資料庫記錄格式"""
        return {
            "bond_id": bond_id,
            "date": datetime.now(),
            "raw_score": result["raw_score"],
            "normalized_score": result["normalized_score"],
            "signal_count": result["signal_count"],
            "risk_level": result["risk_level"],
            "signals_json": json.dumps(
                [
                    {"name": s["name"], "score": s["score"], "description": s["description"]}
                    for s in result["signals"]
                ],
                ensure_ascii=False,
            ),
            "underlying_stock_id": underlying_stock_id,
            "premium_rate": premium_rate,
            "conversion_value": conversion_value,
            "updated_at": datetime.now(),
        }

    @classmethod
    def build_stock_cb_score_map(cls, signal_records: list[dict]) -> dict[str, int]:
        """從訊號記錄建立 underlying_stock_id → max(normalized_score) 映射"""
        score_map: dict[str, int] = {}
        for rec in signal_records:
            uid = rec["underlying_stock_id"]
            score = rec["normalized_score"]
            if uid not in score_map or score > score_map[uid]:
                score_map[uid] = score
        return score_map


def _make_signal(signal_key: str, description: str) -> dict:
    sig = SIGNALS[signal_key]
    return {
        "name": sig["name"],
        "triggered": True,
        "score": sig["score"],
        "description": description,
    }


def _normalize_score(raw_score: int) -> int:
    normalized = ((raw_score - RAW_SCORE_MIN) / RAW_SCORE_RANGE) * 100
    return int(max(0, min(100, normalized)))


def _get_risk_level(normalized_score: int) -> str:
    if normalized_score >= 80:
        return "強力套利"
    elif normalized_score >= 60:
        return "套利機會"
    elif normalized_score >= 40:
        return "觀望"
    else:
        return "風險警示"
