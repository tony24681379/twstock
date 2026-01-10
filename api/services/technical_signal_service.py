"""技術指標訊號服務"""

from typing import List, Tuple

import pandas as pd

from api.models.stock import ChipSignal


class TechnicalSignalService:
    """技術指標訊號服務

    基於 SKILL_INDEX 中的 20 個技術形態訊號計算技術強度分數。
    技術訊號在 SKILL_INDEX 的索引 32-51。
    """

    # 20 個技術形態訊號定義
    # 索引是在 SKILL_INDEX 中的位置（0-based）
    # 在 skill_series 中的實際索引 = len(INDEX) + skill_index = 20 + skill_index

    SIGNALS = {
        # === 買進訊號（正分）===
        # 強勢訊號（+15分）
        "多頭排列": {
            "score": 15,
            "skill_index": 43,  # SKILL_INDEX[43]
            "description": "MA5 > MA10 > MA20，多頭排列",
        },
        # 中強訊號（+10-12分）
        "三線合一向上": {
            "score": 12,
            "skill_index": 34,
            "description": "三均線糾結後向上突破",
        },
        "四線合一向上": {
            "score": 12,
            "skill_index": 35,
            "description": "四均線糾結後向上突破",
        },
        "5 20黃金交叉": {
            "score": 10,
            "skill_index": 38,
            "description": "MA5 向上穿越 MA20",
        },
        # 轉強訊號（+8分）
        "KD向上": {
            "score": 8,
            "skill_index": 39,
            "description": "KD低檔（<20）黃金交叉",
        },
        "MACD>0": {"score": 8, "skill_index": 40, "description": "MACD 柱狀圖轉正"},
        "DMI向上": {
            "score": 8,
            "skill_index": 42,
            "description": "+DI > -DI 且 ADX 上升",
        },
        # 爆發訊號（+6分）
        "跳空向上": {
            "score": 6,
            "skill_index": 36,
            "description": "跳空缺口向上（成交量放大）",
        },
        "長紅吞噬": {
            "score": 6,
            "skill_index": 37,
            "description": "長紅 K 線吞噬前一根",
        },
        "布林通道上軌": {
            "score": 6,
            "skill_index": 41,
            "description": "價格突破布林上軌（通道窄縮）",
        },
        # 位置訊號（+5分）
        "季線以上": {"score": 5, "skill_index": 44, "description": "股價站上 MA60"},
        "近期新高": {"score": 5, "skill_index": 32, "description": "60日新高"},
        # === 賣出訊號（負分）===
        "空頭排列": {
            "score": -15,
            "skill_index": 50,
            "description": "MA5 < MA10 < MA20，空頭排列",
        },
        "三線合一向下": {
            "score": -12,
            "skill_index": 45,
            "description": "三均線糾結後向下破位",
        },
        "四線合一向下": {
            "score": -12,
            "skill_index": 46,
            "description": "四均線糾結後向下破位",
        },
        "5 20死亡交叉": {
            "score": -10,
            "skill_index": 49,
            "description": "MA5 向下跌破 MA20",
        },
        "跳空向下": {"score": -6, "skill_index": 47, "description": "跳空缺口向下"},
        "長黑吞噬": {
            "score": -6,
            "skill_index": 48,
            "description": "長黑 K 線吞噬前一根",
        },
        "季線以下": {"score": -5, "skill_index": 51, "description": "股價跌破 MA60"},
        "近期新低": {"score": -5, "skill_index": 33, "description": "60日新低"},
    }

    @classmethod
    def calculate_signals(cls, skill_series: pd.Series) -> Tuple[List[ChipSignal], int]:
        """
        從 SKILL_INDEX 數據計算技術訊號

        Args:
            skill_series: get_stock_from_bulk_data() 返回的 skill_series
                         索引為 INDEX + SKILL_INDEX（共 20 + 54 = 74 個項目）

        Returns:
            (訊號列表, 原始總分)

        範例：
            >>> from twstock.all import INDEX, SKILL_INDEX
            >>> len(INDEX)  # 20
            >>> len(SKILL_INDEX)  # 54
            >>> # 技術訊號在 SKILL_INDEX[32:52]
            >>> # 在 skill_series 中的索引是 20+32 到 20+51
        """
        from twstock.all import INDEX

        signals = []
        total_score = 0

        # INDEX 長度（通常是 20）
        index_len = len(INDEX)

        # 遍歷每個技術訊號
        for signal_name, config in cls.SIGNALS.items():
            # 在 SKILL_INDEX 中的索引位置
            skill_idx = config["skill_index"]

            # 在 skill_series 中的實際索引 = len(INDEX) + skill_idx
            actual_idx = index_len + skill_idx

            # 檢查索引是否有效
            if actual_idx >= len(skill_series):
                continue

            # 檢查訊號是否觸發
            # 技術訊號方法返回日期字串（觸發時）或 None/False（未觸發）
            try:
                signal_value = skill_series.iloc[actual_idx]
            except (IndexError, KeyError):
                continue

            # 判斷訊號是否觸發
            if (
                signal_value is not None
                and signal_value != False
                and signal_value != ""
            ):
                signals.append(
                    ChipSignal(
                        name=signal_name,
                        triggered=True,
                        score=config["score"],
                        description=config["description"],
                    )
                )
                total_score += config["score"]

        return signals, total_score

    @classmethod
    def normalize_score(cls, raw_score: int) -> int:
        """
        標準化分數到 0-100 區間

        分數範圍：
        - 理論最高：+101（實際罕見，約 +30 到 +60）
        - 理論最低：-71（實際罕見，約 -10 到 -30）
        - 映射範圍：-30 到 +60

        Args:
            raw_score: 原始分數（可能為負）

        Returns:
            標準化後的分數（0-100）

        範例：
            >>> TechnicalSignalService.normalize_score(-30)  # 最低
            0
            >>> TechnicalSignalService.normalize_score(0)    # 中性
            33
            >>> TechnicalSignalService.normalize_score(60)   # 最高
            100
        """
        # 線性映射：(-30, 60) -> (0, 100)
        # normalized = ((raw_score + 30) / 90) * 100
        normalized = int(((raw_score + 30) / 90) * 100)

        # 限制在 0-100 範圍內
        return max(0, min(100, normalized))
        return max(0, min(100, normalized))
