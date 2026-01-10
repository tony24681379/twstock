"""籌碼集中度訊號計算服務"""

from typing import Dict, List, Tuple

from api.models.stock import ChipSignal
from twstock.database import ConcentrationData


class SignalService:
    """籌碼集中度訊號計算服務"""

    # 訊號定義與分數
    SIGNALS = {
        "perfect_structure": {
            "name": "完美結構",
            "score": 20,
            "description": "大戶↑ 超大戶↑ 散戶↓ (同週)",
        },
        "super_large_2w": {
            "name": "超大戶連買2週",
            "score": 15,
            "description": "超大戶連續2週增加",
        },
        "large_3w": {
            "name": "大戶連買3週",
            "score": 12,
            "description": "大戶連續3週增加",
        },
        "large_spike": {
            "name": "大戶急買",
            "score": 10,
            "description": "單週大戶增加>1%",
        },
        "acceleration": {
            "name": "加速集中",
            "score": 10,
            "description": "近5週變化>前5週",
        },
        "持續10w": {"name": "10週持續買", "score": 8, "description": "W1-W10變化>3%"},
        "retail_3w_sell": {
            "name": "散戶連賣3週",
            "score": 8,
            "description": "散戶連續3週減少",
        },
        "retail_panic": {
            "name": "散戶恐慌",
            "score": 6,
            "description": "單週散戶減少>1%",
        },
        "distribution": {
            "name": "出貨訊號",
            "score": -15,
            "description": "大戶↓ 散戶↑ (同週)",
        },
        "retail_fomo": {
            "name": "散戶狂熱",
            "score": -10,
            "description": "散戶連續3週增加",
        },
    }

    @classmethod
    def calculate_signals(
        cls, concentration_data: List[ConcentrationData]
    ) -> Tuple[List[ChipSignal], int, float, float]:
        """
        計算所有籌碼集中度訊號

        Args:
            concentration_data: 籌碼集中度資料列表（至少需要 10 週，從新到舊排序）

        Returns:
            (訊號列表, 總分數, 預期報酬, 勝率)
        """
        if not concentration_data or len(concentration_data) < 2:
            return [], 0, 0.0, 0.0

        # 確保資料從新到舊排序
        data = sorted(concentration_data, key=lambda x: x.date, reverse=True)

        signals = []
        total_score = 0

        # 計算每週變化
        changes = cls._calculate_weekly_changes(data)

        # 1. 完美結構 (+20)
        signal = cls._check_perfect_structure(changes)
        if signal:
            signals.append(signal)
            total_score += signal.score

        # 2. 超大戶連買2週 (+15)
        signal = cls._check_super_large_2w(changes)
        if signal:
            signals.append(signal)
            total_score += signal.score

        # 3. 大戶連買3週 (+12)
        signal = cls._check_large_3w(changes)
        if signal:
            signals.append(signal)
            total_score += signal.score

        # 4. 大戶急買 (+10)
        signal = cls._check_large_spike(changes)
        if signal:
            signals.append(signal)
            total_score += signal.score

        # 5. 加速集中 (+10)
        signal = cls._check_acceleration(changes)
        if signal:
            signals.append(signal)
            total_score += signal.score

        # 6. 10週持續買 (+8)
        signal = cls._check_10w_持續(data)
        if signal:
            signals.append(signal)
            total_score += signal.score

        # 7. 散戶連賣3週 (+8)
        signal = cls._check_retail_3w_sell(changes)
        if signal:
            signals.append(signal)
            total_score += signal.score

        # 8. 散戶恐慌 (+6)
        signal = cls._check_retail_panic(changes)
        if signal:
            signals.append(signal)
            total_score += signal.score

        # 9. 出貨訊號 (-15)
        signal = cls._check_distribution(changes)
        if signal:
            signals.append(signal)
            total_score += signal.score

        # 10. 散戶狂熱 (-10)
        signal = cls._check_retail_fomo(changes)
        if signal:
            signals.append(signal)
            total_score += signal.score

        # 計算預期報酬和勝率（基於歷史統計）
        expected_return, win_rate = cls._calculate_expected_metrics(
            signals, total_score
        )

        return signals, total_score, expected_return, win_rate

    @classmethod
    def _calculate_weekly_changes(cls, data: List[ConcentrationData]) -> List[Dict]:
        """計算每週變化"""
        changes = []
        for i in range(len(data) - 1):
            current = data[i]
            previous = data[i + 1]

            changes.append(
                {
                    "week": i,
                    "date": current.date,
                    "more_than_400_change": current.more_than_400
                    - previous.more_than_400,
                    "more_than_1000_change": current.more_than_1000
                    - previous.more_than_1000,
                    "less_than_20_change": current.less_than_20 - previous.less_than_20,
                }
            )

        return changes

    @classmethod
    def _check_perfect_structure(cls, changes: List[Dict]) -> ChipSignal | None:
        """檢查完美結構：大戶↑ 超大戶↑ 散戶↓ (同週)"""
        if not changes:
            return None

        latest = changes[0]
        if (
            latest["more_than_400_change"] > 0
            and latest["more_than_1000_change"] > 0
            and latest["less_than_20_change"] < 0
        ):
            return ChipSignal(
                name=cls.SIGNALS["perfect_structure"]["name"],
                triggered=True,
                score=cls.SIGNALS["perfect_structure"]["score"],
                description=cls.SIGNALS["perfect_structure"]["description"],
            )
        return None

    @classmethod
    def _check_super_large_2w(cls, changes: List[Dict]) -> ChipSignal | None:
        """檢查超大戶連買2週"""
        if len(changes) < 2:
            return None

        consecutive_weeks = 0
        for change in changes:
            if change["more_than_1000_change"] > 0:
                consecutive_weeks += 1
            else:
                break

        if consecutive_weeks >= 2:
            return ChipSignal(
                name=cls.SIGNALS["super_large_2w"]["name"],
                triggered=True,
                score=cls.SIGNALS["super_large_2w"]["score"],
                description=f"連續{consecutive_weeks}週增加",
            )
        return None

    @classmethod
    def _check_large_3w(cls, changes: List[Dict]) -> ChipSignal | None:
        """檢查大戶連買3週"""
        if len(changes) < 3:
            return None

        consecutive_weeks = 0
        for change in changes:
            if change["more_than_400_change"] > 0:
                consecutive_weeks += 1
            else:
                break

        if consecutive_weeks >= 3:
            return ChipSignal(
                name=cls.SIGNALS["large_3w"]["name"],
                triggered=True,
                score=cls.SIGNALS["large_3w"]["score"],
                description=f"連續{consecutive_weeks}週增加",
            )
        return None

    @classmethod
    def _check_large_spike(cls, changes: List[Dict]) -> ChipSignal | None:
        """檢查大戶急買：單週增加>1%"""
        if not changes:
            return None

        latest = changes[0]
        if latest["more_than_400_change"] > 1.0:
            return ChipSignal(
                name=cls.SIGNALS["large_spike"]["name"],
                triggered=True,
                score=cls.SIGNALS["large_spike"]["score"],
                description=f"單週+{latest['more_than_400_change']:.2f}%",
            )
        return None

    @classmethod
    def _check_acceleration(cls, changes: List[Dict]) -> ChipSignal | None:
        """檢查加速集中：近5週平均變化 > 前5週平均變化"""
        if len(changes) < 10:
            return None

        recent_5w_avg = sum(c["more_than_400_change"] for c in changes[:5]) / 5
        previous_5w_avg = sum(c["more_than_400_change"] for c in changes[5:10]) / 5

        if recent_5w_avg > previous_5w_avg and recent_5w_avg > 0:
            return ChipSignal(
                name=cls.SIGNALS["acceleration"]["name"],
                triggered=True,
                score=cls.SIGNALS["acceleration"]["score"],
                description=f"近5週平均+{recent_5w_avg:.2f}% > 前5週+{previous_5w_avg:.2f}%",
            )
        return None

    @classmethod
    def _check_10w_持續(cls, data: List[ConcentrationData]) -> ChipSignal | None:
        """檢查10週持續買：W1-W10 變化>3%"""
        if len(data) < 10:
            return None

        latest = data[0].more_than_400
        ten_weeks_ago = data[9].more_than_400
        total_change = latest - ten_weeks_ago

        if total_change > 3.0:
            return ChipSignal(
                name=cls.SIGNALS["持續10w"]["name"],
                triggered=True,
                score=cls.SIGNALS["持續10w"]["score"],
                description=f"10週累積+{total_change:.2f}%",
            )
        return None

    @classmethod
    def _check_retail_3w_sell(cls, changes: List[Dict]) -> ChipSignal | None:
        """檢查散戶連賣3週"""
        if len(changes) < 3:
            return None

        consecutive_weeks = 0
        for change in changes:
            if change["less_than_20_change"] < 0:
                consecutive_weeks += 1
            else:
                break

        if consecutive_weeks >= 3:
            return ChipSignal(
                name=cls.SIGNALS["retail_3w_sell"]["name"],
                triggered=True,
                score=cls.SIGNALS["retail_3w_sell"]["score"],
                description=f"連續{consecutive_weeks}週減少",
            )
        return None

    @classmethod
    def _check_retail_panic(cls, changes: List[Dict]) -> ChipSignal | None:
        """檢查散戶恐慌：單週減少>1%"""
        if not changes:
            return None

        latest = changes[0]
        if latest["less_than_20_change"] < -1.0:
            return ChipSignal(
                name=cls.SIGNALS["retail_panic"]["name"],
                triggered=True,
                score=cls.SIGNALS["retail_panic"]["score"],
                description=f"單週{latest['less_than_20_change']:.2f}%",
            )
        return None

    @classmethod
    def _check_distribution(cls, changes: List[Dict]) -> ChipSignal | None:
        """檢查出貨訊號：大戶↓ 散戶↑ (同週)"""
        if not changes:
            return None

        latest = changes[0]
        if latest["more_than_400_change"] < 0 and latest["less_than_20_change"] > 0:
            return ChipSignal(
                name=cls.SIGNALS["distribution"]["name"],
                triggered=True,
                score=cls.SIGNALS["distribution"]["score"],
                description=cls.SIGNALS["distribution"]["description"],
            )
        return None

    @classmethod
    def _check_retail_fomo(cls, changes: List[Dict]) -> ChipSignal | None:
        """檢查散戶狂熱：散戶連續3週增加"""
        if len(changes) < 3:
            return None

        consecutive_weeks = 0
        for change in changes:
            if change["less_than_20_change"] > 0:
                consecutive_weeks += 1
            else:
                break

        if consecutive_weeks >= 3:
            return ChipSignal(
                name=cls.SIGNALS["retail_fomo"]["name"],
                triggered=True,
                score=cls.SIGNALS["retail_fomo"]["score"],
                description=f"連續{consecutive_weeks}週增加",
            )
        return None

    @classmethod
    def _calculate_expected_metrics(
        cls, signals: List[ChipSignal], total_score: int
    ) -> Tuple[float, float]:
        """
        計算預期報酬和勝率（基於歷史統計）

        這裡使用簡化的計算邏輯：
        - 預期報酬：基於訊號分數的線性估算（可以是負值）
        - 勝率：基於訊號數量和分數
        """
        if not signals:
            return 0.0, 0.0

        # 簡化版本：預期報酬 = 分數 * 0.13 （完美結構 20分 約 2.6%）
        # 負分也計算（表示預期虧損）
        expected_return = total_score * 0.13

        # 簡化版本：勝率基準 50%，每個正向訊號增加 5%，負向訊號減少 5%
        positive_signals = sum(1 for s in signals if s.score > 0)
        negative_signals = sum(1 for s in signals if s.score < 0)
        win_rate = 50.0 + (positive_signals * 5) - (negative_signals * 5)
        win_rate = max(10.0, min(80.0, win_rate))  # 限制在 10%-80% 範圍

        return round(expected_return, 2), round(win_rate, 1)

    @classmethod
    def normalize_score(cls, total_score: int) -> int:
        """
        將分數標準化到 0-100 區間

        最高分：20+15+12+10+10+8+8+6 = 89
        最低分：-15-10 = -25
        範圍：-25 到 89 (114 分範圍)
        """
        # 將 -25~89 映射到 0~100
        if total_score <= -25:
            return 0
        elif total_score >= 89:
            return 100
        else:
            # 線性映射
            normalized = int(((total_score + 25) / 114) * 100)
            return max(0, min(100, normalized))
