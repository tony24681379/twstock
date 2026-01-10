"""基本面訊號計算服務"""

from typing import Dict, List, Tuple

import pandas as pd


class FundamentalSignalService:
    """基本面訊號計算服務

    基於 StockInfo 和 StockEPS 表的資料，計算 10 個基本面訊號。
    重點強調「由虧轉正」(+30) 和「由正轉虧」(-30) 兩個最重要的訊號。
    """

    # 訊號定義（共 10 個）
    SIGNALS = {
        "profit_turnaround": {
            "name": "由虧轉正",
            "score": 30,
            "description": "從虧損轉為獲利，基本面重大轉折"
        },
        "loss_turnaround": {
            "name": "由正轉虧",
            "score": -30,
            "description": "從獲利轉為虧損，基本面重大惡化"
        },
        "eps_growth": {
            "name": "EPS連續成長",
            "score": 20,
            "description": "最近4季EPS連續正成長"
        },
        "eps_decline": {
            "name": "EPS連續衰退",
            "score": -20,
            "description": "最近4季EPS連續下降"
        },
        "per_reasonable": {
            "name": "本益比合理",
            "score": 15,
            "description": "本益比介於 0-20 之間"
        },
        "per_too_high": {
            "name": "本益比過高",
            "score": -15,
            "description": "本益比超過 40"
        },
        "high_dividend_yield": {
            "name": "高股利殖利率",
            "score": 12,
            "description": "股利殖利率 > 4%"
        },
        "eps_stable": {
            "name": "EPS穩定",
            "score": 8,
            "description": "EPS標準差小於平均值的20%"
        },
        "high_payout_ratio": {
            "name": "高配息率",
            "score": 8,
            "description": "配息率 > 60%"
        },
        "low_dividend": {
            "name": "低股利",
            "score": -8,
            "description": "股利殖利率 < 1%"
        }
    }

    @classmethod
    def calculate_signals(
        cls,
        stock_info: Dict,
        eps_data: pd.DataFrame,
        current_price: float
    ) -> Tuple[List[dict], int]:
        """計算基本面訊號

        Args:
            stock_info: StockInfo 表的單筆資料（dict）
            eps_data: 最近 4 季 EPS (DataFrame with columns: year, quarter, eps)
                     已按 year DESC, quarter DESC 排序
            current_price: 當前股價

        Returns:
            (訊號列表, 原始分數)
        """
        signals = []
        raw_score = 0

        # 檢查 EPS 資料是否存在
        has_eps_data = eps_data is not None and not eps_data.empty if isinstance(eps_data, pd.DataFrame) else False

        # 1. 由虧轉正 / 由正轉虧（最重要，+30/-30）
        if has_eps_data and len(eps_data) >= 2:
            latest_eps = eps_data['eps'].iloc[0]  # 最新季
            prev_eps = eps_data['eps'].iloc[1]    # 前一季

            if latest_eps > 0 and prev_eps < 0:
                signals.append({
                    "name": cls.SIGNALS["profit_turnaround"]["name"],
                    "triggered": True,
                    "score": cls.SIGNALS["profit_turnaround"]["score"],
                    "description": f"最近一季EPS {latest_eps:.2f}，前季虧損 {prev_eps:.2f}"
                })
                raw_score += cls.SIGNALS["profit_turnaround"]["score"]
            elif latest_eps < 0 and prev_eps > 0:
                signals.append({
                    "name": cls.SIGNALS["loss_turnaround"]["name"],
                    "triggered": True,
                    "score": cls.SIGNALS["loss_turnaround"]["score"],
                    "description": f"最近一季虧損 {latest_eps:.2f}，前季獲利 {prev_eps:.2f}"
                })
                raw_score += cls.SIGNALS["loss_turnaround"]["score"]

        # 2. EPS 連續成長 / 連續衰退
        if has_eps_data and len(eps_data) >= 4:
            eps_values = eps_data['eps'].iloc[:4].values  # 最近 4 季

            # 檢查連續成長：全為正數且連續遞增
            if all(eps > 0 for eps in eps_values):
                is_growing = all(eps_values[i] > eps_values[i + 1] for i in range(3))
                if is_growing:
                    signals.append({
                        "name": cls.SIGNALS["eps_growth"]["name"],
                        "triggered": True,
                        "score": cls.SIGNALS["eps_growth"]["score"],
                        "description": f"連續4季成長: {eps_values[3]:.2f} → {eps_values[0]:.2f}"
                    })
                    raw_score += cls.SIGNALS["eps_growth"]["score"]
                else:
                    # 檢查連續衰退：全為正數但連續遞減
                    is_declining = all(eps_values[i] < eps_values[i + 1] for i in range(3))
                    if is_declining:
                        signals.append({
                            "name": cls.SIGNALS["eps_decline"]["name"],
                            "triggered": True,
                            "score": cls.SIGNALS["eps_decline"]["score"],
                            "description": f"連續4季衰退: {eps_values[3]:.2f} → {eps_values[0]:.2f}"
                        })
                        raw_score += cls.SIGNALS["eps_decline"]["score"]

        # 3. 本益比合理性
        per = stock_info.get('per')
        if per is not None and per > 0:
            if 0 < per < 20:
                signals.append({
                    "name": cls.SIGNALS["per_reasonable"]["name"],
                    "triggered": True,
                    "score": cls.SIGNALS["per_reasonable"]["score"],
                    "description": f"PER={per:.1f} < 20"
                })
                raw_score += cls.SIGNALS["per_reasonable"]["score"]
            elif per > 40:
                signals.append({
                    "name": cls.SIGNALS["per_too_high"]["name"],
                    "triggered": True,
                    "score": cls.SIGNALS["per_too_high"]["score"],
                    "description": f"PER={per:.1f} > 40"
                })
                raw_score += cls.SIGNALS["per_too_high"]["score"]

        # 4. 股利殖利率
        cash_dividend = stock_info.get('cash_dividend', 0)
        if current_price > 0 and cash_dividend >= 0:
            dividend_yield = (cash_dividend / current_price) * 100

            if dividend_yield > 4:
                signals.append({
                    "name": cls.SIGNALS["high_dividend_yield"]["name"],
                    "triggered": True,
                    "score": cls.SIGNALS["high_dividend_yield"]["score"],
                    "description": f"殖利率 {dividend_yield:.2f}% > 4%"
                })
                raw_score += cls.SIGNALS["high_dividend_yield"]["score"]
            elif dividend_yield < 1:
                signals.append({
                    "name": cls.SIGNALS["low_dividend"]["name"],
                    "triggered": True,
                    "score": cls.SIGNALS["low_dividend"]["score"],
                    "description": f"殖利率 {dividend_yield:.2f}% < 1%"
                })
                raw_score += cls.SIGNALS["low_dividend"]["score"]

        # 5. EPS 穩定性
        if has_eps_data and len(eps_data) >= 4:
            eps_values = eps_data['eps'].iloc[:4].values
            if all(eps > 0 for eps in eps_values):
                eps_mean = eps_values.mean()
                eps_std = eps_values.std()

                if eps_mean > 0 and eps_std < eps_mean * 0.2:
                    signals.append({
                        "name": cls.SIGNALS["eps_stable"]["name"],
                        "triggered": True,
                        "score": cls.SIGNALS["eps_stable"]["score"],
                        "description": f"標準差 {eps_std:.2f} < 平均 {eps_mean:.2f} * 0.2"
                    })
                    raw_score += cls.SIGNALS["eps_stable"]["score"]

        # 6. 高配息率
        if has_eps_data and len(eps_data) >= 4 and cash_dividend > 0:
            eps_sum = eps_data['eps'].iloc[:4].sum()
            if eps_sum > 0:
                payout_ratio = (cash_dividend / eps_sum) * 100

                if payout_ratio > 60:
                    signals.append({
                        "name": cls.SIGNALS["high_payout_ratio"]["name"],
                        "triggered": True,
                        "score": cls.SIGNALS["high_payout_ratio"]["score"],
                        "description": f"配息率 {payout_ratio:.1f}% > 60%"
                    })
                    raw_score += cls.SIGNALS["high_payout_ratio"]["score"]

        return signals, raw_score

    @classmethod
    def normalize_score(cls, raw_score: int) -> int:
        """標準化分數到 0-100

        原始分數範圍：-73 到 +93
        標準化公式：((raw_score + 73) / 166) * 100

        Args:
            raw_score: 原始分數

        Returns:
            標準化後的分數 (0-100)
        """
        normalized = int(((raw_score + 73) / 166) * 100)
        return max(0, min(100, normalized))

    @classmethod
    def calculate_trend(cls, eps_series: pd.Series) -> str:
        """計算 EPS 趨勢

        Args:
            eps_series: EPS 序列（最新在前）

        Returns:
            "上升" | "下降" | "持平"
        """
        if len(eps_series) < 2:
            return "持平"

        # 計算線性趨勢：比較最新和最舊的值
        latest = eps_series.iloc[0]
        oldest = eps_series.iloc[-1]

        if latest > oldest * 1.1:  # 成長超過 10%
            return "上升"
        elif latest < oldest * 0.9:  # 衰退超過 10%
            return "下降"
        else:
            return "持平"
