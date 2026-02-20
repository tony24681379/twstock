"""基本面訊號計算服務"""

from typing import Dict, List, Optional, Tuple

import pandas as pd


class FundamentalSignalService:
    """基本面訊號計算服務

    基於 StockInfo、StockEPS、月營收、歷史股利等資料，計算 18 個基本面訊號。
    最重要訊號：EPS連續成長 (+25) 和 EPS連續衰退 (-25)。
    """

    # 訊號定義（共 18 個）
    SIGNALS = {
        # === EPS / 估值訊號 9 個 ===
        "profit_turnaround": {
            "name": "由虧轉正",
            "score": 18,
            "description": "從虧損轉為獲利，基本面重大轉折"
        },
        "loss_turnaround": {
            "name": "由正轉虧",
            "score": -18,
            "description": "從獲利轉為虧損，基本面重大惡化"
        },
        "eps_growth": {
            "name": "EPS連續成長",
            "score": 25,
            "description": "最近4季EPS連續正成長"
        },
        "eps_decline": {
            "name": "EPS連續衰退",
            "score": -25,
            "description": "最近4季EPS連續下降"
        },
        "per_reasonable": {
            "name": "本益比合理",
            "score": 12,
            "description": "本益比介於 0-20 之間"
        },
        "per_too_high": {
            "name": "本益比過高",
            "score": -12,
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
            "score": 5,
            "description": "配息率 > 60%"
        },
        # === 營收訊號 5 個 ===
        "revenue_continuous_growth": {
            "name": "營收連續正成長",
            "score": 15,
            "description": "最近3個月營收年增率皆 > 0%"
        },
        "revenue_accelerating": {
            "name": "營收加速成長",
            "score": 12,
            "description": "近3月平均年增率 > 前3月，且 > 10%"
        },
        "revenue_turnaround": {
            "name": "營收由衰轉增",
            "score": 10,
            "description": "最近月年增率轉正，前2月皆為負"
        },
        "revenue_continuous_decline": {
            "name": "營收連續衰退",
            "score": -15,
            "description": "最近3個月營收年增率皆 < 0%"
        },
        "revenue_freeze": {
            "name": "營收急凍",
            "score": -18,
            "description": "最近月營收年增率 < -20%"
        },
        # === 股利訊號 2 個 ===
        "dividend_continuous_growth": {
            "name": "股利連續成長",
            "score": 10,
            "description": "最近3年現金股利連續增加"
        },
        "dividend_cut": {
            "name": "股利大幅削減",
            "score": -12,
            "description": "最新年度現金股利較前年減少30%以上"
        },
        # === PSR 訊號 2 個 ===
        "psr_low": {
            "name": "PSR偏低",
            "score": 12,
            "description": "PSR 介於 0-1 之間"
        },
        "psr_high": {
            "name": "PSR過高",
            "score": -10,
            "description": "PSR > 10"
        },
    }

    @classmethod
    def calculate_signals(
        cls,
        stock_info: Dict,
        eps_data: pd.DataFrame,
        current_price: float,
        revenue_data: pd.DataFrame = None,
        dividend_history: pd.DataFrame = None,
        psr: float = None,
    ) -> Tuple[List[dict], int]:
        """計算基本面訊號

        Args:
            stock_info: StockInfo 表的單筆資料（dict）
            eps_data: 最近 4 季 EPS (DataFrame with columns: year, quarter, eps)
                     已按 year DESC, quarter DESC 排序
            current_price: 當前股價
            revenue_data: 月營收 (DataFrame with columns: year, month, revenue, yoy_change, ...)
                         已按 year DESC, month DESC 排序（最新在前）
            dividend_history: 歷史股利 (DataFrame with columns: year, cash_dividend, stock_dividend)
                            已按 year DESC 排序（最新在前）
            psr: 股價營收比（已計算好的值）

        Returns:
            (訊號列表, 原始分數)
        """
        signals = []
        raw_score = 0

        # 檢查 EPS 資料是否存在
        has_eps_data = eps_data is not None and not eps_data.empty if isinstance(eps_data, pd.DataFrame) else False

        # 1. 由虧轉正 / 由正轉虧（+18/-18）
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

        # === 營收訊號（需要至少 6 個月有效 yoy_change）===
        has_revenue = (
            revenue_data is not None
            and isinstance(revenue_data, pd.DataFrame)
            and not revenue_data.empty
        )
        if has_revenue:
            yoy_values = revenue_data['yoy_change'].dropna().values
            if len(yoy_values) >= 6:
                recent_3 = yoy_values[:3]  # 最近 3 個月（最新在前）
                prev_3 = yoy_values[3:6]   # 前 3 個月

                # 7. 營收連續正成長
                if all(v > 0 for v in recent_3):
                    signals.append({
                        "name": cls.SIGNALS["revenue_continuous_growth"]["name"],
                        "triggered": True,
                        "score": cls.SIGNALS["revenue_continuous_growth"]["score"],
                        "description": f"近3月年增率: {recent_3[0]:.1f}%, {recent_3[1]:.1f}%, {recent_3[2]:.1f}%"
                    })
                    raw_score += cls.SIGNALS["revenue_continuous_growth"]["score"]

                # 8. 營收加速成長
                recent_avg = recent_3.mean()
                prev_avg = prev_3.mean()
                if recent_avg > prev_avg and recent_avg > 10:
                    signals.append({
                        "name": cls.SIGNALS["revenue_accelerating"]["name"],
                        "triggered": True,
                        "score": cls.SIGNALS["revenue_accelerating"]["score"],
                        "description": f"近3月均值 {recent_avg:.1f}% > 前3月 {prev_avg:.1f}%"
                    })
                    raw_score += cls.SIGNALS["revenue_accelerating"]["score"]

                # 9. 營收由衰轉增
                if yoy_values[0] > 0 and yoy_values[1] < 0 and yoy_values[2] < 0:
                    signals.append({
                        "name": cls.SIGNALS["revenue_turnaround"]["name"],
                        "triggered": True,
                        "score": cls.SIGNALS["revenue_turnaround"]["score"],
                        "description": f"最新月 {yoy_values[0]:.1f}%，前2月 {yoy_values[1]:.1f}%, {yoy_values[2]:.1f}%"
                    })
                    raw_score += cls.SIGNALS["revenue_turnaround"]["score"]

                # 10. 營收連續衰退
                if all(v < 0 for v in recent_3):
                    signals.append({
                        "name": cls.SIGNALS["revenue_continuous_decline"]["name"],
                        "triggered": True,
                        "score": cls.SIGNALS["revenue_continuous_decline"]["score"],
                        "description": f"近3月年增率: {recent_3[0]:.1f}%, {recent_3[1]:.1f}%, {recent_3[2]:.1f}%"
                    })
                    raw_score += cls.SIGNALS["revenue_continuous_decline"]["score"]

                # 11. 營收急凍
                if yoy_values[0] < -20:
                    signals.append({
                        "name": cls.SIGNALS["revenue_freeze"]["name"],
                        "triggered": True,
                        "score": cls.SIGNALS["revenue_freeze"]["score"],
                        "description": f"最新月年增率 {yoy_values[0]:.1f}% < -20%"
                    })
                    raw_score += cls.SIGNALS["revenue_freeze"]["score"]

        # === 股利訊號（需要歷史股利資料）===
        has_dividend_history = (
            dividend_history is not None
            and isinstance(dividend_history, pd.DataFrame)
            and not dividend_history.empty
        )
        if has_dividend_history:
            div_values = dividend_history['cash_dividend'].values  # 最新在前

            # 12. 股利連續成長（需要至少 3 年）
            if len(div_values) >= 3:
                recent_3y = div_values[:3]
                if all(recent_3y[i] > recent_3y[i + 1] for i in range(2)) and recent_3y[2] > 0:
                    signals.append({
                        "name": cls.SIGNALS["dividend_continuous_growth"]["name"],
                        "triggered": True,
                        "score": cls.SIGNALS["dividend_continuous_growth"]["score"],
                        "description": f"連續3年成長: {recent_3y[2]:.1f} → {recent_3y[0]:.1f}"
                    })
                    raw_score += cls.SIGNALS["dividend_continuous_growth"]["score"]

            # 13. 股利大幅削減（需要至少 2 年）
            if len(div_values) >= 2 and div_values[1] > 0:
                change_pct = (div_values[0] - div_values[1]) / div_values[1]
                if change_pct < -0.3:
                    signals.append({
                        "name": cls.SIGNALS["dividend_cut"]["name"],
                        "triggered": True,
                        "score": cls.SIGNALS["dividend_cut"]["score"],
                        "description": f"股利 {div_values[1]:.1f} → {div_values[0]:.1f}（減少 {abs(change_pct)*100:.0f}%）"
                    })
                    raw_score += cls.SIGNALS["dividend_cut"]["score"]

        # === PSR 訊號 ===
        if psr is not None and psr > 0:
            # 14. PSR 偏低
            if 0 < psr <= 1:
                signals.append({
                    "name": cls.SIGNALS["psr_low"]["name"],
                    "triggered": True,
                    "score": cls.SIGNALS["psr_low"]["score"],
                    "description": f"PSR={psr:.2f} ≤ 1"
                })
                raw_score += cls.SIGNALS["psr_low"]["score"]
            # 15. PSR 過高
            elif psr > 10:
                signals.append({
                    "name": cls.SIGNALS["psr_high"]["name"],
                    "triggered": True,
                    "score": cls.SIGNALS["psr_high"]["score"],
                    "description": f"PSR={psr:.2f} > 10"
                })
                raw_score += cls.SIGNALS["psr_high"]["score"]

        return signals, raw_score

    @classmethod
    def normalize_score(cls, raw_score: int) -> int:
        """標準化分數到 0-100

        原始分數範圍：-110 到 +139
        標準化公式：((raw_score + 110) / 249) * 100

        Args:
            raw_score: 原始分數

        Returns:
            標準化後的分數 (0-100)
        """
        normalized = int(((raw_score + 110) / 249) * 100)
        return max(0, min(100, normalized))

    @classmethod
    def predict_next_eps(
        cls,
        eps_data: pd.DataFrame,
        revenue_data: pd.DataFrame = None,
    ) -> Optional[Dict]:
        """預測下一季 EPS

        公式：predicted_eps = 去年同季 EPS × (1 + 近3月平均營收年增率 / 100)
        若去年同季不存在，fallback 為最近 4 季平均 EPS。

        Args:
            eps_data: 最近 EPS (DataFrame with columns: year, quarter, eps)
                     已按 year DESC, quarter DESC 排序（最新在前）
            revenue_data: 月營收 (DataFrame with columns: yoy_change, ...)

        Returns:
            預測結果 dict 或 None
        """
        has_eps = eps_data is not None and isinstance(eps_data, pd.DataFrame) and not eps_data.empty
        if not has_eps or len(eps_data) < 2:
            return None

        # 推算下一季
        latest_year = int(eps_data['year'].iloc[0])
        latest_quarter = int(eps_data['quarter'].iloc[0])
        if latest_quarter == 4:
            target_year = latest_year + 1
            target_quarter = 1
        else:
            target_year = latest_year
            target_quarter = latest_quarter + 1

        # 找去年同季 EPS
        same_quarter_mask = (
            (eps_data['year'] == target_year - 1)
            & (eps_data['quarter'] == target_quarter)
        )
        same_quarter_rows = eps_data[same_quarter_mask]

        if not same_quarter_rows.empty:
            base_eps = float(same_quarter_rows['eps'].iloc[0])
            method = "seasonal_revenue_adjusted"
        else:
            # fallback: 最近 4 季平均
            base_eps = float(eps_data['eps'].iloc[:min(4, len(eps_data))].mean())
            method = "average_revenue_adjusted"

        # 營收年增率調整
        revenue_adjustment = 1.0
        has_revenue = (
            revenue_data is not None
            and isinstance(revenue_data, pd.DataFrame)
            and not revenue_data.empty
        )
        if has_revenue:
            yoy_values = revenue_data['yoy_change'].dropna().values
            if len(yoy_values) >= 3:
                avg_yoy = float(yoy_values[:3].mean())
                revenue_adjustment = 1.0 + avg_yoy / 100

        predicted_eps = round(base_eps * revenue_adjustment, 2)

        return {
            "value": predicted_eps,
            "target_year": target_year,
            "target_quarter": target_quarter,
            "method": method,
            "is_prediction": True,
        }

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
