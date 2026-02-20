"""基本面訊號計算服務測試"""

import pandas as pd
import pytest

from api.services.fundamental_signal_service import FundamentalSignalService


class TestFundamentalSignals:
    """基本面訊號計算測試"""

    def setup_method(self):
        """初始化測試資料"""
        self.stock_info = {
            "per": 15.0,
            "cash_dividend": 5.0,
            "stock_dividend": 0.0,
            "capital": 100000.0,
            "outstanding_shares": 10000.0,
        }
        self.current_price = 100.0

    def test_profit_turnaround_signal(self):
        """測試由虧轉正訊號（+18 分）"""
        # 最新季獲利 1.5，前一季虧損 -0.5
        eps_data = pd.DataFrame(
            {
                "year": [2024, 2024],
                "quarter": [2, 1],
                "eps": [1.5, -0.5],
            }
        )

        signals, raw_score = FundamentalSignalService.calculate_signals(
            self.stock_info, eps_data, self.current_price
        )

        # 驗證訊號觸發
        profit_turnaround = next(
            (s for s in signals if s["name"] == "由虧轉正"), None
        )
        assert profit_turnaround is not None, "由虧轉正訊號應該觸發"
        assert profit_turnaround["triggered"] is True
        assert profit_turnaround["score"] == 18
        assert raw_score >= 18

    def test_loss_turnaround_signal(self):
        """測試由正轉虧訊號（-18 分）"""
        # 最新季虧損 -1.0，前一季獲利 2.0
        eps_data = pd.DataFrame(
            {
                "year": [2024, 2024],
                "quarter": [2, 1],
                "eps": [-1.0, 2.0],
            }
        )

        signals, raw_score = FundamentalSignalService.calculate_signals(
            self.stock_info, eps_data, self.current_price
        )

        # 驗證訊號觸發
        loss_turnaround = next(
            (s for s in signals if s["name"] == "由正轉虧"), None
        )
        assert loss_turnaround is not None, "由正轉虧訊號應該觸發"
        assert loss_turnaround["triggered"] is True
        assert loss_turnaround["score"] == -18
        # raw_score 可能包含其他訊號，所以不檢查總分，只檢查訊號本身

    def test_eps_growth_signal(self):
        """測試 EPS 連續成長訊號（+25 分）"""
        # 連續 4 季 EPS 成長：5.0 > 4.0 > 3.5 > 3.0
        eps_data = pd.DataFrame(
            {
                "year": [2024, 2024, 2023, 2023],
                "quarter": [2, 1, 4, 3],
                "eps": [5.0, 4.0, 3.5, 3.0],
            }
        )

        signals, raw_score = FundamentalSignalService.calculate_signals(
            self.stock_info, eps_data, self.current_price
        )

        # 驗證訊號觸發
        eps_growth = next(
            (s for s in signals if s["name"] == "EPS連續成長"), None
        )
        assert eps_growth is not None, "EPS連續成長訊號應該觸發"
        assert eps_growth["triggered"] is True
        assert eps_growth["score"] == 25
        assert raw_score >= 25

    def test_eps_decline_signal(self):
        """測試 EPS 連續衰退訊號（-25 分）"""
        # 連續 4 季 EPS 衰退：3.0 < 3.5 < 4.0 < 5.0（從最新到最舊）
        eps_data = pd.DataFrame(
            {
                "year": [2024, 2024, 2023, 2023],
                "quarter": [2, 1, 4, 3],
                "eps": [3.0, 3.5, 4.0, 5.0],
            }
        )

        signals, raw_score = FundamentalSignalService.calculate_signals(
            self.stock_info, eps_data, self.current_price
        )

        # 驗證訊號觸發
        eps_decline = next(
            (s for s in signals if s["name"] == "EPS連續衰退"), None
        )
        assert eps_decline is not None, "EPS連續衰退訊號應該觸發"
        assert eps_decline["triggered"] is True
        assert eps_decline["score"] == -25

    def test_reasonable_per_signal(self):
        """測試本益比合理訊號（+12 分）"""
        stock_info = self.stock_info.copy()
        stock_info["per"] = 18.0  # 0 < PER < 20

        eps_data = pd.DataFrame(
            {
                "year": [2024],
                "quarter": [1],
                "eps": [1.0],
            }
        )

        signals, raw_score = FundamentalSignalService.calculate_signals(
            stock_info, eps_data, self.current_price
        )

        # 驗證訊號觸發
        reasonable_per = next(
            (s for s in signals if s["name"] == "本益比合理"), None
        )
        assert reasonable_per is not None, "本益比合理訊號應該觸發"
        assert reasonable_per["triggered"] is True
        assert reasonable_per["score"] == 12

    def test_high_per_signal(self):
        """測試本益比過高訊號（-12 分）"""
        stock_info = self.stock_info.copy()
        stock_info["per"] = 45.0  # PER > 40

        eps_data = pd.DataFrame(
            {
                "year": [2024],
                "quarter": [1],
                "eps": [1.0],
            }
        )

        signals, raw_score = FundamentalSignalService.calculate_signals(
            stock_info, eps_data, self.current_price
        )

        # 驗證訊號觸發
        high_per = next((s for s in signals if s["name"] == "本益比過高"), None)
        assert high_per is not None, "本益比過高訊號應該觸發"
        assert high_per["triggered"] is True
        assert high_per["score"] == -12

    def test_high_dividend_yield_signal(self):
        """測試高股利殖利率訊號（+12 分）"""
        stock_info = self.stock_info.copy()
        stock_info["cash_dividend"] = 5.0  # 5/100 = 5% > 4%

        eps_data = pd.DataFrame(
            {
                "year": [2024],
                "quarter": [1],
                "eps": [1.0],
            }
        )

        signals, raw_score = FundamentalSignalService.calculate_signals(
            stock_info, eps_data, 100.0
        )

        # 驗證訊號觸發
        high_dividend = next(
            (s for s in signals if s["name"] == "高股利殖利率"), None
        )
        assert high_dividend is not None, "高股利殖利率訊號應該觸發"
        assert high_dividend["triggered"] is True
        assert high_dividend["score"] == 12

    def test_eps_stability_signal(self):
        """測試 EPS 穩定訊號（+8 分）"""
        # EPS 穩定：標準差 < 平均值 * 0.2
        eps_data = pd.DataFrame(
            {
                "year": [2024, 2024, 2023, 2023],
                "quarter": [2, 1, 4, 3],
                "eps": [5.0, 5.1, 4.9, 5.0],  # 非常穩定
            }
        )

        signals, raw_score = FundamentalSignalService.calculate_signals(
            self.stock_info, eps_data, self.current_price
        )

        # 驗證訊號觸發
        eps_stability = next(
            (s for s in signals if s["name"] == "EPS穩定"), None
        )
        assert eps_stability is not None, "EPS穩定訊號應該觸發"
        assert eps_stability["triggered"] is True
        assert eps_stability["score"] == 8

    def test_high_payout_ratio_signal(self):
        """測試高配息率訊號（+5 分）"""
        stock_info = self.stock_info.copy()
        stock_info["cash_dividend"] = 15.0  # 配息率 = 15 / (5*4) = 75% > 60%

        eps_data = pd.DataFrame(
            {
                "year": [2024, 2024, 2023, 2023],
                "quarter": [2, 1, 4, 3],
                "eps": [5.0, 5.0, 5.0, 5.0],
            }
        )

        signals, raw_score = FundamentalSignalService.calculate_signals(
            stock_info, eps_data, self.current_price
        )

        # 驗證訊號觸發
        high_payout = next(
            (s for s in signals if s["name"] == "高配息率"), None
        )
        assert high_payout is not None, "高配息率訊號應該觸發"
        assert high_payout["triggered"] is True
        assert high_payout["score"] == 5

    def test_normalization_max_score(self):
        """測試分數標準化 - 最高分"""
        # 最高原始分數：139
        normalized = FundamentalSignalService.normalize_score(139)
        assert normalized == 100, "最高分應該標準化為 100"

    def test_normalization_min_score(self):
        """測試分數標準化 - 最低分"""
        # 最低原始分數：-110
        normalized = FundamentalSignalService.normalize_score(-110)
        assert normalized == 0, "最低分應該標準化為 0"

    def test_normalization_mid_score(self):
        """測試分數標準化 - 中間值"""
        # 中間值：原始分數 10
        normalized = FundamentalSignalService.normalize_score(10)
        expected = int(((10 + 110) / 249) * 100)
        assert normalized == expected, f"中間值標準化應該為 {expected}"

    def test_empty_eps_data(self):
        """測試空 EPS 資料的處理"""
        eps_data = None

        signals, raw_score = FundamentalSignalService.calculate_signals(
            self.stock_info, eps_data, self.current_price
        )

        # 不應該觸發盈虧轉折或 EPS 相關訊號
        profit_turnaround = next(
            (s for s in signals if s["name"] == "由虧轉正"), None
        )
        loss_turnaround = next(
            (s for s in signals if s["name"] == "由正轉虧"), None
        )
        eps_growth = next(
            (s for s in signals if s["name"] == "EPS連續成長"), None
        )

        assert profit_turnaround is None, "空 EPS 資料不應觸發由虧轉正"
        assert loss_turnaround is None, "空 EPS 資料不應觸發由正轉虧"
        assert eps_growth is None, "空 EPS 資料不應觸發 EPS 連續成長"

    def test_mutually_exclusive_turnaround_signals(self):
        """測試由虧轉正和由正轉虧互斥"""
        # 只能有一個觸發
        eps_data_profit = pd.DataFrame(
            {
                "year": [2024, 2024],
                "quarter": [2, 1],
                "eps": [1.5, -0.5],
            }
        )

        signals_profit, _ = FundamentalSignalService.calculate_signals(
            self.stock_info, eps_data_profit, self.current_price
        )

        profit_count = sum(1 for s in signals_profit if s["name"] == "由虧轉正")
        loss_count = sum(1 for s in signals_profit if s["name"] == "由正轉虧")

        assert profit_count == 1, "由虧轉正應該觸發一次"
        assert loss_count == 0, "由正轉虧不應觸發"

        # 反向測試
        eps_data_loss = pd.DataFrame(
            {
                "year": [2024, 2024],
                "quarter": [2, 1],
                "eps": [-1.0, 2.0],
            }
        )

        signals_loss, _ = FundamentalSignalService.calculate_signals(
            self.stock_info, eps_data_loss, self.current_price
        )

        profit_count = sum(1 for s in signals_loss if s["name"] == "由虧轉正")
        loss_count = sum(1 for s in signals_loss if s["name"] == "由正轉虧")

        assert profit_count == 0, "由虧轉正不應觸發"
        assert loss_count == 1, "由正轉虧應該觸發一次"


class TestRevenueSignals:
    """營收訊號測試"""

    def setup_method(self):
        self.stock_info = {
            "per": 15.0,
            "cash_dividend": 3.0,
            "stock_dividend": 0.0,
            "capital": 100000.0,
            "outstanding_shares": 10000.0,
        }
        self.eps_data = pd.DataFrame(
            {"year": [2024], "quarter": [1], "eps": [1.0]}
        )

    def _make_revenue(self, yoy_values):
        """建立營收 DataFrame（最新在前）"""
        rows = []
        for i, yoy in enumerate(yoy_values):
            rows.append({
                "year": 2024, "month": 12 - i, "revenue": 1000.0,
                "mom_change": 0.0, "yoy_change": yoy,
                "cumulative_revenue": 1000.0, "cumulative_yoy_change": 0.0,
            })
        return pd.DataFrame(rows)

    def test_revenue_continuous_growth(self):
        """營收連續正成長 (+12)"""
        revenue = self._make_revenue([5.2, 3.1, 8.7, 2.0, 1.0, -1.0])
        signals, _ = FundamentalSignalService.calculate_signals(
            self.stock_info, self.eps_data, 100.0, revenue_data=revenue
        )
        assert any(s["name"] == "營收連續正成長" for s in signals)

    def test_revenue_continuous_growth_not_triggered(self):
        """營收連續正成長不觸發（有一個月為負）"""
        revenue = self._make_revenue([5.2, -1.0, 8.7, 2.0, 1.0, -1.0])
        signals, _ = FundamentalSignalService.calculate_signals(
            self.stock_info, self.eps_data, 100.0, revenue_data=revenue
        )
        assert not any(s["name"] == "營收連續正成長" for s in signals)

    def test_revenue_accelerating(self):
        """營收加速成長 (+10)"""
        revenue = self._make_revenue([15.0, 12.0, 18.0, 5.0, 8.0, 11.0])
        signals, _ = FundamentalSignalService.calculate_signals(
            self.stock_info, self.eps_data, 100.0, revenue_data=revenue
        )
        assert any(s["name"] == "營收加速成長" for s in signals)

    def test_revenue_accelerating_not_triggered(self):
        """營收加速成長不觸發（減速）"""
        revenue = self._make_revenue([12.0, 11.0, 13.0, 18.0, 20.0, 15.0])
        signals, _ = FundamentalSignalService.calculate_signals(
            self.stock_info, self.eps_data, 100.0, revenue_data=revenue
        )
        assert not any(s["name"] == "營收加速成長" for s in signals)

    def test_revenue_turnaround(self):
        """營收由衰轉增 (+15)"""
        revenue = self._make_revenue([3.5, -2.1, -4.0, 1.0, 2.0, 3.0])
        signals, _ = FundamentalSignalService.calculate_signals(
            self.stock_info, self.eps_data, 100.0, revenue_data=revenue
        )
        assert any(s["name"] == "營收由衰轉增" for s in signals)

    def test_revenue_turnaround_not_triggered(self):
        """營收由衰轉增不觸發（前月已正成長）"""
        revenue = self._make_revenue([3.5, 1.2, -4.0, 1.0, 2.0, 3.0])
        signals, _ = FundamentalSignalService.calculate_signals(
            self.stock_info, self.eps_data, 100.0, revenue_data=revenue
        )
        assert not any(s["name"] == "營收由衰轉增" for s in signals)

    def test_revenue_continuous_decline(self):
        """營收連續衰退 (-12)"""
        revenue = self._make_revenue([-3.2, -5.1, -1.8, 2.0, 1.0, -1.0])
        signals, _ = FundamentalSignalService.calculate_signals(
            self.stock_info, self.eps_data, 100.0, revenue_data=revenue
        )
        assert any(s["name"] == "營收連續衰退" for s in signals)

    def test_revenue_freeze(self):
        """營收急凍 (-15)"""
        revenue = self._make_revenue([-25.3, -5.0, 3.0, 2.0, 1.0, -1.0])
        signals, _ = FundamentalSignalService.calculate_signals(
            self.stock_info, self.eps_data, 100.0, revenue_data=revenue
        )
        assert any(s["name"] == "營收急凍" for s in signals)

    def test_revenue_freeze_not_triggered(self):
        """營收急凍不觸發（未達門檻）"""
        revenue = self._make_revenue([-15.0, -5.0, 3.0, 2.0, 1.0, -1.0])
        signals, _ = FundamentalSignalService.calculate_signals(
            self.stock_info, self.eps_data, 100.0, revenue_data=revenue
        )
        assert not any(s["name"] == "營收急凍" for s in signals)

    def test_revenue_insufficient_data(self):
        """資料不足時不觸發任何營收訊號"""
        revenue = self._make_revenue([5.0, 3.0, 8.0, 2.0])  # 只有 4 個月
        signals, _ = FundamentalSignalService.calculate_signals(
            self.stock_info, self.eps_data, 100.0, revenue_data=revenue
        )
        revenue_signals = [s for s in signals if "營收" in s["name"]]
        assert len(revenue_signals) == 0


class TestDividendSignals:
    """股利訊號測試"""

    def setup_method(self):
        self.stock_info = {
            "per": 15.0,
            "cash_dividend": 3.0,
            "stock_dividend": 0.0,
            "capital": 100000.0,
            "outstanding_shares": 10000.0,
        }
        self.eps_data = pd.DataFrame(
            {"year": [2024], "quarter": [1], "eps": [1.0]}
        )

    def test_dividend_continuous_growth(self):
        """股利連續成長 (+10)"""
        div_history = pd.DataFrame({
            "year": [114, 113, 112],
            "cash_dividend": [3.0, 2.5, 2.0],
            "stock_dividend": [0, 0, 0],
        })
        signals, _ = FundamentalSignalService.calculate_signals(
            self.stock_info, self.eps_data, 100.0, dividend_history=div_history
        )
        assert any(s["name"] == "股利連續成長" for s in signals)

    def test_dividend_continuous_growth_not_triggered(self):
        """股利連續成長不觸發（中間持平）"""
        div_history = pd.DataFrame({
            "year": [114, 113, 112],
            "cash_dividend": [3.0, 3.0, 2.0],
            "stock_dividend": [0, 0, 0],
        })
        signals, _ = FundamentalSignalService.calculate_signals(
            self.stock_info, self.eps_data, 100.0, dividend_history=div_history
        )
        assert not any(s["name"] == "股利連續成長" for s in signals)

    def test_dividend_cut(self):
        """股利大幅削減 (-12)"""
        div_history = pd.DataFrame({
            "year": [114, 113],
            "cash_dividend": [1.5, 3.0],
            "stock_dividend": [0, 0],
        })
        signals, _ = FundamentalSignalService.calculate_signals(
            self.stock_info, self.eps_data, 100.0, dividend_history=div_history
        )
        assert any(s["name"] == "股利大幅削減" for s in signals)

    def test_dividend_cut_not_triggered(self):
        """股利小幅減少不觸發"""
        div_history = pd.DataFrame({
            "year": [114, 113],
            "cash_dividend": [2.5, 3.0],
            "stock_dividend": [0, 0],
        })
        signals, _ = FundamentalSignalService.calculate_signals(
            self.stock_info, self.eps_data, 100.0, dividend_history=div_history
        )
        assert not any(s["name"] == "股利大幅削減" for s in signals)

    def test_dividend_insufficient_data(self):
        """資料不足不觸發"""
        div_history = pd.DataFrame({
            "year": [114],
            "cash_dividend": [3.0],
            "stock_dividend": [0],
        })
        signals, _ = FundamentalSignalService.calculate_signals(
            self.stock_info, self.eps_data, 100.0, dividend_history=div_history
        )
        div_signals = [s for s in signals if "股利" in s["name"] and s["name"] != "高股利殖利率"]
        assert len(div_signals) == 0


class TestPSRSignals:
    """PSR 訊號測試"""

    def setup_method(self):
        self.stock_info = {
            "per": 15.0,
            "cash_dividend": 3.0,
            "stock_dividend": 0.0,
            "capital": 100000.0,
            "outstanding_shares": 10000.0,
        }
        self.eps_data = pd.DataFrame(
            {"year": [2024], "quarter": [1], "eps": [1.0]}
        )

    def test_psr_low(self):
        """PSR 偏低 (+8)"""
        signals, _ = FundamentalSignalService.calculate_signals(
            self.stock_info, self.eps_data, 100.0, psr=0.8
        )
        assert any(s["name"] == "PSR偏低" for s in signals)

    def test_psr_high(self):
        """PSR 過高 (-8)"""
        signals, _ = FundamentalSignalService.calculate_signals(
            self.stock_info, self.eps_data, 100.0, psr=15.3
        )
        assert any(s["name"] == "PSR過高" for s in signals)

    def test_psr_normal(self):
        """PSR 正常範圍不觸發"""
        signals, _ = FundamentalSignalService.calculate_signals(
            self.stock_info, self.eps_data, 100.0, psr=3.5
        )
        assert not any(s["name"] == "PSR偏低" for s in signals)
        assert not any(s["name"] == "PSR過高" for s in signals)

    def test_psr_none(self):
        """PSR 為 None 不觸發"""
        signals, _ = FundamentalSignalService.calculate_signals(
            self.stock_info, self.eps_data, 100.0, psr=None
        )
        assert not any(s["name"] == "PSR偏低" for s in signals)
        assert not any(s["name"] == "PSR過高" for s in signals)


class TestEPSPrediction:
    """EPS 預測測試"""

    def test_seasonal_with_revenue(self):
        """有去年同季 EPS + 營收資料"""
        eps_data = pd.DataFrame({
            "year": [2025, 2024, 2024, 2024, 2024],
            "quarter": [1, 4, 3, 2, 1],
            "eps": [2.5, 2.3, 2.1, 1.9, 2.0],
        })
        revenue = pd.DataFrame({
            "year": [2025, 2025, 2025],
            "month": [3, 2, 1],
            "revenue": [1000, 1000, 1000],
            "yoy_change": [15.0, 10.0, 20.0],
        })
        result = FundamentalSignalService.predict_next_eps(eps_data, revenue)
        assert result is not None
        assert result["target_year"] == 2025
        assert result["target_quarter"] == 2
        assert result["method"] == "seasonal_revenue_adjusted"
        # 去年 Q2 EPS = 1.9，近 3 月平均 yoy = 15%，predicted = 1.9 * 1.15 = 2.185
        assert result["value"] == 2.18  # round(2.185, 2) = 2.18 (banker's rounding)

    def test_fallback_no_same_quarter(self):
        """無去年同季，fallback 到平均值"""
        eps_data = pd.DataFrame({
            "year": [2025, 2025],
            "quarter": [2, 1],
            "eps": [3.0, 2.0],
        })
        # 下一季 Q3，去年 Q3 不存在
        result = FundamentalSignalService.predict_next_eps(eps_data)
        assert result is not None
        assert result["target_quarter"] == 3
        assert result["method"] == "average_revenue_adjusted"
        assert result["value"] == 2.5  # (3.0 + 2.0) / 2

    def test_no_revenue_data(self):
        """無營收資料，不做調整"""
        eps_data = pd.DataFrame({
            "year": [2025, 2024, 2024, 2024, 2024],
            "quarter": [1, 4, 3, 2, 1],
            "eps": [2.5, 2.3, 2.1, 1.9, 2.0],
        })
        result = FundamentalSignalService.predict_next_eps(eps_data, None)
        assert result is not None
        assert result["value"] == 1.9  # 去年 Q2 原值

    def test_insufficient_eps_data(self):
        """EPS 資料不足"""
        eps_data = pd.DataFrame({
            "year": [2025],
            "quarter": [1],
            "eps": [2.0],
        })
        result = FundamentalSignalService.predict_next_eps(eps_data)
        assert result is None
