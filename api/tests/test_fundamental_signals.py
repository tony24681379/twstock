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
        """測試由虧轉正訊號（最重要，+30 分）"""
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
        assert profit_turnaround["score"] == 30
        assert raw_score >= 30

    def test_loss_turnaround_signal(self):
        """測試由正轉虧訊號（最重要，-30 分）"""
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
        assert loss_turnaround["score"] == -30
        # raw_score 可能包含其他訊號，所以不檢查總分，只檢查訊號本身

    def test_eps_growth_signal(self):
        """測試 EPS 連續成長訊號（+20 分）"""
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
        assert eps_growth["score"] == 20
        assert raw_score >= 20

    def test_eps_decline_signal(self):
        """測試 EPS 連續衰退訊號（-20 分）"""
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
        assert eps_decline["score"] == -20

    def test_reasonable_per_signal(self):
        """測試本益比合理訊號（+15 分）"""
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
        assert reasonable_per["score"] == 15

    def test_high_per_signal(self):
        """測試本益比過高訊號（-15 分）"""
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
        assert high_per["score"] == -15

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
        """測試高配息率訊號（+8 分）"""
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
        assert high_payout["score"] == 8

    def test_low_dividend_signal(self):
        """測試低股利訊號（-8 分）"""
        stock_info = self.stock_info.copy()
        stock_info["cash_dividend"] = 0.5  # 0.5/100 = 0.5% < 1%

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
        low_dividend = next((s for s in signals if s["name"] == "低股利"), None)
        assert low_dividend is not None, "低股利訊號應該觸發"
        assert low_dividend["triggered"] is True
        assert low_dividend["score"] == -8

    def test_normalization_max_score(self):
        """測試分數標準化 - 最高分"""
        # 最高原始分數：93
        normalized = FundamentalSignalService.normalize_score(93)
        assert normalized == 100, "最高分應該標準化為 100"

    def test_normalization_min_score(self):
        """測試分數標準化 - 最低分"""
        # 最低原始分數：-73
        normalized = FundamentalSignalService.normalize_score(-73)
        assert normalized == 0, "最低分應該標準化為 0"

    def test_normalization_mid_score(self):
        """測試分數標準化 - 中間值"""
        # 中間值：原始分數 10
        normalized = FundamentalSignalService.normalize_score(10)
        expected = int(((10 + 73) / 166) * 100)
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
