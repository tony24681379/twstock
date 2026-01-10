"""技術指標計算服務"""

from typing import Any, Dict, List

import numpy as np
import talib


class IndicatorService:
    """技術指標計算服務（使用 ta-lib）"""

    @staticmethod
    def calculate_ma(
        close_prices: np.ndarray, periods: List[int]
    ) -> Dict[str, List[float]]:
        """
        計算移動平均線

        Args:
            close_prices: 收盤價 numpy array
            periods: 週期列表（例如 [5, 10, 20, 60]）

        Returns:
            各週期的移動平均線 dict
        """
        result = {}
        for period in periods:
            ma = talib.SMA(close_prices, timeperiod=period)
            result[f"MA{period}"] = ma.tolist()
        return result

    @staticmethod
    def calculate_macd(close_prices: np.ndarray) -> Dict[str, List[float]]:
        """
        計算 MACD（12, 26, 9）

        Returns:
            {'macd': [...], 'signal': [...], 'histogram': [...]}
        """
        macd, signal, histogram = talib.MACD(
            close_prices, fastperiod=12, slowperiod=26, signalperiod=9
        )
        return {
            "macd": macd.tolist(),
            "signal": signal.tolist(),
            "histogram": histogram.tolist(),
        }

    @staticmethod
    def calculate_kd(
        high: np.ndarray, low: np.ndarray, close: np.ndarray
    ) -> Dict[str, List[float]]:
        """
        計算 KD 隨機指標（9, 3, 3）

        Returns:
            {'k': [...], 'd': [...]}
        """
        k, d = talib.STOCH(
            high,
            low,
            close,
            fastk_period=9,
            slowk_period=3,
            slowk_matype=0,
            slowd_period=3,
            slowd_matype=0,
        )
        return {"k": k.tolist(), "d": d.tolist()}

    @staticmethod
    def calculate_rsi(close_prices: np.ndarray, period: int = 14) -> List[float]:
        """
        計算 RSI 相對強弱指標

        Args:
            close_prices: 收盤價
            period: 週期（預設 14）

        Returns:
            RSI 值列表
        """
        rsi = talib.RSI(close_prices, timeperiod=period)
        return rsi.tolist()

    @staticmethod
    def calculate_bollinger_bands(
        close_prices: np.ndarray, period: int = 20, nbdev: int = 2
    ) -> Dict[str, List[float]]:
        """
        計算布林通道

        Args:
            close_prices: 收盤價
            period: 週期（預設 20）
            nbdev: 標準差倍數（預設 2）

        Returns:
            {'upper': [...], 'middle': [...], 'lower': [...]}
        """
        upper, middle, lower = talib.BBANDS(
            close_prices, timeperiod=period, nbdevup=nbdev, nbdevdn=nbdev, matype=0
        )
        return {
            "upper": upper.tolist(),
            "middle": middle.tolist(),
            "lower": lower.tolist(),
        }

    @staticmethod
    def calculate_indicators(
        open_prices: List[float],
        high_prices: List[float],
        low_prices: List[float],
        close_prices: List[float],
        volume: List[float],
        indicators: List[str],
    ) -> Dict[str, Any]:
        """
        根據請求計算多個技術指標

        Args:
            open_prices: 開盤價列表
            high_prices: 最高價列表
            low_prices: 最低價列表
            close_prices: 收盤價列表
            volume: 成交量列表
            indicators: 要計算的指標列表（例如 ['MA', 'MACD', 'RSI']）

        Returns:
            各指標的計算結果
        """
        # 轉換為 numpy array
        close_np = np.array(close_prices, dtype=float)
        high_np = np.array(high_prices, dtype=float)
        low_np = np.array(low_prices, dtype=float)

        result = {}

        for indicator in indicators:
            indicator = indicator.upper().strip()

            if indicator == "MA" or indicator.startswith("MA"):
                # 預設計算 MA5, 10, 20, 60
                result.update(IndicatorService.calculate_ma(close_np, [5, 10, 20, 60]))

            elif indicator == "MACD":
                result["MACD"] = IndicatorService.calculate_macd(close_np)

            elif indicator == "KD":
                result["KD"] = IndicatorService.calculate_kd(high_np, low_np, close_np)

            elif indicator == "RSI":
                result["RSI"] = IndicatorService.calculate_rsi(close_np)

            elif indicator == "BB" or indicator == "BBANDS":
                result["BB"] = IndicatorService.calculate_bollinger_bands(close_np)

        return result
        return result
