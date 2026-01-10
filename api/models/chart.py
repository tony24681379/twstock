"""圖表資料 Pydantic Models"""
from typing import Any, Dict, List

from pydantic import BaseModel, Field


class OHLCVData(BaseModel):
    """OHLCV K 線資料點"""
    time: str = Field(..., description="時間（ISO 格式或 Unix timestamp）")
    open: float
    high: float
    low: float
    close: float
    volume: float


class ChartDataResponse(BaseModel):
    """圖表資料回應"""
    stock_id: str
    period: str = Field(..., description="時間範圍（1M/3M/6M/1Y）")
    ohlcv: List[OHLCVData] = Field(..., description="K 線資料")
    indicators: Dict[str, Any] = Field(default_factory=dict, description="技術指標")

    class Config:
        json_schema_extra = {
            "example": {
                "stock_id": "2330",
                "period": "3M",
                "ohlcv": [
                    {
                        "time": "2025-10-01",
                        "open": 1000.0,
                        "high": 1050.0,
                        "low": 990.0,
                        "close": 1030.0,
                        "volume": 50000.0
                    }
                ],
                "indicators": {
                    "MA5": [1025.0, 1028.0, ...],
                    "MA20": [1015.0, 1018.0, ...],
                    "MACD": {
                        "macd": [5.2, 6.1, ...],
                        "signal": [4.8, 5.5, ...],
                        "histogram": [0.4, 0.6, ...]
                    }
                }
            }
        }
