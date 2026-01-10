"""pytest 測試配置檔"""
import pytest
import pytest_asyncio
from httpx import ASGITransport, AsyncClient

from api.main import app


@pytest_asyncio.fixture
async def client():
    """測試客戶端 fixture"""
    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as ac:
        yield ac


@pytest.fixture
def sample_stock_list_item():
    """範例 StockListItem 資料"""
    from datetime import datetime
    return {
        "stock_id": "2330",
        "name": "台積電",
        "close_price": 600.0,
        "chip_strength": 80,
        "technical_strength": 75,
        "overall_strength": 85,
        "signal_strength": 85,  # 必填
        "signal_count": 5,
        "expected_return": 12.5,
        "win_rate": 65.0,
        "risk_level": "低",
        "chip_signals": [],
        "technical_signals": [],
        "signals": [],
        "major_signals": ["完美結構", "大戶連買3週"],
        "last_updated": datetime.now()  # 必填
    }


@pytest.fixture
def sample_stock_detail():
    """範例 StockDetail 資料"""
    from datetime import datetime
    return {
        "basic_info": {
            "stock_id": "2330",
            "name": "台積電",
            "industry": "半導體業"
        },
        "price_info": {
            "close": 600.0,
            "open": 595.0,
            "high": 605.0,
            "low": 590.0,
            "volume": 25000000,
            "date": datetime.now(),
            "change": 5.0,
            "change_percent": 0.84
        },
        "signal_strength": 85,
        "expected_return": 12.5,
        "win_rate": 65.0,
        "concentration_summary": {
            "large_holders_pct": 45.5,
            "super_large_holders_pct": 38.2,
            "retail_pct": 12.3,
            "latest_date": datetime.now()
        },
        "chip_signals": [],
        "technical_signals": [],
        "signals": []
    }
    }
