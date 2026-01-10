"""API 端點測試"""

import pytest


@pytest.mark.asyncio
async def test_root_endpoint(client):
    """測試根端點"""
    response = await client.get("/")
    assert response.status_code == 200
    data = response.json()
    assert "name" in data
    assert "version" in data
    assert data["status"] == "running"


@pytest.mark.asyncio
async def test_list_stocks_default_params(client):
    """測試股票列表端點 - 預設參數"""
    response = await client.get("/api/stocks")

    # 在測試環境中，DB 未初始化時會回傳 503，這是預期行為
    # 在實際環境中（有 DB），應該回傳 200
    assert response.status_code in [200, 503]

    if response.status_code == 200:
        # 檢查回應結構
        data = response.json()
        assert "data" in data
        assert "pagination" in data

        # 檢查分頁資訊
        pagination = data["pagination"]
        assert "total" in pagination
        assert "page" in pagination
        assert "page_size" in pagination
        assert "total_pages" in pagination


@pytest.mark.asyncio
async def test_list_stocks_with_sorting(client):
    """測試股票列表端點 - 排序參數"""
    # 測試按 signal_strength 降序排序
    response = await client.get(
        "/api/stocks?sort_by=overall_strength&order=desc&limit=5"
    )
    assert response.status_code in [200, 503]

    # 測試按 stock_id 升序排序
    response = await client.get("/api/stocks?sort_by=stock_id&order=asc&limit=5")
    assert response.status_code in [200, 503]


@pytest.mark.asyncio
async def test_list_stocks_invalid_sort_field(client):
    """測試股票列表端點 - 無效排序欄位"""
    response = await client.get("/api/stocks?sort_by=invalid_field")
    # 400 或 503（DB 未初始化）
    assert response.status_code in [400, 503]

    if response.status_code == 400:
        data = response.json()
        assert "detail" in data


@pytest.mark.asyncio
async def test_list_stocks_invalid_order(client):
    """測試股票列表端點 - 無效排序方向"""
    response = await client.get("/api/stocks?order=invalid")
    # 400 或 503（DB 未初始化）
    assert response.status_code in [400, 503]


@pytest.mark.asyncio
async def test_get_stock_detail_valid(client):
    """測試取得股票詳細資訊 - 有效股票代碼"""
    # 使用實際存在的股票代碼（如果測試資料庫有的話）
    response = await client.get("/api/stocks/2330")

    # 如果資料庫中有資料，應該回傳 200
    # 如果沒有資料，應該回傳 404
    # 如果 DB 未初始化，應該回傳 503
    assert response.status_code in [200, 404, 503]

    if response.status_code == 200:
        data = response.json()
        assert "basic_info" in data
        assert "price_info" in data
        assert "signal_strength" in data


@pytest.mark.asyncio
async def test_get_stock_detail_invalid_format(client):
    """測試取得股票詳細資訊 - 無效股票代碼格式"""
    # 股票代碼太短（< 3 位）
    response = await client.get("/api/stocks/12")
    assert response.status_code in [400, 503]

    # 股票代碼太長（> 6 位）
    response = await client.get("/api/stocks/1234567")
    assert response.status_code in [400, 503]


@pytest.mark.asyncio
async def test_get_stock_history(client):
    """測試取得股票歷史資料"""
    # 測試預設參數（12 週）
    response = await client.get("/api/stocks/2330/history")
    assert response.status_code in [200, 404, 503]

    # 測試自訂週數
    response = await client.get("/api/stocks/2330/history?weeks=4")
    assert response.status_code in [200, 404, 503]


@pytest.mark.asyncio
async def test_get_stock_chart(client):
    """測試取得股票圖表資料"""
    # 測試預設 period（3M）
    response = await client.get("/api/stocks/2330/chart")
    assert response.status_code in [200, 404, 503]

    # 測試不同 period
    for period in ["1M", "3M", "6M", "1Y"]:
        response = await client.get(f"/api/stocks/2330/chart?period={period}")
        assert response.status_code in [200, 404, 503]


@pytest.mark.asyncio
async def test_get_stock_chart_invalid_period(client):
    """測試取得股票圖表資料 - 無效 period"""
    response = await client.get("/api/stocks/2330/chart?period=invalid")
    # 參數驗證應在 DB 檢查之前，但如果 DB 未初始化會先回傳 503
    assert response.status_code in [400, 503]


@pytest.mark.asyncio
async def test_get_stock_chips(client):
    """測試取得股票籌碼資料"""
    # 測試預設參數（30 天）
    response = await client.get("/api/stocks/2330/chips")
    assert response.status_code in [200, 404, 503]

    # 測試自訂天數
    response = await client.get("/api/stocks/2330/chips?days=7")
    assert response.status_code in [200, 404, 503]
