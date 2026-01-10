"""Pydantic Models 測試"""

import pytest
from pydantic import ValidationError

from api.models.stock import (
    BasicInfo,
    ConcentrationSummary,
    PaginationMetadata,
    PriceInfo,
    StockDetail,
    StockListItem,
    StockListResponse,
)


def test_stock_list_item_valid(sample_stock_list_item):
    """測試 StockListItem 模型驗證 - 有效資料"""
    item = StockListItem(**sample_stock_list_item)
    assert item.stock_id == "2330"
    assert item.name == "台積電"
    assert item.close_price == 600.0
    assert item.chip_strength == 80
    assert item.technical_strength == 75
    assert item.overall_strength == 85


def test_stock_list_item_missing_required_fields():
    """測試 StockListItem 模型驗證 - 缺少必填欄位"""
    with pytest.raises(ValidationError) as exc_info:
        StockListItem(stock_id="2330")  # 缺少其他必填欄位

    errors = exc_info.value.errors()
    assert len(errors) > 0
    # 檢查是否有 name 欄位的錯誤
    assert any(error["loc"][0] == "name" for error in errors)


def test_stock_list_item_invalid_types():
    """測試 StockListItem 模型驗證 - 錯誤型別"""
    with pytest.raises(ValidationError):
        StockListItem(
            stock_id="2330",
            name="台積電",
            close_price="not a number",  # 應該是 float
            chip_strength=80,
            technical_strength=75,
            overall_strength=85,
            signal_count=5,
            expected_return=12.5,
            win_rate=65.0,
        )


def test_basic_info_valid():
    """測試 BasicInfo 模型驗證"""
    info = BasicInfo(
        stock_id="2330",
        name="台積電",
        industry="半導體業",
        market_value=15500000000000.0,
    )
    assert info.stock_id == "2330"
    assert info.name == "台積電"


def test_price_info_valid():
    """測試 PriceInfo 模型驗證"""
    from datetime import datetime

    price = PriceInfo(
        close=600.0,
        open=595.0,
        high=605.0,
        low=590.0,
        volume=25000000,
        date=datetime.now(),
        change=5.0,
        change_percent=0.84,
    )
    assert price.close == 600.0
    assert price.volume == 25000000


def test_concentration_summary_valid():
    """測試 ConcentrationSummary 模型驗證"""
    from datetime import datetime

    summary = ConcentrationSummary(
        large_holders_pct=45.5,
        super_large_holders_pct=38.2,
        retail_pct=12.3,
        latest_date=datetime.now(),
    )
    assert summary.large_holders_pct == 45.5
    assert summary.retail_pct == 12.3


def test_stock_detail_valid(sample_stock_detail):
    """測試 StockDetail 模型驗證 - 有效資料"""
    detail = StockDetail(**sample_stock_detail)
    assert detail.basic_info.stock_id == "2330"
    assert detail.price_info.close == 600.0
    assert detail.signal_strength == 85


def test_pagination_metadata_calculation():
    """測試 PaginationMetadata 計算正確性"""
    # 測試第一頁
    meta = PaginationMetadata(
        total=250, page=1, page_size=100, total_pages=3, has_next=True, has_prev=False
    )
    assert meta.page == 1
    assert meta.total_pages == 3
    assert meta.has_next is True
    assert meta.has_prev is False

    # 測試第二頁
    meta = PaginationMetadata(
        total=250, page=2, page_size=100, total_pages=3, has_next=True, has_prev=True
    )
    assert meta.page == 2
    assert meta.has_next is True
    assert meta.has_prev is True

    # 測試最後一頁
    meta = PaginationMetadata(
        total=250, page=3, page_size=50, total_pages=3, has_next=False, has_prev=True
    )
    assert meta.page == 3
    assert meta.has_next is False
    assert meta.has_prev is True


def test_stock_list_response_valid(sample_stock_list_item):
    """測試 StockListResponse 模型驗證"""
    response = StockListResponse(
        data=[StockListItem(**sample_stock_list_item)],
        pagination=PaginationMetadata(
            total=100,
            page=1,
            page_size=10,
            total_pages=10,
            has_next=True,
            has_prev=False,
        ),
    )
    assert len(response.data) == 1
    assert response.pagination.total == 100
