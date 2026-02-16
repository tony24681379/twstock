## 1. 資料庫層

- [x] 1.1 在 `twstock/database.py` 新增 `ConvertibleBond` model（bond_id, name, underlying_stock_id, conversion_price, issue_date, maturity_date, put_date, put_price, coupon_rate, issued_amount, outstanding_amount, is_active, updated_at）
- [x] 1.2 在 `twstock/database.py` 新增 `ConvertibleBondDaily` model（bond_id, date, open, high, low, close, volume, underlying_close, conversion_value, premium_rate, arbitrage_spread），含 (bond_id, date) 複合唯一索引
- [x] 1.3 在 `twstock/database.py` 新增 `ConvertibleBondSignals` model（bond_id, date, raw_score, normalized_score, signal_count, risk_level, signals_json, underlying_stock_id, premium_rate, conversion_value, updated_at）
- [x] 1.4 在 `DatabaseManager` 新增批次方法：`save_convertible_bonds()`, `save_convertible_bond_daily()`, `save_convertible_bond_signals()`, `bulk_load_convertible_bond_daily()`, `bulk_check_cb_needs_update()`

## 2. 資料爬蟲層

- [x] 2.1 建立 `twstock/convertible_fetcher.py`，實作 `ConvertibleBondFetcher` 類別，使用 FinMind API（`TaiwanStockConvertibleBondInfo` + `TaiwanStockConvertibleBondDaily`）
- [x] 2.2 實作 `get_convertible_bond_list()` 方法：取得所有 CB 清單並同步 is_active 狀態
- [x] 2.3 實作 `fetch_convertible_bond_daily()` 方法：取得每日交易資料，計算衍生欄位（conversion_value, premium_rate, arbitrage_spread）
- [x] 2.4 實作 FinMind API 不可用時的錯誤處理與日誌
- [ ] 2.5 驗證 FinMind API 連線和資料格式（手動測試 or 簡單測試腳本）

## 3. 套利訊號引擎

- [x] 3.1 建立 `api/services/convertible_signal_service.py`，定義 10 個訊號常數與觸發條件
- [x] 3.2 實作 6 個套利訊號：折價套利(+20), 低溢價率(+15), 催換在即(+15), 到期賣回保護(+12), 深度價內(+10), 低於面額(+8)
- [x] 3.3 實作 2 個標的股連動訊號：標的股強勢(+12), 標的股弱勢(-15)，讀取 stock_technical_signals 表
- [x] 3.4 實作 2 個風險訊號：高溢價風險(-15), 到期逼近(-10)
- [x] 3.5 實作標準化公式 `((raw_score + 40) / 140) * 100` 和風險分級（強力套利/套利機會/觀望/風險警示）
- [x] 3.6 實作批次訊號計算並寫入 `convertible_bond_signals` 表
- [x] 3.7 實作 underlying_stock_id → cb_arbitrage_score 映射（同一標的多檔 CB 取最高分）

## 4. 批次更新整合

- [x] 4.1 在 `twstock/all.py` 新增 `get_all_convertible_bonds_parallel()` 方法：清單同步 → 智慧更新 → 訊號計算
- [x] 4.2 在 `main.py` 或主要入口整合 CB 批次更新流程，使其每日執行
- [x] 4.3 在 Excel 報表新增「可轉債套利」sheet（第一個 sheet），欄位：CB代碼、名稱、標的股、收盤價、轉換價、溢價率、套利空間、套利評分、訊號數、風險等級、主要訊號

## 5. API 層

- [x] 5.1 建立 `api/models/convertible.py`，定義 Pydantic models：ConvertibleBondListItem, ConvertibleBondDetail, ConvertibleBondHistoryPoint, ConvertibleBondListResponse
- [x] 5.2 建立 `api/routers/convertible.py`，實作 `GET /api/convertible`（列表，支援 sort_by/order/limit/offset/active_only）
- [x] 5.3 實作 `GET /api/convertible/{bond_id}`（詳情，含完整訊號列表）
- [x] 5.4 實作 `GET /api/convertible/{bond_id}/history`（歷史趨勢，預設 90 天）
- [x] 5.5 實作 `GET /api/stocks/{stock_id}/convertible`（股票關聯 CB 查詢）
- [x] 5.6 在 `api/main.py` 註冊 convertible router
- [x] 5.7 修改 `StockListItem` model 新增 `cb_arbitrage_score: Optional[int]` 欄位
- [x] 5.8 修改 `StockService.get_stock_list()` 在查詢時 LEFT JOIN `convertible_bond_signals` 取得 cb_arbitrage_score
- [x] 5.9 在股票列表 API 新增 `sort_by=cb_arbitrage_score` 排序支援（null 值置底）

## 6. 前端 - 可轉債總表

- [x] 6.1 建立 `dashboard/src/pages/ConvertiblePage.tsx`：CB 列表頁面，表格含所有欄位（代碼、名稱、標的股、收盤價、轉換價、轉換價值、溢價率、套利空間、套利評分、訊號數、風險等級、到期日、成交量）
- [x] 6.2 實作排序功能（點擊欄位標題切換排序，URL 參數同步）
- [x] 6.3 實作評分色彩（80+綠色、60+淺綠、40+黃色、<40 紅色）和溢價率色彩（<0%深綠、0-5%淺綠、5-15%灰色、>15%紅色）
- [x] 6.4 建立 `dashboard/src/pages/ConvertibleDetailPage.tsx`：CB 詳情頁，含基本資訊、套利指標卡片、訊號列表、歷史趨勢圖

## 7. 前端 - 股票總表整合

- [x] 7.1 修改 `dashboard/src/types/stock.ts` 新增 `cb_arbitrage_score?: number | null` 欄位
- [x] 7.2 修改 `dashboard/src/pages/HomePage.tsx` 表格新增「CB 套利」欄位，顯示分數/色彩/「-」
- [x] 7.3 實作 CB 分數點擊導航到可轉債總表
- [x] 7.4 新增 `cb_arbitrage_score` 排序支援（null 值置底）

## 8. 前端 - 導航與路由

- [x] 8.1 修改 `dashboard/src/App.tsx` 新增路由：`/convertible` → ConvertiblePage, `/convertible/:bondId` → ConvertibleDetailPage
- [x] 8.2 修改頂部導航新增「可轉債」入口，與「股票」並列
- [x] 8.3 在 `dashboard/src/lib/api.ts` 新增 CB API 函數：fetchConvertibleBonds, fetchConvertibleBondDetail, fetchConvertibleBondHistory

## 9. 測試與驗證

- [ ] 9.1 驗證 FinMind API 資料品質：CB 清單完整性、每日資料正確性
- [ ] 9.2 驗證套利訊號計算：手動驗算幾支 CB 的訊號觸發和分數
- [ ] 9.3 驗證股票總表 cb_arbitrage_score 顯示正確（有 CB 的股票顯示分數，無 CB 的顯示「-」）
- [ ] 9.4 端到端驗證：從 API 抓資料 → DB 儲存 → 訊號計算 → API 回傳 → 前端顯示
