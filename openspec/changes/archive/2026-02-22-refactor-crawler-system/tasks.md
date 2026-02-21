## 1. HeaderManager 提取

- [x] 1.1 建立 `twstock/header_manager.py`，實作 `HeaderManager` 類：`get_headers()`, `refresh()`, `api_latest_date` 屬性，asyncio.Lock 並發保護，5 秒防抖
- [x] 1.2 將 `wantgoo.py` 中 `_initialize_headers()` 和 `_refresh_headers()` 的邏輯遷移到 HeaderManager，保留 WantgooInitializer 作為 Playwright 策略
- [x] 1.3 HeaderManager 初始化後自動探測 API 最新日期（透過 2330 candlestick API）

## 2. BatchExecutor 提取

- [x] 2.1 建立 `twstock/batch_executor.py`，實作 `batch_execute()` async 函式：Semaphore 並發控制、進度回報、錯誤隔離、執行摘要
- [x] 2.2 Worker 簽名定義為 `Callable[[str], Awaitable[tuple[str, Any]]]`，失敗項目不包含在結果字典中

## 3. WantgooFetcher 重構

- [x] 3.1 修改 `WantgooFetcher.__init__` 接受 `header_manager: HeaderManager`，移除 `db_manager` 參數
- [x] 3.2 修改 `fetch_url()` 使用 `self.header_manager.get_headers()` 取得 headers，401/403 時呼叫 `self.header_manager.refresh()`
- [x] 3.3 `fetch_url()` 重試等待改為指數退避：`delay = min(0.5 * 2^retry_i + random(0, 0.5), 10)`
- [x] 3.4 `fetch_info()` 移除 `save_to_db` 參數和所有 DB 寫入邏輯，回傳 `(pd.Series, list)`
- [x] 3.5 `fetch_daily()` 移除 `save_to_db` 和 `total_stock` 參數，只回傳 `pd.DataFrame`
- [x] 3.6 `fetch_concentration_data()` 移除 `save_to_db` 參數，只回傳 `pd.DataFrame`
- [x] 3.7 `fetch_monthly_revenue()` 移除 `save_to_db` 參數，只回傳 `pd.DataFrame`
- [x] 3.8 移除 `wantgoo.py` 中對 `DatabaseManager` 的所有 import 和引用

## 4. FetcherManager 簡化

- [x] 4.1 重構 `get_global_fetcher()`：建立 HeaderManager → 注入 WantgooFetcher，不再將 db_manager 傳給 fetcher
- [x] 4.2 `get_db_manager()` 獨立管理，初始化時從 HeaderManager 取得 `api_latest_date` 設定到 db_manager

## 5. Stock 類調整

- [x] 5.1 `Stock.__init__` 改為接受 `fetcher` 和 `db_manager` 兩個獨立參數（不再從 fetcher.db_manager 間接取得）
- [x] 5.2 `Stock._ensure_resources()` 分別呼叫 `get_global_fetcher()` 和 `get_db_manager()`
- [x] 5.3 `Stock.load_data()` 改為：先 `data = await fetcher.fetch_daily(...)`，再 `await db_manager.save_daily_data(sid, data)`

## 6. All 類調整

- [x] 6.1 API 更新邏輯（原第 216-260 行）改用 `batch_execute()`，worker 內呼叫 `Stock.load_data()`
- [x] 6.2 月營收更新邏輯（原第 296-353 行）改用 `batch_execute()`，worker 內呼叫 `fetcher.fetch_monthly_revenue()` + `db_manager.save_monthly_revenue()`
- [x] 6.3 集中度更新邏輯（原第 421-467 行）改用 `batch_execute()`，worker 內呼叫 `fetcher.fetch_concentration_data()` + `db_manager.save_concentration_data()`
- [x] 6.4 All 類持有獨立的 `db_manager` 參照，不再透過 `self.fetcher.db_manager` 存取

## 7. ConvertibleBondFetcher 調整

- [x] 7.1 `ConvertibleBondFetcher.__init__` 移除 `db_manager` 參數，改為純資料回傳模式
- [x] 7.2 調整所有呼叫 ConvertibleBondFetcher 的地方，fetch 後由調用層負責持久化

## 8. 驗證與清理

- [x] 8.1 所有模組 import 驗證通過（完整流程需啟動 DB 測試）
- [x] 8.2 確認 API 端點（FastAPI）行為不變，API 層不使用 WantgooFetcher
- [x] 8.3 移除 `wantgoo.py` 中殘留的 `BaseFetcher` 空類別（已無用途）
