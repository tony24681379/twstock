## Why

WantgooFetcher 目前身兼多職：HTTP 請求、資料解析、資料庫持久化、Header 管理、API 日期追蹤。這導致 714 行的單一類別無法單元測試、新爬蟲（如 ConvertibleBondFetcher）被迫複製相同的耦合模式、All 類膨脹到 1577 行且有 3 處重複的並發邏輯。重構的目的是分離關注點，使系統可測試、可維護、可擴展。

## What Changes

- **分離資料抓取與持久化**：移除 WantgooFetcher 中 14 處 `if save_to_db and self.db_manager` 模式，Fetcher 只回傳資料，持久化由調用層負責
- **提取 Header 管理**：將 Playwright 初始化、Header 刷新、401/403 重試邏輯從 WantgooFetcher 抽出為獨立的 HeaderManager
- **統一批量並發模式**：提取 All 類中 3 處重複的 `Semaphore + as_completed + 進度顯示` 為通用 BatchExecutor
- **改善重試策略**：將固定 1 秒退避改為指數退避（exponential backoff with jitter）
- **統一資源初始化**：簡化 Stock/All/FetcherManager 之間多層級的懶初始化邏輯

## Capabilities

### New Capabilities
- `fetcher-core`: Fetcher 核心抽象與 HTTP 層，定義資料抓取的介面契約（純資料回傳，不涉及持久化）
- `header-management`: Header 生命週期管理，包含 Playwright 初始化、快取、刷新策略、Fallback 機制
- `batch-executor`: 通用批量並發執行器，統一 Semaphore 控制、進度回報、錯誤收集

### Modified Capabilities
（無既有 spec 的行為需求變更，此次重構為內部架構改善）

## Impact

- **受影響檔案**：
  - `twstock/wantgoo.py` — 大幅重構，拆分為多個模組
  - `twstock/wantgoo_initializer.py` — 整合進 HeaderManager
  - `twstock/fetcher_manager.py` — 簡化初始化邏輯
  - `twstock/stock.py` — 調整 Fetcher 調用方式（不再傳 save_to_db）
  - `twstock/all.py` — 使用 BatchExecutor 取代重複的並發邏輯
  - `twstock/convertible_fetcher.py` — 改用新的 Fetcher 基類
- **API 行為不變**：外部 API 端點和資料格式維持不變
- **資料庫 Schema 不變**：無 migration 需求
- **依賴**：可能新增 `tenacity` 或自行實作 backoff 工具
