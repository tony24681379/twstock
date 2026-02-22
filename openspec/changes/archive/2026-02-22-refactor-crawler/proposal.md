## Why

目前爬蟲的智慧快取機制只有日線資料正常運作，其他三種資料類型都有問題：

1. **月營收**：判斷邏輯 `db_month < current_month` 永遠為 True（營收資料滯後 1+ 月），導致每次執行都對 ~2700 支股票重新呼叫 API
2. **籌碼集中度**：檢查邏輯內嵌在 `all.py:324-335`，不在 `database.py`，無法複用且不一致
3. **股票基本資訊/EPS/股利**：`info_loaded=True` 後永不更新，EPS 季度變動抓不到

此外，`save_stock_info()`、`save_monthly_revenue()`、EPS INSERT 都使用 `ON CONFLICT DO NOTHING`，即使呼叫了 API 也無法覆蓋舊資料。

統一修復可以：
- 月營收從每次 ~2700 支 API 呼叫降到 0（當日已檢查），節省 2-3 分鐘
- EPS/股利等季度/年度資料能自動偵測並更新
- 所有資料類型的更新邏輯一致，降低維護成本

## What Changes

### 核心原則
**無論 API 有無新資料，都記錄「已檢查」時間戳（`*_checked_at`），避免同日重複呼叫。**

### 具體改動

- `stock_update_tracker` 新增 3 個欄位：`revenue_checked_at`、`concentration_checked_at`、`info_checked_at`
- 重寫 `database.py` 的 `bulk_check_monthly_revenue_needs_update()`：改用 `revenue_checked_at >= today` 判斷，不再用月份比較
- 新增 `database.py` 的 `bulk_check_concentration_needs_update()`：從 `all.py` inline 邏輯提取為標準化批次檢查
- `stock.py` 的 `load_data()` 中 info 載入邏輯加鮮度判斷：`info_checked_at` 超過 7 天才重新抓取
- `save_stock_info()`、EPS INSERT、`save_monthly_revenue()` 的 `ON CONFLICT DO NOTHING` 改為 `DO UPDATE`
- 新增通用 `bulk_update_checked_at(stock_ids, field)` 方法
- `update_tracker()` 支援 3 個新欄位
- `all.py` 月營收和集中度流程末尾加 checked_at 標記

## Capabilities

### New Capabilities
（無新增 capability，此為現有能力的統一和補強）

### Modified Capabilities
- `fetcher-core`: 統一所有資料類型的更新判斷邏輯，新增批次鮮度檢查方法，修正 save 方法為 UPSERT

## Impact

- **程式碼**：`twstock/database.py`（Model 擴充 + 3 個方法修改 + 2 個新增方法）、`twstock/all.py`（月營收 + 集中度流程調整）、`twstock/stock.py`（info 鮮度判斷）
- **資料庫**：`stock_update_tracker` 新增 3 個 nullable DateTime 欄位（`ALTER TABLE ADD COLUMN IF NOT EXISTS`，冪等操作）
- **API 呼叫量**：月營收大幅減少（~2700 → 0/次），info 略增（每 7 天一次）
- **效能**：新增的批次查詢為單一 SQL，影響可忽略；月營收省下 2-3 分鐘執行時間
