## Why

`update_convertible_bonds()` 每次執行都重新抓取所有 CB 的每日交易資料（約 100+ 檔，每檔 1 次 HTTP POST + 0.3s delay），即使 DB 已有最新資料也會重抓。database.py 已有 `bulk_check_cb_needs_update()` 方法但完全未被使用。

stock_daily 有 `api_latest_date` 全局探測機制，只更新過時的股票（2600 支中通常只需更新 30-50 支）。CB 應比照此模式，避免每次都重來。

## What Changes

- 在 `update_convertible_bonds()` 中接入 `bulk_check_cb_needs_update()` 檢查，跳過已有最新資料的 CB
- 將 `bulk_check_cb_needs_update()` 的比較基準從 `db_date < today` 改為 `db_date < api_latest_date`（使用 stock_daily 已探測到的最新交易日期），避免假日/週末誤判
- 只對需要更新的 CB 呼叫 `fetch_convertible_bond_daily()`，其餘從 DB 載入

## Capabilities

### New Capabilities

（無新 capability，此為既有 capability 的效能改善）

### Modified Capabilities

- `convertible-bond-data`: 增加智能快取邏輯，CB 每日資料更新前先比對 DB 日期與 API 最新交易日期

## Impact

- **修改檔案**：
  - `twstock/convertible_bond_updater.py` — 加入 `bulk_check_cb_needs_update()` 檢查流程
  - `twstock/database.py` — 調整 `bulk_check_cb_needs_update()` 支援 `api_latest_date` 參數
- **預期效果**：CB 更新從每次 ~40 秒（100+ 檔 × 0.3s delay）降至 <2 秒（同日第二次執行、或 DB 已有最新交易日資料時）
- **無 breaking change**：行為不變，僅跳過不必要的 API 呼叫
