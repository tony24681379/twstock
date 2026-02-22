## 1. Database Schema 擴充

- [x] 1.1 在 `StockUpdateTracker` Model 新增 `revenue_checked_at`、`concentration_checked_at`、`info_checked_at` 三個 nullable DateTime 欄位
- [x] 1.2 在 `init_database()` 中新增 `ALTER TABLE stock_update_tracker ADD COLUMN IF NOT EXISTS` 語句（三個欄位）
- [x] 1.3 `update_tracker()` 方法支援三個新欄位的讀寫（kwargs 解析 + INSERT/UPDATE 語句）

## 2. 通用 checked_at 方法

- [x] 2.1 新增 `bulk_update_checked_at(stock_ids, field)` 方法，含白名單驗證（僅接受三個合法欄位名），空清單直接回傳
- [x] 2.2 實作批次 UPSERT 邏輯：對 `stock_update_tracker` 批次更新指定欄位為當前時間

## 3. 月營收快取修復

- [x] 3.1 重寫 `bulk_check_monthly_revenue_needs_update()`：改用 `revenue_checked_at >= today` 判斷，移除月份比較邏輯
- [x] 3.2 `all.py` 月營收更新流程結尾呼叫 `bulk_update_checked_at(stock_ids, 'revenue_checked_at')`

## 4. 集中度檢查標準化

- [x] 4.1 新增 `bulk_check_concentration_needs_update()` 到 `database.py`：實作三重判斷（無資料 → 需要、今日已檢查 → 不需要、資料超過 7 天 → 需要）
- [x] 4.2 `all.py` 中將集中度 inline 檢查邏輯替換為呼叫 `bulk_check_concentration_needs_update()`
- [x] 4.3 `all.py` 集中度更新流程結尾呼叫 `bulk_update_checked_at(stock_ids, 'concentration_checked_at')`

## 5. 基本資訊鮮度判斷

- [x] 5.1 `stock.py` 的 `load_data()` 中將 `info_loaded` 判斷改為 `info_checked_at` 鮮度判斷（NULL 或超過 7 天才重新抓取）
- [x] 5.2 抓取完成後呼叫 `update_tracker()` 更新 `info_checked_at`

## 6. Save 方法改為 UPSERT

- [x] 6.1 `save_stock_info()`（L596）：`ON CONFLICT (stock_id) DO NOTHING` → `DO UPDATE SET` 更新所有欄位
- [x] 6.2 EPS INSERT（L663）：`ON CONFLICT (stock_id, year, quarter) DO NOTHING` → `DO UPDATE SET` 更新 eps 值
- [x] 6.3 `save_monthly_revenue()`（L1047）：`ON CONFLICT (stock_id, year, month) DO NOTHING` → `DO UPDATE SET` 更新營收欄位
- [x] 6.4 確認 `save_concentration_data()` 維持 `DO NOTHING` 不變（無需修改，僅驗證）

## 7. 驗證

- [x] 7.1 執行完整流程測試：`poetry run python main.py`，確認月營收不再每次都呼叫 ~2700 次 API
- [x] 7.2 二次執行測試：確認月營收、集中度皆因 `checked_at` 跳過更新，info 因 7 天內跳過
