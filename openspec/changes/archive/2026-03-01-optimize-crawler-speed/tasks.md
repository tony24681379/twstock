## 1. database.py — bulk_check_cb_needs_update 改造

- [x] 1.1 `bulk_check_cb_needs_update()` 新增 `reference_date: Optional[date] = None` 參數，當提供時用 `reference_date` 取代 `today` 作比較基準
- [x] 1.2 無 `reference_date` 時 fallback 到 `date.today()`（向後相容）

## 2. convertible_bond_updater.py — 接入智慧更新

- [x] 2.1 `update_convertible_bonds()` 新增 `api_latest_date=None` 可選參數
- [x] 2.2 在 Step 3（取得每日交易資料）前，呼叫 `db_manager.bulk_check_cb_needs_update(bond_ids, reference_date=api_latest_date)` 篩出需要更新的 CB
- [x] 2.3 只對需要更新的 CB 呼叫 `cb_fetcher.fetch_all_cb_daily()`，跳過的 CB 從 DB 載入最新 daily 資料（用 `bulk_load_convertible_bond_daily()`）
- [x] 2.4 合併 API 回傳的 daily 與 DB 載入的 daily，確保訊號計算使用完整資料
- [x] 2.5 印出跳過/更新數量的 log（如 `✓ 80 檔已是最新，20 檔需更新`）

## 3. main.py — 傳遞 api_latest_date

- [x] 3.1 呼叫 `update_convertible_bonds(db_manager, api_latest_date=all.db_manager.api_latest_date)` 傳入已探測的最新交易日期
