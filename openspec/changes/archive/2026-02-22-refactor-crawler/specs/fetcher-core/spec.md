## ADDED Requirements

### Requirement: stock_update_tracker 支援三種 checked_at 欄位
`stock_update_tracker` 表 SHALL 包含 `revenue_checked_at`、`concentration_checked_at`、`info_checked_at` 三個 nullable DateTime 欄位。這些欄位記錄「最後一次檢查 API」的時間，無論 API 是否回傳新資料。

#### Scenario: 初始化時自動建立欄位
- **WHEN** `init_database()` 執行
- **THEN** `stock_update_tracker` 表存在 `revenue_checked_at`、`concentration_checked_at`、`info_checked_at` 三個欄位（使用 `ALTER TABLE ADD COLUMN IF NOT EXISTS`，冪等操作）

#### Scenario: 欄位預設為 NULL
- **WHEN** 新股票首次被寫入 `stock_update_tracker`
- **THEN** 三個 `*_checked_at` 欄位的值為 NULL，代表從未檢查過

### Requirement: 月營收使用 checked_at 判斷是否需要更新
`bulk_check_monthly_revenue_needs_update()` SHALL 以 `revenue_checked_at >= today` 作為「不需要更新」的判斷依據，取代原有的月份比較邏輯。

#### Scenario: 今日已檢查過
- **WHEN** 某股票的 `revenue_checked_at` 為今日日期
- **THEN** 該股票不在需要更新的清單中

#### Scenario: 今日尚未檢查
- **WHEN** 某股票的 `revenue_checked_at` 為 NULL 或早於今日
- **THEN** 該股票出現在需要更新的清單中

#### Scenario: 批次查詢效能
- **WHEN** 傳入 ~2700 支股票代碼
- **THEN** 使用單一 SQL 查詢完成判斷，回傳需要更新的股票代碼清單

### Requirement: 集中度使用標準化批次檢查
`bulk_check_concentration_needs_update()` SHALL 存在於 `database.py` 中（而非 inline 在 `all.py`），使用 `concentration_checked_at` 搭配資料鮮度的雙重判斷。

#### Scenario: 今日已檢查過
- **WHEN** 某股票的 `concentration_checked_at` 為今日日期
- **THEN** 該股票不需要更新

#### Scenario: DB 無集中度資料
- **WHEN** 某股票在 `concentration_data` 表中無任何記錄
- **THEN** 該股票需要更新

#### Scenario: DB 最新集中度資料超過 7 天
- **WHEN** 某股票的最新集中度記錄日期距今超過 7 天，且 `concentration_checked_at` 非今日
- **THEN** 該股票需要更新

#### Scenario: DB 資料在 7 天內且今日已檢查
- **WHEN** 某股票最新集中度資料距今不超過 7 天，且 `concentration_checked_at` 為今日
- **THEN** 該股票不需要更新

### Requirement: 基本資訊使用 7 天鮮度判斷
`stock.py` 的 `load_data()` 中 info 載入邏輯 SHALL 檢查 `info_checked_at`，超過 7 天才重新抓取基本資訊（含 EPS、股利）。

#### Scenario: 首次載入（無 checked_at 記錄）
- **WHEN** 某股票的 `info_checked_at` 為 NULL
- **THEN** 從 API 抓取基本資訊並更新 `info_checked_at`

#### Scenario: 7 天內已檢查過
- **WHEN** 某股票的 `info_checked_at` 距今不超過 7 天
- **THEN** 直接從 DB 載入，不呼叫 API

#### Scenario: 超過 7 天未檢查
- **WHEN** 某股票的 `info_checked_at` 距今超過 7 天
- **THEN** 從 API 重新抓取基本資訊並更新 `info_checked_at`

### Requirement: save 方法使用 UPSERT 覆蓋舊資料
`save_stock_info()`、EPS INSERT、`save_monthly_revenue()` SHALL 使用 `ON CONFLICT DO UPDATE` 取代 `ON CONFLICT DO NOTHING`，讓修正後的資料能正確覆蓋舊值。

#### Scenario: 月營收初估值被確定值覆蓋
- **WHEN** 同一支股票同一月份的月營收已存在（初估值），API 回傳確定值
- **THEN** 資料庫中的該筆記錄被更新為確定值

#### Scenario: EPS 調整後覆蓋
- **WHEN** 同一支股票同一季度的 EPS 已存在，API 回傳調整後的值
- **THEN** 資料庫中的該筆 EPS 記錄被更新為新值

#### Scenario: 股票基本資訊更新
- **WHEN** 同一支股票的基本資訊已存在，API 回傳新的資本額或流通股數
- **THEN** 資料庫中的基本資訊被更新

#### Scenario: 集中度保持 DO NOTHING
- **WHEN** `save_concentration_data()` 被呼叫
- **THEN** 維持 `ON CONFLICT DO NOTHING` 行為不變（集中度為週快照，不會被修正）

### Requirement: 通用批次 checked_at 更新方法
`bulk_update_checked_at(stock_ids, field)` SHALL 為通用方法，批次更新指定欄位的 checked_at 時間戳。`field` 參數 MUST 使用白名單驗證，僅接受 `revenue_checked_at`、`concentration_checked_at`、`info_checked_at`。

#### Scenario: 批次標記月營收已檢查
- **WHEN** 呼叫 `bulk_update_checked_at(stock_ids, 'revenue_checked_at')`
- **THEN** 指定股票的 `revenue_checked_at` 被設為當前時間

#### Scenario: 無效欄位名稱被拒絕
- **WHEN** 呼叫 `bulk_update_checked_at(stock_ids, 'invalid_field')`
- **THEN** 拋出 `ValueError`，不執行任何 SQL

#### Scenario: 空清單不執行 SQL
- **WHEN** 呼叫 `bulk_update_checked_at([], 'revenue_checked_at')`
- **THEN** 不執行任何資料庫操作，直接回傳

### Requirement: 更新流程結尾標記 checked_at
`all.py` 的月營收和集中度更新流程 SHALL 在 API 呼叫完成後（無論是否有新資料），呼叫 `bulk_update_checked_at()` 標記所有已處理的股票。

#### Scenario: 月營收更新流程結尾
- **WHEN** 月營收批次更新流程完成（含成功和失敗的股票）
- **THEN** 對所有嘗試更新的股票呼叫 `bulk_update_checked_at(stock_ids, 'revenue_checked_at')`

#### Scenario: 集中度更新流程結尾
- **WHEN** 集中度批次更新流程完成
- **THEN** 對所有嘗試更新的股票呼叫 `bulk_update_checked_at(stock_ids, 'concentration_checked_at')`
