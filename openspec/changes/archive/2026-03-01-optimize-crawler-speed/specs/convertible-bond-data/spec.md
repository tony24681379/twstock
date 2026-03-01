## MODIFIED Requirements

### Requirement: 可轉債每日交易資料
系統 SHALL 每日取得所有流通 CB 的交易資料。

#### Scenario: 取得每日交易資料
- **WHEN** 系統執行 CB 每日更新
- **THEN** 呼叫 TPEX cbDayQry endpoint
- **THEN** 每筆記錄包含：bond_id、date、open、high、low、close（CB 價格，以面額 100 為基準）、volume（成交量）

#### Scenario: 計算衍生欄位
- **WHEN** 系統取得 CB 每日價格
- **THEN** 系統計算以下衍生欄位：
  - `conversion_shares = 100000 / conversion_price`（轉換張數，面額 10 萬元）
  - `conversion_value = conversion_shares * underlying_close / 1000`（轉換價值，換回百元面額基準）
  - `premium_rate = (cb_close - conversion_value) / conversion_value * 100`（轉換溢價率 %）
  - `arbitrage_spread = -premium_rate - 0.6`（套利空間 %，扣除約 0.6% 交易成本）

#### Scenario: 取得標的股收盤價
- **WHEN** 計算轉換價值需要標的股收盤價
- **THEN** 從 `stock_daily` 表讀取對應 `underlying_stock_id` 的當日收盤價
- **THEN** 若標的股當日無資料，從 TWSE/TPEX Open Data 補齊最新收盤價

#### Scenario: 智慧更新（僅更新過時資料）
- **WHEN** 系統執行 CB 每日更新
- **THEN** 呼叫 `bulk_check_cb_needs_update()` 批次查詢 `convertible_bond_daily` 表中每支 CB 的最新日期
- **THEN** 以 `api_latest_date`（由 HeaderManager 探測的最新交易日期）為基準，僅更新 DB 日期 < `api_latest_date` 的 CB
- **THEN** DB 已有最新交易日資料的 CB SHALL 跳過 API 呼叫
- **THEN** 跳過 API 的 CB 仍從 DB 載入最新一筆 daily 資料，用於訊號計算

#### Scenario: api_latest_date 不可用時 fallback
- **WHEN** `api_latest_date` 為 None（HeaderManager 探測失敗或未初始化）
- **THEN** 以 `date.today()` 為基準比較（與未優化前行為一致）

### Requirement: 批次更新效能
系統 SHALL 在合理時間內完成所有 CB 資料更新。

#### Scenario: 首次執行
- **WHEN** 資料庫無 CB 資料，首次執行完整抓取
- **THEN** 100+ 檔 CB，完成時間 SHALL < 2 分鐘

#### Scenario: 增量更新（有過時資料）
- **WHEN** 資料庫已有資料但部分 CB 過時
- **THEN** 僅對過時的 CB 呼叫 API，完成時間 SHALL < 30 秒

#### Scenario: 無需更新（DB 已是最新）
- **WHEN** 所有 CB 的 DB 最新日期 >= `api_latest_date`
- **THEN** 跳過所有 API 呼叫，僅從 DB 載入資料
- **THEN** 完成時間 SHALL < 2 秒

#### Scenario: 並行限流
- **WHEN** 並行抓取多支 CB 資料
- **THEN** 使用 asyncio.Semaphore 限制並行數為 2（TPEX 安全限制）
- **THEN** 每次請求間隔 0.3 秒
