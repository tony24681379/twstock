# 可轉債資料爬取與儲存規格

## ADDED Requirements

### Requirement: 可轉債清單取得
系統 SHALL 取得台灣市場所有流通中的可轉換公司債清單。

#### Scenario: 從 FinMind API 取得 CB 清單
- **WHEN** 系統執行可轉債清單同步
- **THEN** 呼叫 FinMind `TaiwanStockConvertibleBondInfo` dataset 取得所有 CB 基本資訊
- **THEN** 每筆 CB 記錄包含：bond_id（代碼）、name（名稱）、underlying_stock_id（標的股票代碼）、conversion_price（轉換價）、issue_date（發行日）、maturity_date（到期日）、put_date（賣回日）、put_price（賣回價）、issued_amount（發行總額）

#### Scenario: CB 清單變動處理
- **WHEN** API 回傳的 CB 清單與資料庫不一致
- **THEN** 新增的 CB 標記 `is_active = true` 並寫入資料庫
- **THEN** 已到期或下市的 CB 標記 `is_active = false`，保留歷史資料

#### Scenario: FinMind API 不可用
- **WHEN** FinMind API 回應非 200 狀態碼或 timeout
- **THEN** 系統記錄錯誤訊息並跳過本次 CB 更新
- **THEN** 不影響現有股票資料更新流程

### Requirement: 可轉債每日交易資料
系統 SHALL 每日取得所有流通 CB 的交易資料。

#### Scenario: 取得每日交易資料
- **WHEN** 系統執行 CB 每日更新
- **THEN** 呼叫 FinMind `TaiwanStockConvertibleBondDaily` dataset
- **THEN** 每筆記錄包含：bond_id、date、open、high、low、close（CB 價格，以面額 100 為基準）、volume（成交量）

#### Scenario: 計算衍生欄位
- **WHEN** 系統取得 CB 每日價格
- **THEN** 系統計算以下衍生欄位：
  - `conversion_shares = 100000 / conversion_price`（轉換張數，面額 10 萬元）
  - `conversion_value = conversion_shares * underlying_close * 1000`（轉換價值）
  - `premium_rate = (cb_close - conversion_value) / conversion_value * 100`（轉換溢價率 %）
  - `arbitrage_spread = -premium_rate - 0.6`（套利空間 %，扣除約 0.6% 交易成本）

#### Scenario: 取得標的股收盤價
- **WHEN** 計算轉換價值需要標的股收盤價
- **THEN** 從 `stock_daily` 表讀取對應 `underlying_stock_id` 的當日收盤價
- **THEN** 若標的股當日無資料，使用最近一個交易日收盤價

#### Scenario: 智慧更新（僅更新過時資料）
- **WHEN** 系統執行 CB 每日更新
- **THEN** 先批次查詢 `convertible_bond_daily` 表中每支 CB 的最新日期
- **THEN** 僅更新 DB 日期 < API 最新日期的 CB
- **THEN** 首次執行抓取 250 天歷史，後續僅抓取增量

### Requirement: 資料庫表結構
系統 SHALL 使用 PostgreSQL 儲存可轉債資料。

#### Scenario: convertible_bond 表
- **WHEN** 系統初始化資料庫
- **THEN** 建立 `convertible_bond` 表，包含以下欄位：
  - `bond_id` (String, PK) - 可轉債代碼
  - `name` (String) - 可轉債名稱
  - `underlying_stock_id` (String, indexed) - 標的股票代碼
  - `conversion_price` (Float) - 轉換價格
  - `issue_date` (DateTime) - 發行日期
  - `maturity_date` (DateTime) - 到期日期
  - `put_date` (DateTime, nullable) - 下次賣回日
  - `put_price` (Float, nullable) - 賣回價格
  - `coupon_rate` (Float) - 票面利率
  - `issued_amount` (Float) - 發行總額（張）
  - `outstanding_amount` (Float) - 流通在外餘額（張）
  - `is_active` (Boolean, default=True) - 是否仍在交易
  - `updated_at` (DateTime) - 最後更新時間

#### Scenario: convertible_bond_daily 表
- **WHEN** 系統初始化資料庫
- **THEN** 建立 `convertible_bond_daily` 表，包含以下欄位：
  - `id` (BigInteger, PK, auto)
  - `bond_id` (String, indexed)
  - `date` (DateTime)
  - `open` (Float)
  - `high` (Float)
  - `low` (Float)
  - `close` (Float) - CB 收盤價（面額基準）
  - `volume` (Float) - 成交量
  - `underlying_close` (Float) - 標的股收盤價
  - `conversion_value` (Float) - 轉換價值
  - `premium_rate` (Float) - 溢價率 (%)
  - `arbitrage_spread` (Float) - 套利空間 (%)
  - 複合唯一索引：`(bond_id, date)`

#### Scenario: 批次寫入
- **WHEN** 寫入 CB 每日資料
- **THEN** 使用 `ON CONFLICT (bond_id, date) DO UPDATE` 策略（幂等操作）
- **THEN** 單次 INSERT 批次處理所有記錄（與現有 `save_daily_data` 模式一致）

### Requirement: 批次更新效能
系統 SHALL 在合理時間內完成所有 CB 資料更新。

#### Scenario: 首次執行
- **WHEN** 資料庫無 CB 資料，首次執行完整抓取
- **THEN** 150 檔 CB × 250 天歷史資料，完成時間 SHALL < 2 分鐘

#### Scenario: 增量更新
- **WHEN** 資料庫已有前一日資料
- **THEN** 僅抓取需更新的 CB（通常全部），完成時間 SHALL < 30 秒

#### Scenario: 並行限流
- **WHEN** 並行抓取多支 CB 資料
- **THEN** 使用 asyncio.Semaphore 限制並行數為 `MAX_API_WORKERS`（預設 10）
- **THEN** 遵守 FinMind API 600 req/hr 限制
