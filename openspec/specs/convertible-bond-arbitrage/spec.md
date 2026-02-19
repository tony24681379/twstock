# 可轉債套利訊號計算引擎規格

## ADDED Requirements

### Requirement: 套利訊號定義
系統 SHALL 計算 10 個可轉債套利訊號，每個訊號有明確觸發條件和分數。

#### Scenario: 折價套利訊號（+20）
- **WHEN** 轉換價值 > CB 市價 × 1.006（含 0.6% 交易成本）
- **THEN** 觸發「折價套利」訊號，分數 +20
- **THEN** 描述包含：套利空間百分比

#### Scenario: 低溢價率訊號（+15）
- **WHEN** 溢價率 < 5% 且標的股 overall_strength > 50
- **THEN** 觸發「低溢價率」訊號，分數 +15
- **THEN** 描述包含：當前溢價率和標的股強度

#### Scenario: 催換在即訊號（+15）
- **WHEN** 標的股收盤價 > 轉換價 × 1.3
- **THEN** 觸發「催換在即」訊號，分數 +15
- **THEN** 描述包含：標的股價與轉換價的比率

#### Scenario: 到期賣回保護訊號（+12）
- **WHEN** 距下次賣回日 < 180 天且賣回價 > CB 市價
- **THEN** 觸發「到期賣回保護」訊號，分數 +12
- **THEN** 描述包含：賣回日期和預期報酬

#### Scenario: 深度價內訊號（+10）
- **WHEN** 標的股收盤價 > 轉換價 × 1.5
- **THEN** 觸發「深度價內」訊號，分數 +10
- **THEN** 描述包含：價內深度百分比

#### Scenario: 低於面額訊號（+8）
- **WHEN** CB 收盤價 < 100（面額）
- **THEN** 觸發「低於面額」訊號，分數 +8
- **THEN** 描述包含：折價幅度

#### Scenario: 標的股強勢訊號（+12）
- **WHEN** 標的股 overall_strength > 70
- **THEN** 觸發「標的股強勢」訊號，分數 +12
- **THEN** 描述包含：標的股強度評分

#### Scenario: 標的股弱勢訊號（-15）
- **WHEN** 標的股 overall_strength < 30
- **THEN** 觸發「標的股弱勢」訊號，分數 -15
- **THEN** 描述包含：標的股強度評分

#### Scenario: 高溢價風險訊號（-15）
- **WHEN** 溢價率 > 20%
- **THEN** 觸發「高溢價風險」訊號，分數 -15
- **THEN** 描述包含：當前溢價率

#### Scenario: 到期逼近訊號（-10）
- **WHEN** 距到期日 < 90 天且溢價率 > 10%
- **THEN** 觸發「到期逼近」訊號，分數 -10
- **THEN** 描述包含：剩餘天數和溢價率

### Requirement: 訊號評分標準化
系統 SHALL 將原始訊號分數標準化為 0-100 分。

#### Scenario: 標準化公式
- **WHEN** 計算完所有訊號的原始分數總和
- **THEN** 使用公式：`normalized = ((raw_score + 40) / 140) * 100`
- **THEN** 結果取整數，限制在 0-100 範圍內（`max(0, min(100, normalized))`）

#### Scenario: 最高分情境
- **WHEN** 所有正向訊號全部觸發（+20+15+15+12+10+8+12 = +92）
- **THEN** 標準化分數 ≈ 94

#### Scenario: 最低分情境
- **WHEN** 所有負向訊號全部觸發（-15-15-10 = -40）
- **THEN** 標準化分數 = 0

### Requirement: 訊號分級
系統 SHALL 根據標準化分數將套利機會分級。

#### Scenario: 強力套利（80-100）
- **WHEN** 標準化分數 >= 80
- **THEN** 風險等級為「強力套利」

#### Scenario: 套利機會（60-79）
- **WHEN** 標準化分數 >= 60 且 < 80
- **THEN** 風險等級為「套利機會」

#### Scenario: 觀望（40-59）
- **WHEN** 標準化分數 >= 40 且 < 60
- **THEN** 風險等級為「觀望」

#### Scenario: 風險警示（0-39）
- **WHEN** 標準化分數 < 40
- **THEN** 風險等級為「風險警示」

### Requirement: 訊號預計算與儲存
系統 SHALL 預計算訊號並儲存到資料庫。

#### Scenario: convertible_bond_signals 表
- **WHEN** 系統初始化資料庫
- **THEN** 建立 `convertible_bond_signals` 表：
  - `bond_id` (String, PK)
  - `date` (DateTime) - 計算日期
  - `raw_score` (Integer) - 原始分數
  - `normalized_score` (Integer) - 標準化分數 (0-100)
  - `signal_count` (Integer) - 觸發訊號數
  - `risk_level` (String) - 風險等級
  - `signals_json` (JSONB) - 訊號詳情
  - `underlying_stock_id` (String) - 標的股代碼
  - `premium_rate` (Float) - 最新溢價率
  - `conversion_value` (Float) - 最新轉換價值
  - `updated_at` (DateTime)

#### Scenario: 批次訊號計算
- **WHEN** 每日更新 CB 資料後
- **THEN** 批次計算所有 active CB 的訊號
- **THEN** 使用 `ON CONFLICT (bond_id) DO UPDATE` 策略寫入
- **THEN** 150 檔 CB 訊號計算 SHALL < 5 秒

### Requirement: 標的股關聯查詢
系統 SHALL 支援從標的股查詢其對應的 CB 套利分數。

#### Scenario: 建立股票→CB 映射
- **WHEN** 計算完所有 CB 訊號
- **THEN** 建立 `underlying_stock_id → max(normalized_score)` 映射
- **THEN** 若同一標的股有多檔 CB，取最高分的 CB 分數作為該股票的 `cb_arbitrage_score`
