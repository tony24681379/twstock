## ADDED Requirements

### Requirement: PSR 計算
系統 SHALL 計算 PSR（股價營收比）= 市值 / 年營收。市值 = 當前股價 × 流通股數（千股）× 1000。年營收 = 最近 12 個月營收累加（千元）× 1000。結果四捨五入至小數點兩位。

#### Scenario: 正常計算 PSR
- **WHEN** 股價 = 100，流通股數 = 500,000（千股），近 12 月營收累計 = 10,000,000（千元）
- **THEN** PSR = (100 × 500,000 × 1000) / (10,000,000 × 1000) = 5.00

#### Scenario: 營收為零或缺失
- **WHEN** 近 12 月營收資料不存在或總和為 0
- **THEN** PSR = null

#### Scenario: 流通股數缺失
- **WHEN** `outstanding_shares` 為 null 或 0
- **THEN** PSR = null

### Requirement: PSR 偏低訊號
系統 SHALL 在 PSR 介於 0 到 1 之間（含）時觸發「PSR偏低」訊號，分數 +8。

#### Scenario: PSR 在合理低點
- **WHEN** PSR = 0.8
- **THEN** 觸發「PSR偏低」訊號，score = +8

#### Scenario: PSR 為負或 null
- **WHEN** PSR = null（無法計算）
- **THEN** 不觸發此訊號

### Requirement: PSR 過高訊號
系統 SHALL 在 PSR > 10 時觸發「PSR過高」訊號，分數 -8。

#### Scenario: PSR 偏高
- **WHEN** PSR = 15.3
- **THEN** 觸發「PSR過高」訊號，score = -8

#### Scenario: PSR 在正常範圍
- **WHEN** PSR = 3.5
- **THEN** 不觸發 PSR 偏低或過高訊號

### Requirement: 前端展示 PSR
系統 SHALL 在前端「估值指標」tab 新增 PSR 卡片，顯示 PSR 數值及文字說明（偏低/合理/偏高）。

#### Scenario: PSR 有值
- **WHEN** PSR = 3.5
- **THEN** 估值指標 tab 顯示 PSR 卡片，數值 3.5，標示「合理」

#### Scenario: PSR 無法計算
- **WHEN** PSR = null
- **THEN** 估值指標 tab 顯示 PSR 卡片，數值顯示「-」
