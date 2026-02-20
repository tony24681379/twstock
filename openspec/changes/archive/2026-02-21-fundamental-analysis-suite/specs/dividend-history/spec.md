## ADDED Requirements

### Requirement: 存儲歷史股利資料
系統 SHALL 新增 `stock_dividend` 資料表，結構為 (stock_id STRING PK, year INTEGER PK, cash_dividend FLOAT, stock_dividend FLOAT, updated_at DATETIME)，存儲每支股票每年的配息記錄。

#### Scenario: 儲存多年股利資料
- **WHEN** Wantgoo `ex-dividend-data` 回傳 5 年股利歷史
- **THEN** 系統將 5 筆資料全部存入 `stock_dividend` 表，每筆對應一個年度

#### Scenario: 更新既有股利資料
- **WHEN** 同一 stock_id + year 的資料已存在，且 API 回傳新數值
- **THEN** 系統 UPSERT 更新該筆記錄

### Requirement: 從 Wantgoo API 擷取完整股利歷史
系統 SHALL 修改 `fetch_info()` 以存儲 `ex-dividend-data` 回傳的所有年度股利，而非僅取 `dividend[0]`。`stock_info` 表的 `cash_dividend` / `stock_dividend` 欄位繼續保存最新年度值（向後相容）。

#### Scenario: API 回傳多年資料
- **WHEN** `ex-dividend-data` 回傳 `[{period: "114", cashDividend: 3.0, ...}, {period: "113", cashDividend: 2.8, ...}]`
- **THEN** `stock_info.cash_dividend` = 3.0（最新），`stock_dividend` 表存入兩筆 (year=114, year=113)

#### Scenario: API 回傳空陣列
- **WHEN** `ex-dividend-data` 回傳 `[]`
- **THEN** `stock_info.cash_dividend` = 0，不寫入 `stock_dividend` 表

### Requirement: 股利連續成長訊號
系統 SHALL 在最近 3 年現金股利連續增加（每年 > 前年）時觸發「股利連續成長」訊號，分數 +10。

#### Scenario: 連續 3 年股利遞增
- **WHEN** 歷史股利為 year=114: 3.0, year=113: 2.5, year=112: 2.0
- **THEN** 觸發「股利連續成長」訊號，score = +10

#### Scenario: 中間一年持平
- **WHEN** 歷史股利為 year=114: 3.0, year=113: 3.0, year=112: 2.0
- **THEN** 不觸發此訊號（113 年未增加）

### Requirement: 股利大幅削減訊號
系統 SHALL 在最新年度現金股利較前一年減少 30% 以上時觸發「股利大幅削減」訊號，分數 -12。

#### Scenario: 股利大幅減少
- **WHEN** 最新年度股利 = 1.5，前一年度 = 3.0（減少 50%）
- **THEN** 觸發「股利大幅削減」訊號，score = -12

#### Scenario: 股利小幅減少
- **WHEN** 最新年度股利 = 2.5，前一年度 = 3.0（減少 16.7%）
- **THEN** 不觸發此訊號

#### Scenario: 歷史股利資料不足
- **WHEN** `stock_dividend` 表中該股票只有 1 年資料
- **THEN** 不觸發股利成長或削減訊號

### Requirement: 前端顯示歷史股利趨勢
系統 SHALL 在 FundamentalInfo API 回應中包含歷史股利資料（最近 5 年），前端估值指標 tab 展示股利趨勢圖。

#### Scenario: 有歷史股利資料
- **WHEN** 使用者查看某股票的基本面詳情
- **THEN** 估值指標 tab 顯示股利趨勢折線圖（X 軸：年度，Y 軸：現金股利）

#### Scenario: 無歷史股利資料
- **WHEN** 該股票在 `stock_dividend` 表無資料
- **THEN** 不顯示股利趨勢圖，僅顯示現有的殖利率卡片
