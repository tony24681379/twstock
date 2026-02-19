# 可轉債 REST API 規格

## ADDED Requirements

### Requirement: 可轉債列表端點
API SHALL 提供端點以取得所有可轉債及其套利訊號。

#### Scenario: 以預設排序取得所有可轉債
- **WHEN** 客戶端請求 `GET /api/convertible`
- **THEN** API 回傳以 normalized_score 降冪排序的 CB JSON 陣列，狀態碼 200

#### Scenario: 依溢價率排序
- **WHEN** 客戶端請求 `GET /api/convertible?sort_by=premium_rate&order=asc`
- **THEN** API 回傳依溢價率升冪排序的 CB（低溢價優先）

#### Scenario: 依套利空間排序
- **WHEN** 客戶端請求 `GET /api/convertible?sort_by=arbitrage_spread&order=desc`
- **THEN** API 回傳依套利空間降冪排序的 CB

#### Scenario: 分頁
- **WHEN** 客戶端請求 `GET /api/convertible?limit=50&offset=0`
- **THEN** API 回傳從 offset 0 開始的 50 筆 CB，附帶分頁中繼資料

#### Scenario: 篩選活躍 CB
- **WHEN** 客戶端請求 `GET /api/convertible?active_only=true`
- **THEN** API 僅回傳 `is_active = true` 的 CB

#### Scenario: 無效的排序欄位
- **WHEN** 客戶端請求 `GET /api/convertible?sort_by=invalid_field`
- **THEN** API 回傳狀態碼 400，錯誤訊息「Invalid sort field」

#### Scenario: 回應格式
- **WHEN** API 回傳 CB 列表
- **THEN** 每個 CB 物件包含：
  - `bond_id` (str) - 可轉債代碼
  - `name` (str) - 可轉債名稱
  - `underlying_stock_id` (str) - 標的股代碼
  - `underlying_stock_name` (str) - 標的股名稱
  - `close` (float) - CB 收盤價
  - `conversion_price` (float) - 轉換價
  - `conversion_value` (float) - 轉換價值
  - `premium_rate` (float) - 溢價率 (%)
  - `arbitrage_spread` (float) - 套利空間 (%)
  - `normalized_score` (int) - 套利評分 (0-100)
  - `signal_count` (int) - 觸發訊號數
  - `risk_level` (str) - 風險等級
  - `maturity_date` (datetime) - 到期日
  - `volume` (float) - 成交量
  - `last_updated` (datetime)

### Requirement: 可轉債詳情端點
API SHALL 提供端點以取得特定 CB 的詳細資訊。

#### Scenario: 取得 CB 詳情
- **WHEN** 客戶端請求 `GET /api/convertible/{bond_id}`
- **THEN** API 回傳包含完整 CB 資訊的 JSON 物件，狀態碼 200

#### Scenario: CB 不存在
- **WHEN** 客戶端請求不存在的 CB
- **THEN** API 回傳狀態碼 404，錯誤訊息「Bond not found」

#### Scenario: 詳情回應格式
- **WHEN** API 回傳 CB 詳情
- **THEN** 回應包含以下區塊：
  - **基本資訊**：bond_id, name, underlying_stock_id, underlying_stock_name, issue_date, maturity_date, put_date, put_price, coupon_rate, issued_amount, outstanding_amount
  - **交易資訊**：close, volume, conversion_price, conversion_value, premium_rate, arbitrage_spread
  - **訊號資訊**：normalized_score, raw_score, signal_count, risk_level, signals（完整訊號列表，每個含 name, triggered, score, description）
  - **標的股資訊**：underlying_close, underlying_overall_strength

### Requirement: CB 歷史資料端點
API SHALL 提供端點以取得 CB 歷史交易與溢價率趨勢。

#### Scenario: 取得歷史資料
- **WHEN** 客戶端請求 `GET /api/convertible/{bond_id}/history?days=90`
- **THEN** API 回傳 90 天的每日交易資料，狀態碼 200

#### Scenario: 預設天數
- **WHEN** 客戶端請求未帶 days 參數
- **THEN** API 預設回傳 90 天歷史

#### Scenario: 歷史回應格式
- **WHEN** API 回傳歷史資料
- **THEN** 每筆資料包含：date, close, volume, conversion_value, premium_rate, underlying_close

### Requirement: 股票關聯 CB 端點
API SHALL 提供端點以取得特定股票的關聯可轉債。

#### Scenario: 取得股票關聯 CB
- **WHEN** 客戶端請求 `GET /api/stocks/{stock_id}/convertible`
- **THEN** API 回傳該股票作為標的的所有 CB 列表，狀態碼 200

#### Scenario: 股票無關聯 CB
- **WHEN** 股票無對應的可轉債
- **THEN** API 回傳空陣列 `[]`，狀態碼 200

### Requirement: API 效能
API SHALL 在可接受的時間限制內回應。

#### Scenario: 列表端點回應時間
- **WHEN** 客戶端請求 CB 列表
- **THEN** API 在 500 毫秒內回應

#### Scenario: 詳情端點回應時間
- **WHEN** 客戶端請求 CB 詳情
- **THEN** API 在 500 毫秒內回應

#### Scenario: 快取標頭
- **WHEN** API 回應 CB 請求
- **THEN** 回應包含 `Cache-Control: public, max-age=3600`
