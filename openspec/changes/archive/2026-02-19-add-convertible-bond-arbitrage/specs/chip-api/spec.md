# 籌碼 API 規格 - 可轉債擴展

## MODIFIED Requirements

### Requirement: 股票列表端點
API SHALL 提供端點以取得所有具籌碼集中度訊號的股票。

#### Scenario: 以預設排序取得所有股票
- **WHEN** 客戶端請求 `GET /api/stocks`
- **THEN** API 回傳以 stock_id 升冪排序的股票 JSON 陣列，狀態碼 200

#### Scenario: 依訊號強度排序
- **WHEN** 客戶端請求 `GET /api/stocks?sort_by=score&order=desc`
- **THEN** API 回傳依訊號強度降冪排序的股票

#### Scenario: 依預期報酬排序
- **WHEN** 客戶端請求 `GET /api/stocks?sort_by=expected_return&order=desc`
- **THEN** API 回傳依預期報酬百分比降冪排序的股票

#### Scenario: 依勝率排序
- **WHEN** 客戶端請求 `GET /api/stocks?sort_by=win_rate&order=desc`
- **THEN** API 回傳依歷史勝率降冪排序的股票

#### Scenario: 依 CB 套利分數排序
- **WHEN** 客戶端請求 `GET /api/stocks?sort_by=cb_arbitrage_score&order=desc`
- **THEN** API 回傳依可轉債套利分數降冪排序的股票
- **THEN** 無 CB 的股票排在最後（null 值置底）

#### Scenario: 分頁
- **WHEN** 客戶端請求 `GET /api/stocks?limit=100&offset=200`
- **THEN** API 回傳從 offset 200 開始的 100 支股票，附帶分頁中繼資料

#### Scenario: 無效的排序欄位
- **WHEN** 客戶端請求 `GET /api/stocks?sort_by=invalid_field`
- **THEN** API 回傳狀態碼 400，錯誤訊息「Invalid sort field」

#### Scenario: 回應格式
- **WHEN** API 回傳股票列表
- **THEN** 每個股票物件包含：stock_id、name、close_price、signal_strength、signal_count、expected_return、win_rate、risk_level、major_signals、last_updated、**cb_arbitrage_score**（Optional[int]，有 CB 的標的股為 0-100，無 CB 為 null）

## ADDED Requirements

### Requirement: 股票關聯可轉債查詢
API SHALL 支援從股票查詢其關聯的可轉債。

#### Scenario: 取得股票關聯 CB
- **WHEN** 客戶端請求 `GET /api/stocks/{stock_id}/convertible`
- **THEN** API 回傳該股票作為標的的所有 CB 列表（含 bond_id, name, premium_rate, normalized_score, risk_level），狀態碼 200

#### Scenario: 股票無關聯 CB
- **WHEN** 股票無對應的可轉債
- **THEN** API 回傳空陣列 `[]`，狀態碼 200
