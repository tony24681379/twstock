## ADDED Requirements

### Requirement: HeaderManager 封裝 Header 生命週期
HeaderManager SHALL 管理 HTTP headers 的初始化、快取、刷新完整生命週期。外部使用者只透過 `get_headers()` 取得有效的 headers。

#### Scenario: 首次取得 headers 時自動初始化
- **WHEN** 首次呼叫 `header_manager.get_headers()`
- **THEN** 自動執行初始化流程（Playwright → Fallback），回傳完整 headers

#### Scenario: 後續取得 headers 使用快取
- **WHEN** 已初始化後再次呼叫 `get_headers()`
- **THEN** 直接回傳快取的 headers，不重新初始化

### Requirement: Playwright 優先、HTTP Fallback
初始化流程 SHALL 先嘗試 Playwright 獲取完整 headers（含 `x-client-signature`）。若 Playwright 失敗或不可用，SHALL 降級為 httpx 基本 cookies。

#### Scenario: Playwright 成功
- **WHEN** Playwright 可用且正常運行
- **THEN** headers 包含 `x-client-signature` 和 `cookie`

#### Scenario: Playwright 失敗降級
- **WHEN** Playwright 不可用或初始化拋出異常
- **THEN** 使用 httpx 訪問 Wantgoo 取得基本 cookies，headers 不含 `x-client-signature`

### Requirement: 401/403 時刷新 headers
當呼叫 `refresh()` 時，SHALL 先嘗試簡單的 cookie 刷新（保留 `x-client-signature`）。若簡單刷新失敗，SHALL 使用 Playwright 完整重新獲取。

#### Scenario: Cookie 過期刷新
- **WHEN** 呼叫 `refresh()` 且 `x-client-signature` 仍有效
- **THEN** 僅更新 cookie，保留 `x-client-signature`，回傳 True

#### Scenario: 完整 headers 失效
- **WHEN** 簡單 cookie 刷新失敗
- **THEN** 使用 Playwright 完整重新獲取所有 headers

#### Scenario: 刷新全部失敗
- **WHEN** 簡單刷新和 Playwright 都失敗
- **THEN** 回傳 False，headers 維持現狀

### Requirement: 刷新防抖機制
5 秒內的重複 `refresh()` 呼叫 SHALL 被跳過（回傳 True），避免多個並發請求同時觸發刷新。

#### Scenario: 短時間內重複刷新
- **WHEN** 5 秒內有第二次 `refresh()` 呼叫
- **THEN** 直接回傳 True，不執行實際刷新

#### Scenario: 超過防抖間隔後刷新
- **WHEN** 距離上次刷新超過 5 秒後呼叫 `refresh()`
- **THEN** 執行正常刷新流程

### Requirement: 並發安全
所有 HeaderManager 的初始化和刷新操作 SHALL 使用 asyncio.Lock 保護，確保同一時間只有一個協程在執行初始化或刷新。

#### Scenario: 多個協程同時初始化
- **WHEN** 10 個協程同時呼叫 `get_headers()`
- **THEN** 只有第一個執行初始化，其餘等待鎖釋放後取得快取結果

### Requirement: API 最新日期探測
HeaderManager SHALL 在初始化成功後，透過 API 測試請求探測最新資料日期，並透過 `api_latest_date` 屬性公開。

#### Scenario: 初始化時探測日期
- **WHEN** 初始化 headers 成功
- **THEN** 呼叫 2330 的 candlestick API 取得最新 tradeDate，設定 `api_latest_date`

#### Scenario: 探測失敗不影響初始化
- **WHEN** 日期探測 API 請求失敗
- **THEN** `api_latest_date` 為 None，但 headers 初始化仍視為成功
