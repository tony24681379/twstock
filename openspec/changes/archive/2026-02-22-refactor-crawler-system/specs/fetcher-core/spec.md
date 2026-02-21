## ADDED Requirements

### Requirement: Fetcher 方法只回傳資料
WantgooFetcher 的所有 fetch 方法（`fetch_info`, `fetch_daily`, `fetch_concentration_data`, `fetch_monthly_revenue`）SHALL 只回傳解析後的資料（`dict` 或 `pd.DataFrame`），不得直接呼叫 DatabaseManager 的任何方法。

#### Scenario: fetch_info 回傳純資料
- **WHEN** 呼叫 `fetcher.fetch_info(sid)`
- **THEN** 回傳包含 stock info 的 `pd.Series`，不觸發任何資料庫寫入

#### Scenario: fetch_daily 回傳純資料
- **WHEN** 呼叫 `fetcher.fetch_daily(sid, num)`
- **THEN** 回傳包含每日交易資料的 `pd.DataFrame`，不觸發任何資料庫寫入

#### Scenario: fetch_concentration_data 回傳純資料
- **WHEN** 呼叫 `fetcher.fetch_concentration_data(sid, weeks=10)`
- **THEN** 回傳包含籌碼集中度的 `pd.DataFrame`，不觸發任何資料庫寫入

#### Scenario: fetch_monthly_revenue 回傳純資料
- **WHEN** 呼叫 `fetcher.fetch_monthly_revenue(sid, months=12)`
- **THEN** 回傳包含月營收的 `pd.DataFrame`，不觸發任何資料庫寫入

### Requirement: Fetcher 不持有 DatabaseManager 參照
WantgooFetcher 的建構式 SHALL NOT 接受 `db_manager` 參數。Fetcher 實例不得持有對 DatabaseManager 的任何參照。

#### Scenario: 建構 Fetcher 無需 DB
- **WHEN** 建構 `WantgooFetcher(header_manager=hm)`
- **THEN** 實例不包含 `db_manager` 屬性

### Requirement: Fetcher 接受 HeaderManager 注入
WantgooFetcher 的建構式 SHALL 接受 `header_manager: HeaderManager` 參數，用於取得 HTTP 請求所需的 headers。

#### Scenario: 使用注入的 HeaderManager
- **WHEN** 呼叫任何 fetch 方法
- **THEN** 透過 `header_manager.get_headers()` 取得 headers 發送 HTTP 請求

### Requirement: fetch_daily 移除 total_stock 參數
`fetch_daily` SHALL NOT 接受 `total_stock` 參數。持股率計算需要的 `outstanding_shares` 應由調用層提供或在 `purify` 時傳入。

#### Scenario: fetch_daily 簽名簡化
- **WHEN** 呼叫 `fetcher.fetch_daily(sid, num)`
- **THEN** 方法簽名只有 `sid` 和 `num` 兩個必要參數

### Requirement: 指數退避重試
`fetch_url` 的重試等待時間 SHALL 使用指數退避策略：`delay = min(base_delay * 2^retry_i + jitter, max_delay)`，其中 `base_delay=0.5`、`max_delay=10`、`jitter` 為 0~0.5 秒的隨機值。

#### Scenario: 第一次重試等待時間
- **WHEN** 第一次請求失敗需要重試
- **THEN** 等待約 0.5~1.0 秒後重試

#### Scenario: 第四次重試等待時間
- **WHEN** 第四次請求失敗需要重試
- **THEN** 等待約 4.0~4.5 秒後重試

#### Scenario: 最大等待時間上限
- **WHEN** 計算出的退避時間超過 10 秒
- **THEN** 等待時間被截斷為 10 秒

### Requirement: 資料抓取失敗回傳空結果
當 API 請求在所有重試後仍然失敗時，fetch 方法 SHALL 回傳空的 DataFrame（而非拋出 Exception），僅在完全無法恢復的錯誤時才拋出。

#### Scenario: 股票不存在或已下市
- **WHEN** API 對某股票代碼連續回傳錯誤（重試耗盡）
- **THEN** 回傳空的 `pd.DataFrame()`，不拋出異常
