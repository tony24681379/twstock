# 可轉債前端 Dashboard 規格

## ADDED Requirements

### Requirement: 可轉債總表頁面
前端 SHALL 提供獨立的可轉債總表頁面，路由為 `/convertible`。

#### Scenario: 頁面載入
- **WHEN** 使用者導航到 `/convertible`
- **THEN** 頁面顯示所有活躍可轉債的表格
- **THEN** 預設依套利評分（normalized_score）降冪排序

#### Scenario: 表格欄位
- **WHEN** 頁面顯示 CB 列表
- **THEN** 表格包含以下欄位：
  - CB 代碼（bond_id）
  - CB 名稱（name）
  - 標的股（underlying_stock_id + name）
  - CB 收盤價（close）
  - 轉換價（conversion_price）
  - 轉換價值（conversion_value）
  - 溢價率（premium_rate，%）- 負值標綠色，高值標紅色
  - 套利空間（arbitrage_spread，%）- 正值標綠色
  - 套利評分（normalized_score，0-100）- 漸層色彩
  - 訊號數（signal_count）
  - 風險等級（risk_level）- 色彩標籤
  - 到期日（maturity_date）
  - 成交量（volume）

#### Scenario: 排序功能
- **WHEN** 使用者點擊欄位標題
- **THEN** 切換該欄位的升冪/降冪排序
- **THEN** URL 參數同步更新（`?sortBy=premium_rate&order=asc`）

#### Scenario: 評分色彩
- **WHEN** 顯示套利評分欄位
- **THEN** 80-100 顯示綠色背景（強力套利）
- **THEN** 60-79 顯示淺綠色背景（套利機會）
- **THEN** 40-59 顯示黃色背景（觀望）
- **THEN** 0-39 顯示紅色背景（風險警示）

#### Scenario: 溢價率色彩
- **WHEN** 顯示溢價率欄位
- **THEN** 溢價率 < 0%（折價）顯示深綠色
- **THEN** 溢價率 0-5% 顯示淺綠色
- **THEN** 溢價率 5-15% 顯示灰色
- **THEN** 溢價率 > 15% 顯示紅色

#### Scenario: 點擊進入詳情
- **WHEN** 使用者點擊某筆 CB
- **THEN** 導航到 `/convertible/{bond_id}` 詳情頁

### Requirement: 可轉債詳情頁面
前端 SHALL 提供 CB 詳情頁面，路由為 `/convertible/:bondId`。

#### Scenario: 基本資訊區塊
- **WHEN** 頁面載入
- **THEN** 顯示 CB 基本資訊：代碼、名稱、發行日、到期日、票面利率、發行量、流通餘額

#### Scenario: 套利分析區塊
- **WHEN** 頁面載入
- **THEN** 顯示關鍵套利指標卡片：轉換價、轉換價值、溢價率、套利空間
- **THEN** 顯示套利評分（0-100）和風險等級
- **THEN** 顯示觸發的訊號列表（名稱、分數、描述）

#### Scenario: 歷史趨勢圖
- **WHEN** 頁面載入
- **THEN** 顯示 90 天歷史圖表：
  - 折線圖 1：CB 收盤價 vs 轉換價值（雙軸）
  - 折線圖 2：溢價率趨勢

#### Scenario: 標的股連結
- **WHEN** 頁面顯示標的股資訊
- **THEN** 標的股代碼/名稱可點擊，導航到 `/stocks/{underlying_stock_id}`

### Requirement: 導航整合
前端 SHALL 在頂部導航加入可轉債入口。

#### Scenario: 導航項目
- **WHEN** 使用者瀏覽任何頁面
- **THEN** 頂部導航顯示「可轉債」入口，連結到 `/convertible`
- **THEN** 導航順序：股票 | 可轉債

#### Scenario: 當前頁面高亮
- **WHEN** 使用者在 `/convertible` 或 `/convertible/:bondId` 頁面
- **THEN** 「可轉債」導航項目高亮顯示

### Requirement: 股票總表 CB 欄位
前端 SHALL 在現有股票總表中顯示 CB 套利分數。

#### Scenario: CB 分數欄位顯示
- **WHEN** 股票總表載入
- **THEN** 表格新增「CB 套利」欄位
- **THEN** 有關聯 CB 的股票顯示其 `cb_arbitrage_score`（0-100）
- **THEN** 無關聯 CB 的股票該欄顯示「-」

#### Scenario: CB 分數色彩
- **WHEN** 顯示 CB 套利分數
- **THEN** 使用與可轉債總表相同的色彩規則（80+綠色、60+淺綠、40+黃色、<40 紅色）

#### Scenario: 依 CB 分數排序
- **WHEN** 使用者點擊「CB 套利」欄位標題
- **THEN** 依 `cb_arbitrage_score` 排序
- **THEN** 無 CB 的股票排在最後（null 值置底）

#### Scenario: CB 分數可連結
- **WHEN** 使用者點擊某股票的 CB 套利分數
- **THEN** 導航到該股票最高分 CB 的詳情頁 `/convertible/{bond_id}`

### Requirement: 股票詳情頁 CB 區塊
前端 SHALL 在股票詳情頁顯示關聯可轉債資訊。

#### Scenario: 有關聯 CB
- **WHEN** 該股票有對應的可轉債
- **THEN** 顯示「關聯可轉債」區塊，列出所有 CB 的代碼、名稱、溢價率、套利評分
- **THEN** 每筆 CB 可點擊進入詳情頁

#### Scenario: 無關聯 CB
- **WHEN** 該股票無對應的可轉債
- **THEN** 不顯示「關聯可轉債」區塊（完全隱藏，非顯示空狀態）
