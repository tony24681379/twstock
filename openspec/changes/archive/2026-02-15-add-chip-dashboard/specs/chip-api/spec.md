# 籌碼 API 規格

## ADDED Requirements

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

#### Scenario: 分頁
- **WHEN** 客戶端請求 `GET /api/stocks?limit=100&offset=200`
- **THEN** API 回傳從 offset 200 開始的 100 支股票，附帶分頁中繼資料

#### Scenario: 無效的排序欄位
- **WHEN** 客戶端請求 `GET /api/stocks?sort_by=invalid_field`
- **THEN** API 回傳狀態碼 400，錯誤訊息「Invalid sort field」

#### Scenario: 回應格式
- **WHEN** API 回傳股票列表
- **THEN** 每個股票物件包含：stock_id、name、close_price、signal_strength、signal_count、expected_return、win_rate、risk_level、major_signals、last_updated

### Requirement: 股票詳細端點
API SHALL 提供端點以取得特定股票的詳細資訊。

#### Scenario: 取得股票詳細資訊
- **WHEN** 客戶端請求 `GET /api/stocks/{stock_id}`
- **THEN** API 回傳包含詳細股票資訊與當前訊號的 JSON 物件，狀態碼 200

#### Scenario: 股票不存在
- **WHEN** 客戶端請求不存在的股票 `GET /api/stocks/{stock_id}`
- **THEN** API 回傳狀態碼 404，錯誤訊息「Stock not found」

#### Scenario: 無效的股票代碼格式
- **WHEN** 客戶端請求無效代碼 `GET /api/stocks/ABC`
- **THEN** API 回傳狀態碼 400，錯誤訊息「Invalid stock ID format」

#### Scenario: 回應格式 [CLARIFIED]
- **WHEN** API 回傳股票詳細資訊
- **THEN** 回應包含以下欄位：
  - **basic_info**: 基本資訊（stock_id, name, industry, outstanding_shares）
  - **price_info**: 價格資訊（close, open, high, low, volume, date, change, change_percent）
  - **chip_signals**: 籌碼集中度訊號列表，每個訊號包含（name, triggered, score, description）
  - **technical_signals**: 技術訊號列表（空陣列，詳情頁不計算技術指標以優化效能）
  - **expected_return**: 預期報酬率 (%)，基於歷史回測
  - **win_rate**: 歷史勝率 (%)，基於歷史回測
  - **concentration_summary**: 籌碼集中度摘要（large_holders_pct, super_large_holders_pct, retail_pct, latest_date）
  - **signal_strength**: 訊號強度評分 (0-100)，用於前端顯示，等同於籌碼強度
  - **signals**: 完整訊號列表（chip_signals + technical_signals），向後相容

**注意**：StockDetail **不包含** chip_strength, technical_strength, overall_strength 欄位。
這些欄位僅存在於 StockListItem（列表 API `/api/stocks`），因為：
1. 詳情頁不使用這些欄位（只使用 signal_strength）
2. 避免不必要的技術指標計算（節省 2-3 秒）
3. 遵循資料最小化原則（API 只返回前端實際使用的資料）
4. 參見 design.md Decision 10 了解完整設計理由

### Requirement: 股票歷史端點
API SHALL 提供端點以取得歷史籌碼集中度資料。

#### Scenario: 取得 3 個月歷史
- **WHEN** 客戶端請求 `GET /api/stocks/{stock_id}/history?weeks=12`
- **THEN** API 回傳 12 週籌碼集中度資料的 JSON 陣列，狀態碼 200

#### Scenario: 預設 weeks 參數
- **WHEN** 客戶端請求 `GET /api/stocks/{stock_id}/history` 未帶 weeks 參數
- **THEN** API 預設回傳 12 週歷史

#### Scenario: 股票不存在
- **WHEN** 客戶端請求不存在股票的歷史
- **THEN** API 回傳狀態碼 404，錯誤訊息「Stock not found」

#### Scenario: 歷史資料不足
- **WHEN** 客戶端請求 12 週但僅有 8 週可用
- **THEN** API 回傳可用的 8 週資料，狀態碼 200

#### Scenario: 回應格式
- **WHEN** API 回傳歷史資料
- **THEN** 每個資料點包含：week_label（例如「W1」、「W2」）、date、moreThan400_pct、moreThan1000_pct、lessThan20_pct、moreThan400_change、moreThan1000_change、lessThan20_change

### Requirement: 健康檢查端點
API SHALL 提供健康檢查端點用於監控。

#### Scenario: 健康檢查
- **WHEN** 客戶端請求 `GET /api/health`
- **THEN** API 回傳 JSON 物件，包含 status「healthy」、database connection status、timestamp，狀態碼 200

#### Scenario: 資料庫連線失敗
- **WHEN** 健康檢查時資料庫無法連線
- **THEN** API 回傳狀態碼 503，status「unhealthy」與錯誤詳情

### Requirement: 效能
API SHALL 在可接受的時間限制內回應。

#### Scenario: 列表端點回應時間
- **WHEN** 客戶端請求股票列表
- **THEN** API 在 500 毫秒內回應

#### Scenario: 詳細端點回應時間
- **WHEN** 客戶端請求股票詳細資訊
- **THEN** API 在 1000 毫秒內回應

#### Scenario: 歷史端點回應時間
- **WHEN** 客戶端請求 12 週歷史
- **THEN** API 在 1000 毫秒內回應

### Requirement: CORS 設定
API SHALL 支援來自前端儀表板的跨域請求。

#### Scenario: CORS 預檢請求
- **WHEN** 瀏覽器發送帶 Origin 標頭的 OPTIONS 請求
- **THEN** API 回應適當的 Access-Control-Allow-Origin、Access-Control-Allow-Methods、Access-Control-Allow-Headers

#### Scenario: GET 請求的 CORS
- **WHEN** 客戶端從允許的來源發送 GET 請求
- **THEN** API 在回應中包含 Access-Control-Allow-Origin 標頭

### Requirement: 錯誤處理
API SHALL 提供清楚、一致的錯誤回應。

#### Scenario: 驗證錯誤格式
- **WHEN** 客戶端發送無效的請求參數
- **THEN** API 回傳狀態碼 400，JSON 格式：{"error": "message", "detail": "specific validation error"}

#### Scenario: 未找到錯誤格式
- **WHEN** 請求的資源不存在
- **THEN** API 回傳狀態碼 404，JSON 格式：{"error": "Resource not found", "detail": "Stock ID 9999 not found"}

#### Scenario: 伺服器錯誤格式
- **WHEN** 發生內部伺服器錯誤
- **THEN** API 回傳狀態碼 500，JSON 格式：{"error": "Internal server error", "detail": "error description"}，並記錄完整堆疊追蹤

#### Scenario: 資料庫連線錯誤
- **WHEN** 請求期間資料庫連線失敗
- **THEN** API 回傳狀態碼 503，JSON 格式：{"error": "Service unavailable", "detail": "Database connection failed"}

### Requirement: API 文件
API SHALL 提供互動式文件。

#### Scenario: OpenAPI 文件
- **WHEN** 開發者導航至 `/docs`
- **THEN** API 提供互動式 Swagger UI，記錄所有端點

#### Scenario: ReDoc 文件
- **WHEN** 開發者導航至 `/redoc`
- **THEN** API 提供 ReDoc 替代文件介面

#### Scenario: OpenAPI JSON schema
- **WHEN** 客戶端請求 `GET /openapi.json`
- **THEN** API 回傳 OpenAPI 3.0 JSON schema

### Requirement: 快取標頭
API SHALL 提供適當的快取標頭，因資料每日更新。

#### Scenario: 列表端點的快取控制
- **WHEN** API 回應股票列表請求
- **THEN** 回應包含標頭：Cache-Control: public, max-age=3600

#### Scenario: 詳細端點的快取控制
- **WHEN** API 回應股票詳細資訊請求
- **THEN** 回應包含標頭：Cache-Control: public, max-age=3600

#### Scenario: ETag 支援
- **WHEN** API 回應資料
- **THEN** 回應包含 ETag 標頭，帶內容雜湊值用於條件請求

### Requirement: Token 驗證（可選功能）
API SHALL 支援透過環境變數控制的 token 驗證功能。

#### Scenario: 驗證功能關閉時
- **WHEN** 環境變數 REQUIRE_AUTH 設為 false 或未設定
- **THEN** API 接受所有請求，無需驗證

#### Scenario: 驗證功能開啟且無 token
- **WHEN** 環境變數 REQUIRE_AUTH 設為 true 且請求未包含 Authorization 標頭
- **THEN** API 回傳狀態碼 401，錯誤訊息「Authentication required」

#### Scenario: Token 驗證成功且 email 在白名單
- **WHEN** 請求包含有效 Google OAuth token（Authorization: Bearer xxx）且 email 在白名單內
- **THEN** API 處理請求並正常回應

#### Scenario: Token 驗證成功但 email 不在白名單
- **WHEN** 請求包含有效 token 但 email 不在白名單內
- **THEN** API 回傳狀態碼 403，錯誤訊息「Email not authorized: user@example.com」

#### Scenario: Token 無效或過期
- **WHEN** 請求包含無效或過期的 token
- **THEN** API 回傳狀態碼 401，錯誤訊息「Invalid or expired token」

#### Scenario: Token 格式錯誤
- **WHEN** 請求的 Authorization 標頭格式錯誤（不是 Bearer token）
- **THEN** API 回傳狀態碼 401，錯誤訊息「Invalid authorization header format」

### Requirement: 圖表資料端點
API SHALL 提供端點以取得 K 線資料與技術指標。

#### Scenario: 取得預設 K 線資料
- **WHEN** 客戶端請求 `GET /api/stocks/{stock_id}/chart`
- **THEN** API 回傳 3 個月的 OHLCV 資料與預設技術指標（MA5/MA10/MA20），狀態碼 200

#### Scenario: 指定時間範圍
- **WHEN** 客戶端請求 `GET /api/stocks/{stock_id}/chart?period=1Y`
- **THEN** API 回傳 1 年的 K 線資料

#### Scenario: 指定技術指標
- **WHEN** 客戶端請求 `GET /api/stocks/{stock_id}/chart?indicators=MA20,MACD,RSI`
- **THEN** API 僅計算並回傳請求的技術指標

#### Scenario: 無效的時間範圍
- **WHEN** 客戶端請求無效的 period 參數
- **THEN** API 回傳狀態碼 400，錯誤訊息「Invalid period」

### Requirement: 籌碼資料端點
API SHALL 提供端點以取得三大法人、主力、融資融券資料。

#### Scenario: 取得籌碼資料
- **WHEN** 客戶端請求 `GET /api/stocks/{stock_id}/chips?weeks=12`
- **THEN** API 回傳 12 週的三大法人、主力、融資融券資料，狀態碼 200

#### Scenario: 資料格式
- **WHEN** API 回傳籌碼資料
- **THEN** 回應包含：institutional（三大法人）、major（主力）、margin（融資融券）、concentration（籌碼集中度）

### Requirement: 基本面強度評分系統
API SHALL 提供基本面強度評分，基於現有財務資料計算。

#### 基本面訊號定義

基本面強度評分使用 10 個訊號，基於 StockInfo 和 StockEPS 表的現有資料：

**買進訊號（正分）**：
1. **由虧轉正** (+30) - 最重要：最近一季 EPS > 0 且前一季 EPS < 0
2. **EPS 連續成長** (+20)：最近 4 季 EPS 連續正成長
3. **本益比合理** (+15)：0 < PER < 20
4. **高股利殖利率** (+12)：殖利率 > 4%
5. **EPS 穩定** (+8)：最近 4 季 EPS 標準差 < 平均值 * 0.2
6. **高配息率** (+8)：配息率 > 60%

**賣出訊號（負分）**：
1. **由正轉虧** (-30) - 最重要：最近一季 EPS < 0 且前一季 EPS > 0
2. **EPS 連續衰退** (-20)：最近 4 季 EPS 連續下降
3. **本益比過高** (-15)：PER > 40
4. **低股利** (-8)：殖利率 < 1%

#### 評分標準化

- 原始分數範圍：-73 到 +93（最佳 +93，最差 -73）
- 標準化公式：`normalized = ((raw_score + 73) / 166) * 100`
- 輸出範圍：0-100

#### Scenario: 股票列表包含基本面強度
- **WHEN** 客戶端請求 `GET /api/stocks`
- **THEN** 每個股票物件額外包含：
  - **fundamental_strength** (int)：基本面強度評分 (0-100)
  - **fundamental_signals** (array)：觸發的基本面訊號列表
  - **weights** (object)：計算 overall_strength 使用的權重 `{"chip": 0.5, "technical": 0.3, "fundamental": 0.2}`

#### Scenario: 自訂綜合強度權重
- **WHEN** 客戶端請求 `GET /api/stocks?chip_weight=0.4&tech_weight=0.3&fund_weight=0.3`
- **THEN** API 使用自訂權重計算 overall_strength，並在回應的 weights 欄位反映使用的權重

#### Scenario: 權重驗證失敗
- **WHEN** 客戶端請求的三種權重總和不等於 1.0
- **THEN** API 回傳狀態碼 400，錯誤訊息「權重總和必須為 1.0（當前: {total}）」

#### Scenario: 預設權重
- **WHEN** 客戶端請求未指定權重參數
- **THEN** API 使用預設權重：chip 50%, technical 30%, fundamental 20%

#### Scenario: 依基本面強度排序
- **WHEN** 客戶端請求 `GET /api/stocks?sort_by=fundamental_strength&order=desc`
- **THEN** API 回傳依基本面強度降冪排序的股票

### Requirement: 基本面詳細資訊端點
API SHALL 提供端點以取得股票完整基本面分析資訊。

#### Scenario: 取得基本面詳細資訊
- **WHEN** 客戶端請求 `GET /api/stocks/{stock_id}/fundamental`
- **THEN** API 回傳包含完整基本面指標的 JSON 物件，狀態碼 200

#### Scenario: 股票不存在
- **WHEN** 客戶端請求不存在股票的基本面資訊
- **THEN** API 回傳狀態碼 404，錯誤訊息「Stock not found」

#### Scenario: EPS 資料不足
- **WHEN** 股票 EPS 資料少於 4 季
- **THEN** API 使用可用的季度計算，並在回應中標註資料季數

#### Scenario: 基本面詳細回應格式
- **WHEN** API 回傳基本面詳細資訊
- **THEN** 回應包含以下欄位：
  - **eps_recent_4q** (array)：最近 4 季 EPS 數值
  - **eps_trend** (string)：「上升」、「下降」或「持平」
  - **eps_stability** (float)：EPS 標準差
  - **eps_avg** (float)：平均 EPS
  - **per** (float|null)：本益比
  - **dividend_yield** (float)：股利殖利率 (%)
  - **payout_ratio** (float)：配息率 (%)
  - **cash_dividend** (float)：現金股利
  - **fundamental_signals** (array)：觸發的訊號列表
  - **fundamental_strength** (int)：基本面強度評分 (0-100)

#### Scenario: 訊號計算實作驗證
- **WHEN** 系統計算基本面訊號
- **THEN** 所有 10 個訊號規則已實作於 `FundamentalSignalService` (`api/services/fundamental_signal_service.py`)
- **THEN** 訊號計算邏輯已通過 15 個單元測試
- **THEN** 標準化公式：`((raw_score + 73) / 166) * 100`
- **VERIFICATION** 測試檔案：`api/tests/test_fundamental_signals.py`
- **IMPLEMENTATION** Service 層：`api/services/stock_service.py:850-1057`

### Requirement: 月營收資料端點
API SHALL 提供端點以取得股票月營收歷史資料，透過基本面詳細端點回傳。

#### Scenario: 基本面端點包含月營收
- **WHEN** 客戶端請求 `GET /api/stocks/{stock_id}/fundamental`
- **THEN** 回應包含 `revenue_details` 陣列，每個元素包含 year, month, revenue, mom_change, yoy_change, cumulative_revenue, cumulative_yoy_change
- **THEN** 回應包含 `revenue_recent_12m` 陣列（最近12個月營收值，單位：千元）
- **THEN** 回應包含 `revenue_yoy_avg`（平均年增率，單位：%）
- **THEN** 回應包含 `revenue_trend`（營收趨勢：上升/下降/持平）
- **IMPLEMENTATION** 資料來源：`stock_monthly_revenue` 表 (`twstock/database.py:162-180`)
- **IMPLEMENTATION** 批次載入：`bulk_load_monthly_revenue()` (`twstock/database.py:720-813`)

#### Scenario: 月營收資料缺失
- **WHEN** 股票無月營收資料或資料不足12個月
- **THEN** `revenue_details` 返回可用的月份資料（長度可能 < 12）
- **THEN** `revenue_recent_12m` 長度可能 < 12
- **THEN** 前端顯示「暫無月營收資料」提示 (`dashboard/src/components/FundamentalCard.tsx`)

#### Scenario: 營收趨勢計算
- **WHEN** 計算營收趨勢
- **THEN** 比較最近3個月平均營收與前3個月平均營收
- **THEN** 增長 > 5% 為「上升」
- **THEN** 下降 > 5% 為「下降」
- **THEN** 其他為「持平」
- **IMPLEMENTATION** 計算邏輯：`api/services/stock_service.py:995-1014`

#### Scenario: 月營收資料抓取
- **WHEN** 執行月營收批次更新
- **THEN** 使用 `WantgooFetcher.fetch_monthly_revenue()` 從 Wantgoo API 抓取資料 (`twstock/wantgoo.py:466-542`)
- **THEN** 資料包含 7 個欄位：year, month, revenue, mom_change, yoy_change, cumulative_revenue, cumulative_yoy_change
- **THEN** 使用 `save_monthly_revenue()` 批次儲存到資料庫 (`twstock/database.py:637-718`)
- **THEN** 批次更新腳本：`update_all_revenue.py`（支援並行更新，預設10個工作進程）

#### Scenario: 前端營收顯示
- **WHEN** 前端顯示基本面資訊
- **THEN** FundamentalCard 包含「營收成長」Tab (`dashboard/src/components/FundamentalCard.tsx:213-391`)
- **THEN** 顯示月營收趨勢圖（ComposedChart：柱狀圖顯示營收 + 折線圖顯示年增率）
- **THEN** 顯示統計指標卡片（營收趨勢、平均年增率、最新月營收）
- **THEN** 顯示12個月詳細表格（含 MoM、YoY、累計營收）
