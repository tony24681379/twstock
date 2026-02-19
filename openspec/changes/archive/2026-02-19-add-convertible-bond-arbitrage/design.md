## Context

現有 twstock 系統追蹤 ~2700 支台股，涵蓋籌碼、技術、基本面三大維度。系統使用 Wantgoo 作為主要資料來源，PostgreSQL 儲存，FastAPI 提供 API，React 前端呈現。

可轉債（CB）是台股市場中一個獨立的投資工具類別（~150-200 檔），具有明確的數學套利邏輯。目前系統完全不涵蓋 CB 資料。

**現有資料流**：Wantgoo API → WantgooFetcher → DatabaseManager → StockService → FastAPI → React

**關鍵限制**：
- Wantgoo 不確定是否提供 CB API（需驗證）
- 資料庫使用 Neon 免費額度（3GB），CB 資料量極小（<5MB）
- 前端已有成熟的排序/篩選/權重體系

## Goals / Non-Goals

**Goals:**
- 建立完整的 CB 資料爬取管道，從 TWSE/TPEX 官方來源或 FinMind API 取得資料
- 實現 10 個可轉債套利訊號，產出 0-100 標準化評分
- 在股票總表新增 `cb_arbitrage_score` 欄位，將 CB 指標融入現有排序體系
- 建立獨立的可轉債總表頁面 `/convertible`，以 CB 為主體瀏覽
- 將可轉債列為系統最高優先級

**Non-Goals:**
- 不做即時（盤中）CB 報價，僅 T+1 收盤後資料
- 不做 CBAS 選擇權拆解功能（複雜度過高，Phase 3 考慮）
- 不做自動下單或交易執行
- 不修改現有股票訊號計算邏輯
- 不新增付費資料來源依賴

## Decisions

### Decision 1: 資料來源選擇

**選擇**：FinMind API 為主、TWSE/TPEX 官方網頁為備選

**理由**：
- FinMind 提供 4 個 CB 相關 dataset（`TaiwanStockConvertibleBondDaily`, `TaiwanStockConvertibleBondInfo` 等），JSON 格式，無需解析 HTML
- 免費帳號 600 req/hr，CB 僅 ~150 檔，完全夠用
- TWSE/TPEX 官方網頁需解析 HTML table，格式可能變動

**替代方案考慮**：
- Wantgoo：不確定是否有 CB API，需先驗證
- Yahoo Finance：台灣 CB 覆蓋率不明
- TWSE OpenAPI：經查證無 CB 專用端點

**風險緩解**：ConvertibleBondFetcher 使用策略模式，可在不改動上層的情況下切換資料來源

### Decision 2: CB 評分融入股票總表的方式

**選擇**：在 `StockListItem` 新增 `cb_arbitrage_score: Optional[int]` 欄位

**理由**：
- 大多數股票（~2550 支）無對應 CB，該欄位為 `null`
- 有 CB 的標的股（~150 支）顯示 0-100 評分
- 前端可依此欄位排序/篩選，找出有 CB 套利機會的標的股
- 最小化改動：僅新增一個 Optional 欄位，不破壞現有 API contract

**替代方案考慮**：
- 新增獨立權重維度（chip/tech/fund/cb 四維）：過度複雜，且大部分股票無 CB
- 僅在股票詳情頁顯示：用戶無法在總表一眼看到 CB 機會

### Decision 3: 可轉債總表作為獨立路由

**選擇**：`/convertible` 獨立頁面，與股票總表 `/` 平行

**理由**：
- CB 的維度與股票完全不同（溢價率、轉換價值、到期日等），無法套用股票表格
- 獨立頁面可針對 CB 特性設計最佳化的欄位和排序
- 導航新增「可轉債」入口，與「股票」並列

**前端路由規劃**：
```
/                → 股票總表（含 cb_arbitrage_score 欄位）
/stocks/:id      → 股票詳情（含「關聯可轉債」區塊）
/convertible     → 可轉債總表（新頁面）
/convertible/:id → 可轉債詳情（新頁面）
```

### Decision 4: 資料庫表設計

**選擇**：3 張新表，與現有股票表完全獨立

```
convertible_bond        → CB 基本資訊（轉換價、到期日、發行量等）
convertible_bond_daily  → CB 每日交易（價格、成交量、轉換價值、溢價率）
convertible_bond_signals → CB 套利訊號（預計算，類似 stock_technical_signals）
```

**理由**：
- CB 和股票是不同的金融工具，資料結構差異大
- 獨立表避免 JOIN 開銷，CB 查詢不影響股票查詢效能
- 與現有 DatabaseManager 的批次操作模式一致

**關鍵索引**：
- `convertible_bond`: `bond_id` (PK), `underlying_stock_id` (FK 概念，非強制)
- `convertible_bond_daily`: `(bond_id, date)` 複合唯一
- `convertible_bond_signals`: `bond_id` (PK), `normalized_score` (排序用)

### Decision 5: 套利訊號設計

**選擇**：10 個訊號，分三類，原始分數 -40 ~ +100，標準化 0-100

**套利訊號（6 個，核心）**：

| # | 訊號名稱 | 分數 | 條件 |
|---|---------|------|------|
| 1 | 折價套利 | +20 | 轉換價值 > CB 市價 × 1.006（含交易成本） |
| 2 | 低溢價率 | +15 | 溢價率 < 5% 且標的股技術訊號 > 50 |
| 3 | 催換在即 | +15 | 標的股價 > 轉換價 × 130%（公司可強制贖回） |
| 4 | 到期賣回保護 | +12 | 距賣回日 < 6 個月且賣回價 > 市價 |
| 5 | 深度價內 | +10 | 標的股價 > 轉換價 × 150% |
| 6 | 低於面額 | +8 | CB 市價 < 100（面額） |

**標的股連動訊號（2 個）**：

| # | 訊號名稱 | 分數 | 條件 |
|---|---------|------|------|
| 7 | 標的股強勢 | +12 | 標的股 overall_strength > 70 |
| 8 | 標的股弱勢 | -15 | 標的股 overall_strength < 30 |

**風險訊號（2 個）**：

| # | 訊號名稱 | 分數 | 條件 |
|---|---------|------|------|
| 9 | 高溢價風險 | -15 | 溢價率 > 20% |
| 10 | 到期逼近 | -10 | 距到期日 < 3 個月且溢價率 > 10% |

**標準化公式**：`normalized = ((raw_score + 40) / 140) * 100`

### Decision 6: 與現有系統的連接點

**股票 → CB 的連接**：透過 `convertible_bond.underlying_stock_id` 關聯
- 股票總表：查詢時 LEFT JOIN `convertible_bond_signals` 取得 `cb_arbitrage_score`
- 股票詳情頁：查詢該 `stock_id` 對應的所有 CB

**CB → 股票的連接**：CB 訊號計算時讀取標的股的 `overall_strength`
- 使用現有 `stock_technical_signals` 表取得標的股評分
- 不需要重新計算股票訊號

### Decision 7: 爬蟲架構

**選擇**：新建 `ConvertibleBondFetcher` 類別，獨立於 `WantgooFetcher`

**理由**：
- 資料來源不同（FinMind vs Wantgoo），header/auth 機制不同
- CB 數量少（~150 檔），不需要 Playwright header 機制
- 使用相同的 httpx async client 和 semaphore 限流模式

**批次更新流程**（在 `All` 類別新增方法）：
1. `get_convertible_bond_list()` → 取得所有 CB 清單
2. `bulk_check_cb_needs_update()` → 檢查 DB 日期 vs API 最新日期
3. 並行抓取需更新的 CB（MAX_WORKERS=10）
4. 計算套利訊號 → 寫入 `convertible_bond_signals`
5. 回寫 `cb_arbitrage_score` 到股票端（供列表查詢）

## Risks / Trade-offs

| 風險 | 影響 | 緩解 |
|------|------|------|
| FinMind API 變更或停機 | 無法更新 CB 資料 | 備選 TWSE/TPEX HTML 解析；ConvertibleBondFetcher 策略模式支援切換 |
| FinMind 免費額度不足 | 部分 CB 更新失敗 | 150 檔 × 2 API call = 300 req，遠低於 600/hr 限制 |
| CB 清單頻繁變動（新發行/到期下市） | 遺漏新 CB 或查詢已下市 CB | 每日重新同步 CB 清單；標記 `is_active` 狀態 |
| 股票總表新增欄位影響效能 | 列表查詢變慢 | LEFT JOIN 僅 ~150 筆 CB signals，索引優化後影響 <10ms |
| 轉換價格異動（公司調整轉換價） | 套利計算錯誤 | 每日更新 CB 基本資訊，不做快取假設 |
