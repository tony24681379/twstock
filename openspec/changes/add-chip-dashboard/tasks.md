# 實作任務清單

## 1. 後端 API 設置

### 1.1 專案結構
- [x] 1.1.1 在專案根目錄建立 `api/` 目錄
- [x] 1.1.2 初始化 FastAPI 專案，包含 `main.py`、`__init__.py`
- [x] 1.1.3 建立 `api/models/` 目錄用於 Pydantic models
- [x] 1.1.4 建立 `api/routers/` 目錄用於 API routes
- [x] 1.1.5 建立 `api/services/` 目錄用於商業邏輯
- [x] 1.1.6 建立 `api/config.py` 用於設定管理

### 1.2 資料庫整合
- [x] 1.2.1 從 `twstock/database.py` 匯入現有 `DatabaseManager`
- [x] 1.2.2 在 `api/main.py` 建立資料庫連線池初始化
- [x] 1.2.3 實作連線生命週期（startup/shutdown events）
- [x] 1.2.4 以健康檢查端點測試資料庫連線

### 1.3 Pydantic Models
- [x] 1.3.1 建立 `StockListItem` model（stock_id、name、close_price、signal_strength 等）
- [x] 1.3.2 建立 `StockDetail` model（basic_info、price_info、signals、concentration_summary）
- [x] 1.3.3 建立 `StockHistory` model（week_label、date、concentration percentages、changes）
- [x] 1.3.4 建立 `PaginationMetadata` model（total、page、page_size、has_next）
- [x] 1.3.5 建立 `ErrorResponse` model 用於一致的錯誤格式

### 1.4 API 端點 - 股票列表
- [x] 1.4.1 實作 `GET /api/stocks` 與預設排序
- [x] 1.4.2 新增 query parameters：sort_by、order、limit、offset
- [x] 1.4.3 驗證 sort_by 參數（允許：score、expected_return、win_rate、stock_id）
- [x] 1.4.4 實作分頁邏輯與中繼資料
- [x] 1.4.5 新增動態 ORDER BY 子句的 SQL 查詢
- [x] 1.4.6 測試各種排序/分頁組合

### 1.5 API 端點 - 股票詳細
- [x] 1.5.1 實作 `GET /api/stocks/{stock_id}` 端點
- [x] 1.5.2 驗證 stock_id 參數格式（4-6 位數字）
- [x] 1.5.3 查詢 stock_info、stock_daily、concentration_data 表格
- [x] 1.5.4 計算並回傳所有 10 個籌碼集中度訊號
- [x] 1.5.5 處理股票不存在情況（404）
- [x] 1.5.6 用資料庫中現有股票代碼測試

### 1.6 API 端點 - 股票歷史
- [x] 1.6.1 實作 `GET /api/stocks/{stock_id}/history` 端點
- [x] 1.6.2 新增 query parameter：weeks（預設 12，最大 52）
- [x] 1.6.3 查詢 concentration_data，依 date DESC 排序並限制筆數
- [x] 1.6.4 計算每個資料點的週變化
- [x] 1.6.5 以週標籤格式化回應（W1、W2、...）
- [x] 1.6.6 測試具有不同歷史資料量的股票

### 1.7 API 端點 - 健康檢查
- [x] 1.7.1 實作 `GET /api/health` 端點
- [x] 1.7.2 檢查資料庫連線狀態
- [x] 1.7.3 回傳 status、database_status、timestamp
- [x] 1.7.4 資料庫無法連線時回傳 503
- [x] 1.7.5 測試有無資料庫的健康檢查

### 1.8 CORS 設定
- [x] 1.8.1 安裝 fastapi-cors middleware
- [x] 1.8.2 設定允許的來源（localhost:3000 用於開發，正式環境 URL）
- [x] 1.8.3 設定允許的方法（GET、OPTIONS）
- [x] 1.8.4 設定允許的標頭（Content-Type、Authorization）
- [x] 1.8.5 測試來自前端來源的 CORS

### 1.9 錯誤處理
- [x] 1.9.1 實作驗證錯誤的全域例外處理器
- [x] 1.9.2 實作 404 Not Found 處理器
- [x] 1.9.3 實作 500 Internal Server Error 處理器
- [x] 1.9.4 實作資料庫連線錯誤處理器（503）
- [x] 1.9.5 確保所有錯誤類型使用一致的 JSON 錯誤格式
- [x] 1.9.6 新增所有錯誤的日誌記錄與堆疊追蹤

### 1.10 效能最佳化
- [x] 1.10.1 新增資料庫索引：stock_id、date、signal_strength（如不存在）
- [x] 1.10.2 實作回應快取標頭（Cache-Control: public, max-age=3600）
- [x] 1.10.4 最佳化 SQL 查詢以盡可能使用批次載入

**註記**：ETag 支援（原 1.10.3）、負載測試（原 1.10.5）已移至增強功能（6.1.1、6.1.2）

### 1.11 API 文件
- [x] 1.11.1 在 `/docs` 設定 Swagger UI
- [x] 1.11.2 在 `/redoc` 設定 ReDoc
- [x] 1.11.3 新增所有端點的描述與範例
- [x] 1.11.4 以範例記錄請求/回應模型
- [x] 1.11.5 新增 API 使用範例到文件

### 1.12 測試
- [x] 1.12.1 撰寫 Pydantic models 的單元測試
- [ ] 1.12.2 撰寫資料庫服務層的單元測試
- [x] 1.12.3 撰寫所有 API 端點的整合測試
- [x] 1.12.4 撰寫錯誤情境的測試（無效輸入、未找到等）
- [x] 1.12.5 執行測試並達成基本覆蓋（23 個測試通過）

### 1.13 部署準備
- [x] 1.13.1 建立 `requirements.txt` 或更新 `pyproject.toml` 包含 FastAPI 相依套件
- [x] 1.13.2 建立 `.env.example` 檔案，包含必要的環境變數
- [x] 1.13.3 建立後端 API 的 `Dockerfile`
- [x] 1.13.4 新增後端服務到 `docker-compose.yml`
- [x] 1.13.5 在 README 記錄 API 部署

### 1.14 Token 驗證（可選功能）
- [x] 1.14.1 安裝 python-jose：`poetry add python-jose[cryptography]`
- [x] 1.14.2 確認 httpx 已安裝（專案已有，用於取得 Google 公鑰）
- [x] 1.14.3 建立 `api/auth.py` 實作 token 驗證函式
- [x] 1.14.4 實作 `verify_google_token()` 函式：從 Google 取得公鑰並驗證 JWT
- [x] 1.14.5 實作 email 白名單檢查邏輯
- [x] 1.14.6 建立 FastAPI dependency `get_current_user` 用於保護端點
- [x] 1.14.7 在 API endpoints 加上 `Depends(get_current_user)`（根據 REQUIRE_AUTH 決定）
- [x] 1.14.8 實作 REQUIRE_AUTH 環境變數開關邏輯
- [x] 1.14.9 更新 `.env.example` 加入 REQUIRE_AUTH、ALLOWED_EMAILS、GOOGLE_CLIENT_ID
- [ ] 1.14.10 測試 token 驗證（有效/無效/過期）
- [ ] 1.14.11 測試白名單邏輯（在名單/不在名單）
- [ ] 1.14.12 測試 REQUIRE_AUTH 開關（true/false）
- [ ] 1.14.13 測試錯誤回應格式（401、403）

### 1.15 技術指標計算服務
- [x] 1.15.1 確認 ta-lib 已安裝（專案已有）
- [x] 1.15.2 建立 `api/services/indicators.py` 技術指標計算服務
- [x] 1.15.3 實作 calculate_ma() 函式（使用現有 Analytics.moving_average）
- [x] 1.15.4 實作 calculate_macd() 使用 talib.MACD
- [x] 1.15.5 實作 calculate_kd() 使用 talib.STOCH
- [x] 1.15.6 實作 calculate_rsi() 使用 talib.RSI
- [x] 1.15.7 實作 calculate_bollinger_bands() 使用 talib.BBANDS
- [x] 1.15.8 實作 calculate_pivot_points()（使用現有 Analytics 邏輯）
- [x] 1.15.9 測試所有指標計算函式

**註記**：技術指標計算已在前端實作（`dashboard/src/lib/indicators.ts`），後端 `api/services/indicator_service.py` 雖已實作但未使用，保留備用。

### 1.16 圖表資料端點
- [x] 1.16.1 實作 `GET /api/stocks/{stock_id}/chart` 端點
- [x] 1.16.2 新增 query parameters：period（1M/3M/6M/1Y），indicators（逗號分隔）
- [x] 1.16.3 從 StockDaily 查詢 OHLCV 資料（根據 period）
- [x] 1.16.4 呼叫指標計算服務計算請求的指標（註：因前端自行計算，後端不使用）
- [x] 1.16.5 格式化回應 JSON（ohlcv, volume, indicators）
- [x] 1.16.6 測試各種 period 與 indicators 組合

### 1.17 籌碼資料端點
- [x] 1.17.1 實作 `GET /api/stocks/{stock_id}/chips` 端點
- [x] 1.17.2 查詢 InstitutionalInvestors、MajorInvestors、MarginTrading 表格
- [x] 1.17.3 格式化回應（institutional, major, margin, concentration）
- [x] 1.17.4 測試資料正確性

### 1.18 基本面強度評分系統
- [x] 1.18.1 實作 `FundamentalSignalService` 基本面訊號計算服務 (`api/services/fundamental_signal_service.py`)
- [x] 1.18.2 實作 10 個基本面訊號（由虧轉正、由正轉虧、EPS連續成長、EPS連續衰退、本益比合理、本益比過高、高股利殖利率、EPS穩定、高配息率、低股利）
- [x] 1.18.3 實作訊號分數標準化（-73 to +93 → 0 to 100）
- [x] 1.18.4 撰寫基本面訊號單元測試（15個測試案例，`api/tests/test_fundamental_signals.py`）
- [x] 1.18.5 在 `StockListItem` 加入 `fundamental_strength` 和 `fundamental_signals` 欄位
- [x] 1.18.6 實作權重自訂參數（chip_weight, tech_weight, fund_weight）
- [x] 1.18.7 實作綜合強度計算 (`overall_strength`)
- [x] 1.18.8 支援依 `fundamental_strength` 排序

### 1.19 基本面詳細資訊端點
- [x] 1.19.1 實作 `GET /api/stocks/{stock_id}/fundamental` 端點 (`api/routers/stocks.py:206-233`)
- [x] 1.19.2 實作 `StockService.get_fundamental_info()` Service 層方法 (`api/services/stock_service.py:850-1057`)
- [x] 1.19.3 查詢 EPS 資料（最近4季，使用 `bulk_load_eps()`）
- [x] 1.19.4 計算 EPS 相關指標（平均值、趨勢、穩定性）
- [x] 1.19.5 計算股利指標（殖利率、配息率）
- [x] 1.19.6 查詢持股結構資料（董監、外資、投信、自營商）
- [x] 1.19.7 查詢股本資訊（資本額、股本、股票股利）
- [x] 1.19.8 建立 `FundamentalInfo` Pydantic model (`api/models/stock.py:217-254`)
- [x] 1.19.9 處理資料缺失情況（EPS不足4季、無持股結構等）
- [x] 1.19.10 測試端點回應格式

### 1.20 月營收資料功能
- [x] 1.20.1 建立 `StockMonthlyRevenue` 資料庫表 (`twstock/database.py:162-180`)
- [x] 1.20.2 實作 `WantgooFetcher.fetch_monthly_revenue()` 資料抓取方法 (`twstock/wantgoo.py:466-542`)
- [x] 1.20.3 實作 `save_monthly_revenue()` 批次儲存方法 (`twstock/database.py:637-718`)
- [x] 1.20.4 實作 `bulk_load_monthly_revenue()` 批次載入方法 (`twstock/database.py:720-813`，使用 Window Function 優化）
- [x] 1.20.5 建立批次更新腳本 `update_all_revenue.py`（支援並行更新，預設10個工作進程）
- [x] 1.20.6 建立測試腳本 `test_monthly_revenue_flow.py`
- [x] 1.20.7 在 `FundamentalInfo` 加入月營收欄位（`revenue_details`, `revenue_recent_12m`, `revenue_yoy_avg`, `revenue_trend`）
- [x] 1.20.8 實作營收趨勢計算邏輯（比較最近3個月 vs 前3個月）
- [x] 1.20.9 建立 `MonthlyRevenueDetail` Pydantic model (`api/models/stock.py:202-214`)
- [x] 1.20.10 測試月營收資料抓取和載入

### 2.21 Lightweight Charts 整合
- [x] 2.21.1 安裝 Lightweight Charts：`npm install lightweight-charts`
- [x] 2.21.2 建立 `src/components/CandlestickChart.tsx`
- [x] 2.21.3 實作 chart 初始化與配置（主題、網格、時間軸）

## 2. 前端儀表板設置

### 2.1 專案結構
- [x] 2.1.1 在專案根目錄建立 `dashboard/` 目錄
- [x] 2.1.2 使用 Vite 初始化 React + TypeScript 專案：`npm create vite@latest dashboard -- --template react-ts`
- [x] 2.1.3 設定 TypeScript 嚴格模式
- [x] 2.1.4 安裝相依套件：React Router、Tailwind CSS、Recharts
- [x] 2.1.5 建立專案目錄結構：src/pages/、src/components/、src/lib/、src/types/

### 2.2 React Router 設定
- [x] 2.2.1 安裝 React Router v6：`npm install react-router-dom`
- [x] 2.2.2 在 src/main.tsx 設定 BrowserRouter
- [x] 2.2.3 建立路由配置：`/`（列表頁）、`/stocks/:stockId`（詳細頁）
- [x] 2.2.4 測試路由導航

### 2.3 Tailwind CSS 設定
- [x] 2.3.1 安裝與設定 Tailwind CSS
- [x] 2.3.2 在 tailwind.config.js 設定自訂色彩配置
- [x] 2.3.3 新增主要色彩與深色/淺色模式變數
- [x] 2.3.4 設定自訂字體：Poppins（標題）、Open Sans（內文）
- [x] 2.3.5 新增 Google Fonts 引入到 index.html
- [x] 2.3.6 設定深色模式策略（class-based）
- [x] 2.3.7 測試 Tailwind classes

### 2.4 TypeScript 類型
- [x] 2.4.1 建立 src/types/stock.ts
- [x] 2.4.2 建立 src/types/api.ts
- [x] 2.4.3 建立 src/types/signal.ts
- [x] 2.4.4 確保類型匹配後端 models

### 2.5 API Client
- [x] 2.5.1 建立 src/lib/api.ts
- [x] 2.5.2 實作 fetchStocks()
- [x] 2.5.3 實作 fetchStockDetail()
- [x] 2.5.4 實作 fetchStockHistory()
- [x] 2.5.5 實作 fetchHealth()
- [x] 2.5.6 新增錯誤處理與類型安全

**註記**：TanStack Query（原 2.5.7）已移至增強功能（6.7.1）

### 2.6 主要佈局
- [x] 2.6.1 建立 src/components/Layout.tsx 根佈局元件
- [x] 2.6.2 建立標題元件（Header）包含標題與主題切換
- [x] 2.6.3 建立頁尾元件（Footer）包含最後更新時間
- [x] 2.6.4 實作主題切換功能（useContext + localStorage）
- [x] 2.6.5 測試主題持久化

### 2.7 股票列表檢視 - 表格元件
- [x] 2.7.1 建立 src/components/StockTable.tsx
- [x] 2.7.2 實作表格標頭
- [x] 2.7.3 新增排序指示器
- [x] 2.7.4 實作列懸停效果
- [x] 2.7.5 新增訊號強度色彩標示
- [x] 2.7.6 使用 React Router Link 讓列可點擊
- [x] 2.7.7 測試 mock data

### 2.8 股票列表檢視 - 排序與分頁
- [x] 2.8.1 實作排序狀態（useState）
- [x] 2.8.2 新增排序處理器
- [x] 2.8.3 實作方向切換
- [x] 2.8.4 實作分頁控制項
- [x] 2.8.5 使用 useSearchParams 更新 URL query params
- [x] 2.8.6 從 API 取得資料（useEffect）
- [x] 2.8.7 顯示載入狀態

### 2.9 股票列表檢視 - 主頁面
- [x] 2.9.1 建立 src/pages/HomePage.tsx
- [x] 2.9.2 取得初始資料（useEffect + API client）
- [x] 2.9.3 傳遞資料給 StockTable 元件
- [x] 2.9.4 新增頁面標題
- [x] 2.9.5 實作載入骨架
- [x] 2.9.6 處理錯誤狀態
- [x] 2.9.7 測試 API 連線

### 2.10 股票詳細檢視 - 頁面結構
- [x] 2.10.1 建立 src/pages/StockDetailPage.tsx
- [x] 2.10.2 使用 useParams 取得 stockId
- [x] 2.10.3 取得股票詳細資料（useEffect + API）
- [x] 2.10.4 取得歷史資料
- [x] 2.10.5 建立麵包屑導航
- [x] 2.10.6 使用 React Router useNavigate 實作返回按鈕
- [x] 2.10.7 顯示基本資訊
- [x] 2.10.8 處理載入與錯誤

### 2.11 股票詳細檢視 - 訊號區塊
- [x] 2.11.1 建立 src/components/SignalsCard.tsx
- [x] 2.11.2 顯示訊號強度分數
- [x] 2.11.3 顯示訊號數量與主要訊號
- [x] 2.11.4 顯示預期報酬與勝率
- [x] 2.11.5 顯示風險等級
- [x] 2.11.6 列出所有訊號與觸發狀態
- [x] 2.11.7 卡片樣式設計

### 2.12 股票詳細檢視 - 圖表
- [x] 2.12.1 安裝與設定 Recharts
- [x] 2.12.2 建立 src/components/ConcentrationChart.tsx
- [x] 2.12.3 實作多線圖表
- [x] 2.12.4 新增圖例
- [x] 2.12.5 實作懸停工具提示
- [x] 2.12.6 設定圖表色彩
- [x] 2.12.7 使用 React.lazy 延遲載入圖表
- [x] 2.12.8 測試 12 週資料

### 2.13 股票詳細檢視 - 每週變化表格
- [x] 2.13.1 建立 src/components/WeeklyChangesTable.tsx
- [x] 2.13.2 顯示表格欄位
- [x] 2.13.3 顯示百分比變化
- [x] 2.13.4 實作漸層色彩標示
- [x] 2.13.5 處理連續增加的深色標示
- [x] 2.13.6 格式化日期與百分比
- [x] 2.13.7 測試不同資料模式

### 2.14 響應式設計
- [x] 2.14.1 測試 1920px 佈局
- [x] 2.14.2 測試 1440px 佈局
- [x] 2.14.3 測試 1024px 佈局
- [x] 2.14.4 測試 768px 佈局
- [x] 2.14.5 調整表格水平捲動
- [x] 2.14.6 確保觸控目標尺寸
- [x] 2.14.7 測試所有中斷點

**註記**：無障礙性任務（原 2.15.1-2.15.8）已移至增強功能（6.4.1-6.4.8）

### 2.16 效能最佳化
- [x] 2.16.1 使用 Vite 的 Code Splitting（動態 import）
- [x] 2.16.2 使用 React.lazy 延遲載入圖表元件
- [x] 2.16.4 使用 useMemo 記憶化計算
- [x] 2.16.5 使用 React.memo 避免不必要的重渲染

**註記**：防抖輸入（原 2.16.3）、Lighthouse 優化（原 2.16.6）、慢速網路測試（原 2.16.7）已移至增強功能（6.1.3-6.1.5）

### 2.17 測試
- [ ] 2.17.1 撰寫工具函式單元測試
- [ ] 2.17.2 使用 Vitest 測試 StockTable 元件
- [ ] 2.17.3 測試 SignalsCard 元件
- [ ] 2.17.4 測試 ConcentrationChart 元件
- [ ] 2.17.5 撰寫頁面導航整合測試
- [ ] 2.17.6 測試 API client mock 回應
- [ ] 2.17.7 執行所有測試

### 2.18 錯誤處理與邊界情況
- [x] 2.18.1 處理 API 連線錯誤
- [x] 2.18.2 顯示使用者友善錯誤訊息
- [x] 2.18.6 顯示載入骨架

**註記**：自動重試機制（原 2.18.3）、空資料狀態處理（原 2.18.4）、歷史資料不足處理（原 2.18.5）、全面錯誤測試（原 2.18.7）已移至增強功能（6.6.1-6.6.4）

### 2.19 部署準備
- [x] 2.19.1 建立 .env.example（包含 VITE_API_BASE_URL、VITE_REQUIRE_AUTH、VITE_GOOGLE_CLIENT_ID）
- [x] 2.19.2 設定正式環境變數
- [x] 2.19.3 建立 Dockerfile（multi-stage build: build + nginx serve）
- [x] 2.19.4 建立 nginx.conf 用於 SPA 路由（historyApiFallback）
- [x] 2.19.5 新增到 docker-compose.yml
- [x] 2.19.6 執行 `npm run build` 並測試 dist/ 輸出
- [x] 2.19.7 記錄部署流程（Nginx、GitHub Pages、Netlify）

### 2.20 Google OAuth 整合（可選功能）
- [x] 2.20.1 安裝 Google OAuth：`npm install @react-oauth/google`
- [ ] 2.20.2 在 Google Cloud Console 建立 OAuth 2.0 憑證
- [x] 2.20.3 設定 Google OAuth redirect URI（http://localhost:5173）
- [x] 2.20.4 建立 `src/contexts/AuthContext.tsx` 認證狀態管理
- [x] 2.20.5 建立 AuthProvider 並包裹整個應用（在 src/main.tsx）
- [x] 2.20.6 建立 `src/components/LoginButton.tsx`（顯示登入/登出按鈕）
- [x] 2.20.7 建立 `src/pages/LoginPage.tsx`（僅顯示 Google 登入按鈕）
- [x] 2.20.8 建立 `src/components/ProtectedRoute.tsx`（檢查 REQUIRE_AUTH 與 session）
- [x] 2.20.9 更新路由配置加入 /login 路徑
- [x] 2.20.10 在主要路由使用 ProtectedRoute 包裹（/、/stocks/:id）
- [x] 2.20.11 在 Header 元件加入 LoginButton（顯示使用者 email）
- [x] 2.20.12 實作 API client 在請求中附帶 token（Authorization header）
- [ ] 2.20.13 測試登入流程（Google OAuth 彈窗）
- [ ] 2.20.14 測試登出流程
- [ ] 2.20.15 測試 session 持久化（重新整理頁面）
- [ ] 2.20.16 測試 REQUIRE_AUTH=false（公開模式）
- [ ] 2.20.17 測試 REQUIRE_AUTH=true（需登入模式）
- [ ] 2.20.18 測試 email 不在白名單的錯誤處理

## 3. 整合與端對端測試
- [x] 2.21.5 實作成交量 series（柱狀圖）
- [x] 2.21.6 實作響應式容器（根據螢幕尺寸調整）
- [x] 2.21.7 測試深色/淺色主題切換

### 2.22 技術指標整合
- [x] 2.22.1 實作移動平均線疊加（MA5/10/20/60）
- [x] 2.22.2 建立 `src/components/IndicatorPanel.tsx`（MACD/KD/RSI 子圖表）
- [x] 2.22.3 實作指標開關 UI（checkbox 或 toggle buttons）
- [x] 2.22.4 實作 URL 參數同步（useSearchParams）
- [x] 2.22.5 根據啟用的指標動態渲染

### 2.23 時間範圍選擇器
- [x] 2.23.1 建立時間範圍按鈕組（1M/3M/6M/1Y）
- [x] 2.23.2 實作切換邏輯與 API 重新請求
- [x] 2.23.3 更新 URL 參數
- [x] 2.23.4 顯示載入狀態

### 2.24 籌碼資訊 Tabs
- [x] 2.24.1 建立 `src/components/ChipsTabs.tsx`
- [x] 2.24.2 實作 Tab 切換 UI（三大法人/主力/融資融券/集中度）
- [x] 2.24.3 從 API 取得籌碼資料
- [x] 2.24.4 以表格顯示各類籌碼資料

### 2.25 基本面資訊卡片（FundamentalCard）
- [x] 2.25.1 建立 `src/components/FundamentalCard.tsx` 組件
- [x] 2.25.2 實作 6 個 Tab 切換 UI（獲利能力/營收成長/估值指標/持股結構/股本結構/訊號列表）
- [x] 2.25.3 從 API 取得基本面資料（`GET /api/stocks/{stock_id}/fundamental`）
- [x] 2.25.4 建立 `FundamentalInfo` TypeScript 介面 (`dashboard/src/types/stock.ts:151-179`)
- [x] 2.25.5 建立 `MonthlyRevenueDetail` TypeScript 介面 (`dashboard/src/types/stock.ts:140-149`)

### 2.26 獲利能力 Tab
- [x] 2.26.1 實作 EPS 趨勢折線圖（使用 Recharts LineChart）
- [x] 2.26.2 顯示 EPS 統計指標（平均值、趨勢、穩定性）
- [x] 2.26.3 顯示逐季 EPS 詳細表格
- [x] 2.26.4 處理 EPS 資料不足情況

### 2.27 營收成長 Tab
- [x] 2.27.1 實作月營收趨勢圖（使用 ComposedChart：柱狀圖 + 折線圖）
- [x] 2.27.2 左 Y 軸顯示月營收（百萬元），右 Y 軸顯示年增率（%）
- [x] 2.27.3 顯示營收統計指標卡片（營收趨勢、平均年增率、最新月營收）
- [x] 2.27.4 顯示12個月營收詳細表格（含 year/month, revenue, MoM, YoY, cumulative）
- [x] 2.27.5 實作數值自動著色（正值綠色、負值紅色、空值灰色）
- [x] 2.27.6 處理月營收資料缺失情況（顯示「暫無月營收資料」）

### 2.28 估值指標 Tab
- [x] 2.28.1 顯示本益比（PER）卡片
- [x] 2.28.2 顯示股利殖利率卡片
- [x] 2.28.3 顯示配息率卡片
- [x] 2.28.4 顯示現金股利卡片
- [x] 2.28.5 實作卡片佈局（2x2 網格）
- [x] 2.28.6 處理負值本益比顯示

### 2.29 持股結構 Tab
- [x] 2.29.1 顯示董監持股比率
- [x] 2.29.2 顯示外資持股率
- [x] 2.29.3 顯示投信持股率
- [x] 2.29.4 顯示自營商持股率
- [x] 2.29.5 實作彩色分組卡片佈局
- [x] 2.29.6 顯示最新資料日期
- [x] 2.29.7 處理持股結構資料缺失情況（顯示「暫無持股結構資料」）

### 2.30 股本結構 Tab
- [x] 2.30.1 顯示實收資本額（百萬元）
- [x] 2.30.2 顯示股本（千股）
- [x] 2.30.3 顯示股票股利
- [x] 2.30.4 實作卡片佈局
- [x] 2.30.5 處理股本資訊缺失情況（顯示「暫無股本結構資料」）

### 2.31 訊號列表 Tab
- [x] 2.31.1 顯示所有觸發的基本面訊號
- [x] 2.31.2 實作訊號卡片（綠色為正分、紅色為負分）
- [x] 2.31.3 顯示訊號名稱、分數、描述
- [x] 2.31.4 處理無觸發訊號情況（顯示「無觸發訊號」）

### 3.1 本地開發設置
- [ ] 3.1.1 更新主要 docker-compose.yml
- [ ] 3.1.2 設定服務間網路
- [ ] 3.1.3 設定本地環境變數
- [ ] 3.1.4 測試 docker-compose up
- [ ] 3.1.5 驗證前端可連接後端
- [ ] 3.1.6 測試後端資料庫連線

### 3.2 端對端工作流程
- [ ] 3.2.1 測試：載入首頁 → 查看列表 → 排序 → 分頁
- [ ] 3.2.2 測試：點擊股票 → 檢視詳細 → 查看圖表 → 返回
- [ ] 3.2.3 測試：切換主題 → 驗證持久化 → 重整
- [ ] 3.2.4 測試：排序 → 點擊詳細 → 返回 → 排序保留
- [ ] 3.2.5 測試：API 失敗 → 錯誤訊息 → 重試
- [ ] 3.2.6 測試實際 2700 支股票

**註記**：效能驗證（原 3.3.x）、跨瀏覽器測試（原 3.4.x）、負載測試（原 3.5.x）已移至增強功能（6.2.x、6.3.x、6.5.x）

## 4. 文件

### 4.1 API 文件
- [ ] 4.1.1 撰寫 api/README.md 概述
- [ ] 4.1.2 記錄所有端點與範例
- [ ] 4.1.3 記錄身份驗證/授權
- [ ] 4.1.4 記錄環境變數
- [ ] 4.1.5 記錄錯誤代碼與訊息
- [ ] 4.1.6 新增架構圖

### 4.2 前端文件
- [ ] 4.2.1 撰寫 dashboard/README.md 概述
- [ ] 4.2.2 記錄元件結構
- [ ] 4.2.3 記錄主題客製化
- [ ] 4.2.4 記錄環境變數
- [ ] 4.2.5 記錄建置與部署
- [ ] 4.2.6 新增功能截圖

### 4.3 部署指南
- [ ] 4.3.1 撰寫 Docker 部署說明（Nginx 提供靜態檔案）
- [ ] 4.3.2 撰寫前端靜態檔案部署說明（Nginx/GitHub Pages/Netlify）
- [ ] 4.3.3 撰寫後端部署到 Railway/Fly.io 說明
- [ ] 4.3.4 記錄環境變數設定
- [ ] 4.3.5 記錄資料庫連線設置
- [ ] 4.3.6 建立疑難排解章節

### 4.4 使用者指南
- [ ] 4.4.1 撰寫儀表板功能使用指南
- [ ] 4.4.2 解釋訊號強度解讀
- [ ] 4.4.3 解釋籌碼集中度指標
- [ ] 4.4.4 記錄排序與篩選功能
- [ ] 4.4.5 新增 FAQ 章節
- [ ] 4.4.6 包含註解截圖

## 5. 部署

### 5.1 後端部署
- [ ] 5.1.1 選擇部署平台（Railway、Fly.io 或 VPS）
- [ ] 5.1.2 建立正式資料庫連線
- [ ] 5.1.3 設定正式環境變數
- [ ] 5.1.4 部署後端 API
- [ ] 5.1.5 測試已部署的 API 端點
- [ ] 5.1.6 設置監控與日誌

### 5.2 前端部署
- [ ] 5.2.1 選擇部署平台（Nginx 靜態檔案/GitHub Pages/Netlify 或 VPS）
- [ ] 5.2.2 執行 `npm run build` 產生 dist/ 靜態檔案
- [ ] 5.2.3 設定環境變數（VITE_API_BASE_URL）
- [ ] 5.2.4 部署靜態檔案到選定平台
- [ ] 5.2.5 測試已部署的前端
- [ ] 5.2.6 驗證前後端連線（檢查 CORS）
- [ ] 5.2.7 設置監控與分析

### 5.3 正式環境驗證
- [ ] 5.3.1 在正式環境冒煙測試所有功能
- [ ] 5.3.2 測試實際正式資料
- [ ] 5.3.3 驗證效能符合目標
- [ ] 5.3.4 檢查錯誤日誌與監控
- [ ] 5.3.5 從不同地理位置測試
- [ ] 5.3.6 記錄正式環境 URL 與存取資訊

### 5.4 交接
- [ ] 5.4.1 審查所有文件
- [ ] 5.4.2 向利害關係人展示儀表板
- [ ] 5.4.3 提供部署憑證
- [ ] 5.4.4 設置監控警報
- [ ] 5.4.5 排程後續回饋
- [ ] 5.4.6 以 `openspec archive add-chip-dashboard` 封存提案

## 6. 增強功能（可選）

這些功能為「Nice to have」的優化項目，非 MVP 必要功能。可在完成核心任務後，依優先級選擇性實作。

### 6.1 效能優化增強
- [ ] 6.1.1 新增 ETag 支援用於條件請求（原 1.10.3）
- [ ] 6.1.2 以 2700 支股票負載測試 API 並測量回應時間（原 1.10.5）
- [ ] 6.1.3 防抖輸入（原 2.16.3）
- [ ] 6.1.4 最佳化 Lighthouse 分數（目標 >90）（原 2.16.6）
- [ ] 6.1.5 測試慢速網路（Slow 3G）（原 2.16.7）

### 6.2 效能驗證
- [ ] 6.2.1 測量列表 API 回應時間（<500ms）（原 3.3.1）
- [ ] 6.2.2 測量詳細 API 回應時間（<1000ms）（原 3.3.2）
- [ ] 6.2.3 測量歷史 API 回應時間（<1000ms）（原 3.3.3）
- [ ] 6.2.4 測量初始頁面載入（<2s）（原 3.3.4）
- [ ] 6.2.5 測量圖表渲染（<500ms）（原 3.3.5）
- [ ] 6.2.6 測量排序操作（<200ms）（原 3.3.6）
- [ ] 6.2.7 最佳化超過目標的操作（原 3.3.7）

### 6.3 負載測試
- [ ] 6.3.1 使用 Apache Bench 或 wrk（原 3.5.1）
- [ ] 6.3.2 測試 100 並發請求（原 3.5.2）
- [ ] 6.3.3 測試 500 並發請求（原 3.5.3）
- [ ] 6.3.4 測量負載下回應時間（原 3.5.4）
- [ ] 6.3.5 識別並修復瓶頸（原 3.5.5）
- [ ] 6.3.6 確保處理正式負載（原 3.5.6）

### 6.4 無障礙性
- [ ] 6.4.1 新增 alt text（原 2.15.1）
- [ ] 6.4.2 確保表單標籤關聯（原 2.15.2）
- [ ] 6.4.3 測試鍵盤導航（原 2.15.3）
- [ ] 6.4.4 確保 Enter/Space 啟動（原 2.15.4）
- [ ] 6.4.5 新增 ARIA labels（原 2.15.5）
- [ ] 6.4.6 測試螢幕閱讀器（原 2.15.6）
- [ ] 6.4.7 檢查色彩對比（原 2.15.7）
- [ ] 6.4.8 實作 prefers-reduced-motion（原 2.15.8）

### 6.5 跨瀏覽器相容性
- [ ] 6.5.1 測試 Chrome（原 3.4.1）
- [ ] 6.5.2 測試 Firefox（原 3.4.2）
- [ ] 6.5.3 測試 Safari（原 3.4.3）
- [ ] 6.5.4 測試 Edge（原 3.4.4）
- [ ] 6.5.5 修復瀏覽器特定問題（原 3.4.5）
- [ ] 6.5.6 驗證一致性（原 3.4.6）

### 6.6 錯誤處理增強
- [ ] 6.6.1 實作自動重試機制（原 2.18.3）
- [ ] 6.6.2 處理空資料狀態（原 2.18.4）
- [ ] 6.6.3 處理歷史資料不足（原 2.18.5）
- [ ] 6.6.4 測試所有錯誤情境（原 2.18.7）

### 6.7 資料管理優化
- [ ] 6.7.1 設定 TanStack Query 用於 API 快取（原 2.5.7）

## 相依性與平行化說明

**可平行化：**
- 章節 1（後端 API）與章節 2（前端儀表板）可由不同團隊成員同時開發
- 各章節內的許多子章節可平行處理（例如 models、routes、components）
- 章節 6（增強功能）可在核心功能完成後選擇性平行開發

**必須順序執行：**
- 章節 1 必須完成 1.1-1.3 才能開始 1.4-1.7（需要專案結構與 models）
- 章節 2 必須完成 2.1-2.4 才能開始後續章節（需要專案設置與 API client）
- 章節 3 需要章節 1 與 2 的核心功能都完成
- 章節 4（文件）可在各模組完成後即開始撰寫
- 章節 5（部署）需要章節 3 通過
- 章節 6（增強功能）建議在章節 1-5 完成後再進行

**關鍵路徑：**
1. 後端 API 設置（1.1-1.3）→ API 端點（1.4-1.7）→ 測試（1.12）→ 部署準備（1.13）
2. 前端設置（2.1-2.4）→ 元件（2.6-2.13）→ 測試（2.17）→ 部署準備（2.19）
3. 整合測試（3.1-3.2）
4. 文件（4.1-4.4）
5. 部署（5.1-5.3）
6. 增強功能（6.x）- 可選，依優先級執行
