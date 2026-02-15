# Change: 新增現代化籌碼集中度儀表板

## Why

目前 twstock 將籌碼集中度分析產生成 Excel 檔案，使用者必須下載並開啟試算表才能查看股票訊號。這在工作流程中造成摩擦，也難以快速識別高潛力股票或互動式分析趨勢。現代化的網頁儀表板可以實現：

- **即時瀏覽** 籌碼集中度訊號，無需下載檔案
- **互動式排序與篩選** 快速找到符合特定條件的股票
- **視覺化趨勢分析** 透過圖表顯示近 3 個月的歷史資料
- **更好的可訪問性** 從任何裝置的瀏覽器存取

## What Changes

此變更引入完整的網頁儀表板系統，包含前端與後端元件：

### 前端 (新增)
- 現代化高密度資料儀表板，可排序的表格顯示所有股票
- 顯示籌碼集中度訊號：分數、訊號數量、預期報酬、勝率
- 點擊進入個股詳細頁面，包含：
  - **專業 K 線圖**（Lightweight Charts）+ 時間範圍選擇器（1M/3M/6M/1Y）
  - **可開關技術指標**：移動平均線、MACD、KD、RSI、布林通道、Pivot Points
  - **籌碼集中度趨勢**（大戶 >400/1000 張、散戶 <20 張）
  - **籌碼詳細資訊**：三大法人買賣超、主力買賣超、融資融券
- 針對桌面和平板優化的響應式設計
- 支援深色/淺色主題
- Google 帳號登入與 email 白名單（可選功能，環境變數控制）

### 後端 API (新增)
- RESTful API 端點，從 PostgreSQL 資料庫提供股票資料
- `/api/stocks` - 列出所有具籌碼集中度訊號的股票（支援排序/篩選）
- `/api/stocks/{stock_id}` - 取得特定股票的詳細資訊
- `/api/stocks/{stock_id}/history` - 取得籌碼集中度歷史資料
- `/api/stocks/{stock_id}/chart` - 取得 K 線資料與技術指標（支援時間範圍與指標選擇）
- `/api/stocks/{stock_id}/chips` - 取得三大法人、主力、融資融券資料
- Token 驗證（可選功能，環境變數控制）
- CORS 設定支援本地開發與正式環境部署

### 技術堆疊
- **前端**: React 18+, TypeScript, Vite, React Router v6, Tailwind CSS, Lightweight Charts（K 線圖）, Recharts（籌碼圖）, Auth.js（身份驗證）
- **後端**: FastAPI (Python) 透過現有 DatabaseManager 進行非同步 PostgreSQL 存取，ta-lib（技術指標計算），python-jose（JWT 驗證）
- **身份驗證**: Auth.js + Google OAuth 2.0 + Email 白名單（可選功能，環境變數控制）
- **部署**: 前端靜態檔案可部署至任何網頁伺服器（Nginx、Apache），後端獨立部署

## Impact

### 影響的規格
- **chip-dashboard** (新增): 前端儀表板規格
- **chip-api** (新增): 後端 API 規格

### 影響的程式碼
- **新增目錄**:
  - `dashboard/` - 前端 React SPA 應用程式
  - `api/` - 後端 FastAPI 服務
- **重用模組**:
  - `twstock/database.py` - DatabaseManager 用於資料存取
  - 現有的 PostgreSQL 資料庫結構（無需變更）

### Breaking Changes
無 - 這是純新增功能。現有的 Excel 產生功能保持不變。

### Migration Path
不適用 - 新功能，無需遷移。

## Success Criteria
- [x] 使用者可在網頁介面瀏覽所有具籌碼集中度訊號的股票
- [x] 使用者可依分數、預期報酬、勝率、訊號數量排序
- [x] 使用者可點擊股票查看近 3 個月歷史趨勢圖表
- [x] API 回應時間：列表頁 <500ms、詳細頁 <1000ms
- [x] 儀表板支援響應式設計，在桌面（1920px）和平板（768px）正常運作
- [x] 支援深色與淺色兩種主題
- [ ] 使用者可透過 Google 帳號登入（可選功能，未實作）
- [ ] Email 白名單正確控制存取權限（僅白名單內的 email 可存取）（可選功能，未實作）
- [x] 可透過環境變數開關驗證功能（REQUIRE_AUTH=true/false）
- [ ] 未授權使用者看到清楚的錯誤訊息（驗證功能未實作）
