# 籌碼儀表板開發進度記錄

**最後更新**: 2026-01-04 (訊號計算功能完成)

---

## 專案概況

建立台股籌碼集中度分析儀表板，取代 Excel 工作流程。

**技術堆疊**:
- 前端: React 18 + Vite + Tailwind CSS
- 圖表: Lightweight Charts (K線) + Recharts (籌碼圖)
- 後端: FastAPI + PostgreSQL
- 身份驗證: Auth.js (尚未實作)

---

## 已完成功能 ✅

### 後端 API (FastAPI)
- ✅ `/api/stocks` - 股票列表（排序、分頁）
  - ✅ 訊號強度計算（10個訊號）
  - ✅ 預期報酬與勝率
- ✅ `/api/stocks/{stock_id}` - 股票詳細資訊
  - ✅ 完整訊號列表與說明
  - ✅ 訊號強度標準化（0-100）
- ✅ `/api/stocks/{stock_id}/history` - 籌碼集中度歷史（12週）
- ✅ `/api/stocks/{stock_id}/chart` - K線資料（OHLCV）
- ✅ `/api/stocks/{stock_id}/chips` - 籌碼詳細資料（三大法人/主力/融資融券）
- ✅ CORS 設定
- ✅ 資料庫整合（DatabaseManager）

### 前端儀表板
- ✅ 股票列表頁（排序、分頁）
  - ✅ 顯示訊號強度、預期報酬、勝率
  - ✅ 主要訊號列表
  - ✅ 風險等級標示
- ✅ 股票詳細頁
- ✅ K線圖（Lightweight Charts）
  - 蠟燭圖
  - 成交量柱狀圖
  - 時間範圍選擇器（1M/3M/6M/1Y）
- ✅ 技術指標（**前端計算，即時切換**）
  - MA5/MA10/MA20/MA60
  - MACD（MACD線、訊號線、柱狀圖）
  - KD（K值、D值）
  - RSI
  - 布林通道（BB）
- ✅ 技術指標開關按鈕
- ✅ 籌碼集中度趨勢圖（原有功能）
- ✅ 每週籌碼變化表格
- ✅ **籌碼詳細資訊 Tabs**
  - ✅ 三大法人買賣超（30天資料）
  - ✅ 主力買賣超（30天資料）
  - ✅ 融資融券（30天資料）
  - ✅ 籌碼集中度摘要
- ✅ 深色/淺色模式切換
- ✅ 響應式設計

---

## 關鍵技術決策

### 1. React SPA vs Next.js
**決定**: 使用 React SPA（不用 Next.js）
**理由**: 內部工具不需要 SEO，SPA 更簡單

### 2. Auth.js vs Firebase
**決定**: 使用 Auth.js（尚未實作）
**理由**: 開源、自主控制、無第三方服務依賴

### 3. 技術指標計算位置
**決定**: 前端計算（使用 `technicalindicators` 套件）
**理由**: 即時切換、無 API 延遲、減少後端負擔

### 4. 圖表庫
**決定**: Lightweight Charts（K線） + Recharts（籌碼）
**理由**: Lightweight Charts 專業金融圖表效能佳

---

## 最近修復的問題

### 問題 1: DatabaseManager 方法名稱錯誤
- 錯誤: `init_db()` 不存在
- 修正: 改用 `init_database()`

### 問題 2: StockInfo 沒有 name 欄位
- 錯誤: 股票名稱在 `StockList` 表，不在 `StockInfo`
- 修正: JOIN StockList 表取得名稱

### 問題 3: 資料庫欄位命名
- 錯誤: 程式用 `moreThan400`，資料庫是 `more_than_400`
- 修正: 統一使用 snake_case

### 問題 4: FastAPI 路由順序
- 錯誤: `/{stock_id}/chart` 路由被 `/{stock_id}` 攔截
- 修正: 將具體路徑（`/chart`）定義在通用路徑（`/{stock_id}`）之前

### 問題 5: Lightweight Charts v5 API 變更
- 錯誤: `addCandlestickSeries()` 方法不存在
- 修正: 使用 `chart.addSeries(LightweightCharts.candlestickSeries, options)`

### 問題 6: MACD 欄位名稱
- 錯誤: `indicators.MACD?.macd` (小寫)
- 修正: `indicators.MACD?.MACD` (大寫，符合 technicalindicators 套件輸出)

---

## 當前系統狀態

**服務運行中**:
- ✅ PostgreSQL (Docker) - `./start-db.sh`
- ✅ FastAPI (port 8000) - `poetry run uvicorn api.main:app --reload`
- ✅ Vite dev server (port 5173) - `cd dashboard && npm run dev`

**資料庫連線**:
```bash
DATABASE_URL=postgresql+asyncpg://twstock_user:twstock_password123@localhost:5432/twstock
```

**環境變數**:
```bash
# Backend (.env)
DATABASE_URL=postgresql+asyncpg://...
ALLOWED_ORIGINS=http://localhost:5173,http://127.0.0.1:5173

# Frontend (dashboard/.env)
VITE_API_BASE_URL=http://localhost:8000
```

---

### 籌碼詳細資訊
- ✅ 三大法人買賣超資料（InstitutionalInvestors 表）
- ✅ 主力買賣超資料（MajorInvestors 表）
- ✅ 融資融券資料（MarginTrading 表）
- ✅ Tabs 切換 UI

### 訊號強度計算
- ✅ 10 個籌碼訊號邏輯實作
- ✅ 預期報酬計算（基於訊號分數線性估算）
- ✅ 歷史勝率統計（基於訊號數量估算）

## 待實作功能（Phase 2）

### 身份驗證
- ⏳ Auth.js Google OAuth 整合
- ⏳ Email 白名單驗證
- ⏳ 環境變數開關（REQUIRE_AUTH）
- ⏳ 登入/登出 UI

### 部署
- ⏳ Docker Compose 設定
- ⏳ Nginx 反向代理
- ⏳ 環境變數管理
- ⏳ 正式環境設定

---

## 重要檔案位置

### 後端
- `api/main.py` - FastAPI 入口
- `api/routers/stocks.py` - API 路由定義
- `api/services/stock_service.py` - 商業邏輯
- `api/models/stock.py` - Pydantic 模型

### 前端
- `dashboard/src/pages/StockListPage.tsx` - 列表頁
- `dashboard/src/pages/StockDetailPage.tsx` - 詳細頁
- `dashboard/src/components/CandlestickChart.tsx` - K線圖
- `dashboard/src/components/IndicatorCharts.tsx` - 技術指標子圖
- `dashboard/src/lib/indicators.ts` - **技術指標計算（前端）**
- `dashboard/src/lib/api.ts` - API 客戶端

### OpenSpec
- `openspec/changes/add-chip-dashboard/proposal.md` - 提案文件
- `openspec/changes/add-chip-dashboard/design.md` - 設計決策
- `openspec/changes/add-chip-dashboard/specs/` - 詳細規格
- `openspec/changes/add-chip-dashboard/tasks.md` - 任務清單

---

## 下次繼續開發時

### 啟動系統
```bash
# 1. 啟動資料庫
./start-db.sh

# 2. 啟動後端（新 terminal）
cd /Users/tony/twstock
poetry run uvicorn api.main:app --reload

# 3. 啟動前端（新 terminal）
cd /Users/tony/twstock/dashboard
npm run dev
```

### 測試連線
- 前端: http://localhost:5173
- 後端 API: http://localhost:8000/docs
- 健康檢查: http://localhost:8000/api/health

### 建議下一步
1. **測試系統功能**: 重新啟動後端並測試訊號計算
2. **Auth.js 整合**: 加入 Google 登入功能（可選）
3. **測試與優化**: 單元測試、整合測試、效能測試
4. **部署準備**: Docker Compose、環境變數、部署文件

---

## 籌碼訊號實作細節

**10 個訊號邏輯**（根據 CLAUDE.md 定義）:

### 買入訊號 (正分)
1. **完美結構** (+20分): 大戶↑ AND 超大戶↑ AND 散戶↓ (同週)
2. **超大戶連買2週** (+15分): moreThan1000 連續 2 週以上增加
3. **大戶連買3週** (+12分): moreThan400 連續 3 週以上增加
4. **大戶急買** (+10分): 單週 moreThan400 增加 >1%
5. **加速集中** (+10分): 近5週平均變化 > 前5週平均變化
6. **10週持續買** (+8分): W1-W10 變化 >3%
7. **散戶連賣3週** (+8分): lessThan20 連續 3 週以上減少
8. **散戶恐慌** (+6分): 單週 lessThan20 減少 >1%

### 風險警告 (負分)
9. **出貨訊號** (-15分): 大戶↓ AND 散戶↑ (同週)
10. **散戶狂熱** (-10分): lessThan20 連續 3 週以上增加

### 分數標準化
- 原始分數範圍: -25 ~ 89 分
- 標準化: 0-100 區間
- 風險等級:
  - 70-100: 低風險
  - 40-69: 中風險
  - 0-39: 高風險

### 預期報酬與勝率
- **預期報酬**: 訊號分數 × 0.13% (完美結構 20分 ≈ 2.6%)
- **勝率**: 基準 50% + 每個正向訊號 5%，上限 80%

---

## 技術指標實作細節

**前端即時計算**（使用 `technicalindicators` 套件）:

```typescript
// dashboard/src/lib/indicators.ts
import { SMA, MACD, Stochastic, RSI, BollingerBands } from 'technicalindicators'

export class IndicatorCalculator {
  static calculateAll(ohlcvData: OHLCVData[], enabledIndicators: string[]): CalculatedIndicators {
    // 根據 enabledIndicators 計算對應指標
    // MA5/10/20/60, MACD, KD, RSI, BB
  }
}
```

**使用方式**:
```typescript
// StockDetailPage.tsx
const calculatedIndicators = useMemo(() => {
  if (!chartData?.ohlcv) return {}
  return IndicatorCalculator.calculateAll(chartData.ohlcv, enabledIndicators)
}, [chartData, enabledIndicators])
```

**優勢**:
- ✅ 即時切換，無 API 延遲
- ✅ 減少後端負擔
- ✅ 用戶體驗更好

---

## OpenSpec 狀態

**Proposal**: add-chip-dashboard
**狀態**: 進行中（Phase 1 MVP 已完成）

**驗證命令**:
```bash
openspec validate add-chip-dashboard --strict
```

---

## 疑難排解

### 資料庫連線失敗
```bash
# 確認 Docker 是否運行
docker ps

# 重新啟動資料庫
./start-db.sh
```

### npm 權限問題
```bash
# 使用臨時 cache 目錄
npm install --cache /tmp/npm-cache
```

### Lightweight Charts 顯示問題
- 確認使用動態 import: `import('lightweight-charts')`
- 確認正確使用 v5 API: `chart.addSeries()`

---

**記錄完成時間**: 2026-01-04
**下次更新**: 實作 Phase 2 功能後更新
