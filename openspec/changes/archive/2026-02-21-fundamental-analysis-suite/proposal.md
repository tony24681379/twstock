## Why

現有基本面分析存在三個結構性缺口：
1. **營收資料有蒐集但沒產生訊號** — `stock_monthly_revenue` 表有 12 個月營收資料（含年增率、月增率），前端有展示，但 `FundamentalSignalService` 完全不使用營收資料來評分，導致營收加速成長或衰退的公司無法被訊號系統偵測到。
2. **估值維度單一** — 目前僅有 PER 一個估值指標（且只觸發 `本益比合理` / `本益比過高` 兩個訊號），缺少 PSR（股價營收比）等可用現有資料計算的估值指標。
3. **股利資料只存最新一年** — `stock_info` 僅存當年 `cash_dividend` / `stock_dividend`，Wantgoo API `ex-dividend-data` 回傳多年歷史股利，但 fetch_info 只取 `dividend[0]`。無法判斷股利是否連續成長或突然削減。

這三個缺口使得基本面訊號（10 個）的涵蓋面不夠廣——EPS 佔 4 個訊號、PER 佔 2 個、股利佔 2 個、穩定性/配息率各 1 個，**營收 0 個**。

## What Changes

### 新增營收訊號（4-6 個）
- 在 `FundamentalSignalService` 新增營收相關訊號，例如：營收連續正成長、營收年增率加速、營收由衰轉增、營收連續衰退
- 需要在 `calculate_signals()` 方法中加入 `revenue_data: pd.DataFrame` 參數
- 更新正規化公式（理論範圍會因新訊號而擴大）

### 新增 PSR 估值指標
- 利用現有 `stock_monthly_revenue`（年營收 = 近 12 月累加）+ `stock_info.outstanding_shares` + 股價 → 計算 PSR
- 前端「估值指標」tab 新增 PSR 卡片
- 可新增 PSR 相關訊號（PSR 偏低 / PSR 過高）

### 存儲歷史股利資料
- 新增 `stock_dividend` 資料表（stock_id, year, cash_dividend, stock_dividend）
- 修改 `wantgoo.py` `fetch_info()` 存儲多年股利（Wantgoo API 已回傳完整歷史）
- 新增訊號：股利連續成長、股利突然削減
- 前端「估值指標」tab 新增歷史股利趨勢圖

### EPS 預測（基礎版）
- 基於歷史 EPS 季節性模式 + 最新營收年增率，估算下一季 EPS
- 純計算邏輯，不需外部模型
- 前端「獲利能力」tab 顯示預測 EPS（標註為預測值）

### 更新評分體系
- `SIGNAL_DEFINITIONS.md` 新增所有新訊號的定義
- 重新校正正規化公式
- 更新前端訊號列表顯示

## Capabilities

### New Capabilities
- `revenue-signals`: 營收相關訊號計算（營收成長/衰退偵測、營收加速/減速判斷）
- `dividend-history`: 歷史股利資料存儲與分析（多年股利趨勢、股利成長率、連續配息年數）
- `eps-prediction`: 基礎 EPS 預測（季節性模式 + 營收趨勢推估）
- `psr-valuation`: PSR 估值指標計算與訊號

### Modified Capabilities
- `fundamental-info`: 新增營收訊號、PSR、歷史股利、EPS 預測等欄位到 FundamentalInfo 回應

## Impact

### 後端
- `api/services/fundamental_signal_service.py` — 新增營收/股利/PSR 訊號，修改 `calculate_signals()` 簽名
- `api/services/stock_service.py` — `get_fundamental_info()` 傳入營收資料、歷史股利、計算 PSR
- `api/models/stock.py` — `FundamentalInfo` 新增欄位（psr、eps_prediction、dividend_history 等）
- `twstock/database.py` — 新增 `StockDividend` 資料表
- `twstock/wantgoo.py` — `fetch_info()` 修改為存儲多年股利資料
- `docs/SIGNAL_DEFINITIONS.md` — 新增訊號定義、更新正規化公式

### 前端
- `dashboard/src/types/stock.ts` — FundamentalInfo 新增欄位
- `dashboard/src/components/FundamentalCard.tsx` — 估值 tab 新增 PSR、獲利 tab 新增預測 EPS、股利趨勢圖

### 資料庫
- 新增 `stock_dividend` 表（DDL migration）
- 正規化公式範圍變更（所有已算分數需要重新計算）

### 風險
- 正規化公式變更會影響所有股票的 `fundamental_strength` 分數，進而影響 `overall_strength` 排序
- EPS 預測為推估值，需在 UI 明確標示，避免誤導
