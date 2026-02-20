## Context

現有基本面分析系統（`FundamentalSignalService`）有 10 個訊號，全部基於 EPS + PER + 股利殖利率。營收資料（`stock_monthly_revenue`）已存在但完全未參與評分。股利只存最新一年（`stock_info.cash_dividend`），無法追蹤歷史趨勢。估值指標僅有 PER。

**現有資料流：**
```
Wantgoo API → fetch_info() → stock_info (PER, 股利)
                            → stock_eps (逐季 EPS)
           → fetch_monthly_revenue() → stock_monthly_revenue (12 個月)
           → ex-dividend-data → 只取 dividend[0]，丟棄歷史
```

**現有訊號分布：** EPS 4 個 + PER 2 個 + 殖利率 2 個 + 穩定性/配息 2 個 = 10 個。營收 0 個。

## Goals / Non-Goals

**Goals:**
- 新增 4-6 個營收訊號，讓營收資料參與基本面評分
- 新增 `stock_dividend` 表存儲歷史股利，支援股利趨勢分析
- 新增 PSR 指標，用現有資料（營收 + 股數 + 股價）計算
- 新增基礎 EPS 預測（季節性 + 營收趨勢）
- 更新正規化公式以涵蓋新訊號

**Non-Goals:**
- 不做 PBR（每股淨值資料不在現有 API 中）
- 不做 ROE/ROA（需資產負債表，超出現有資料源）
- 不做機器學習模型 — EPS 預測用簡單統計方法
- 不改變現有 10 個訊號的定義或分數

## Decisions

### 1. 營收訊號設計：使用已存在的 revenue DataFrame

**決策：** 在 `calculate_signals()` 新增 `revenue_data` 參數，直接使用 `bulk_load_monthly_revenue()` 回傳的 DataFrame。

**理由：** `stock_service.py` 的 `get_fundamental_info()` 已經載入 12 個月營收資料，只需將同一份 DataFrame 傳入訊號計算即可，零額外查詢。

**替代方案：** 另建營收訊號服務 → 拒絕，因為營收訊號邏輯簡單，不需獨立服務。

### 2. 歷史股利存儲：新增 `stock_dividend` 表

**決策：** 新增 `stock_dividend` 表（stock_id, year, cash_dividend, stock_dividend），修改 `fetch_info()` 存儲 Wantgoo `ex-dividend-data` 回傳的所有年度。

**理由：** Wantgoo API 已經回傳完整歷史股利（`dividend` 陣列），目前只取 `dividend[0]`，改為全部存入即可。
`stock_info` 的 `cash_dividend`/`stock_dividend` 欄位保留不動（向後相容），用於快速查詢最新股利。

**替代方案：** 擴展 `stock_info` 加入歷史 → 拒絕，因為是多年一對多關係，需獨立表。

### 3. PSR 計算：即時計算，不存儲

**決策：** PSR = 市值 / 年營收 = (股價 × 流通股數) / (近 12 月營收累計)。在 `get_fundamental_info()` 即時計算。

**理由：** 輸入資料（股價、流通股數、營收）已全部存在，PSR 隨股價變動，存儲意義不大。

### 4. EPS 預測：季節性加權平均 + 營收年增率調整

**決策：** 預測下一季 EPS = 去年同季 EPS × (1 + 近 3 月平均營收年增率 / 100)。如果去年同季 EPS 不存在，fallback 到最近 4 季平均。

**理由：** 簡單、可解釋、不需訓練。營收年增率是最直覺的成長代理指標。

**替代方案：** 線性回歸 → 過度工程化，且 4-8 個 EPS 數據點不足以建立可靠模型。

### 5. 正規化公式更新策略

**決策：** 新增訊號後重新計算理論最大/最小分數，更新公式。所有股票的分數會在下次 API 請求時自動重算（因為 `calculate_signals` 是即時計算，不存儲標準化分數）。

**理由：** 現有架構已是即時計算 + 快取（TTL 6 小時），更新公式後快取過期即自動更新。

## Risks / Trade-offs

**[分數漂移]** 新增訊號後正規化範圍擴大，同樣的原始分數在新公式下對應的 normalized 值會不同
→ 緩解：一次性更新，發布時清除快取 (`/api/cache/clear`)

**[EPS 預測誤差]** 簡單模型準確度有限，尤其對周期性產業或一次性損益
→ 緩解：UI 明確標示「預測值」，不參與訊號評分（僅展示用）

**[Wantgoo 股利 API 變更]** 目前 `ex-dividend-data` 回傳格式未來可能變動
→ 緩解：`fetch_info()` 已有錯誤處理，新增存儲邏輯也加入防禦

**[資料庫 migration]** 新增 `stock_dividend` 表需要 DDL 變更
→ 緩解：是新增表（CREATE TABLE），不影響現有表結構
