## 1. 資料庫與資料層

- [x] 1.1 在 `twstock/database.py` 新增 `StockDividend` ORM model（stock_id, year, cash_dividend, stock_dividend, updated_at），加入複合主鍵 (stock_id, year)
- [x] 1.2 在 `twstock/database.py` 新增 `save_stock_dividends(stock_id, dividends_list)` 批次 UPSERT 方法
- [x] 1.3 在 `twstock/database.py` 新增 `bulk_load_dividends(stock_ids, years=5)` 批次載入方法，回傳 Dict[str, pd.DataFrame]
- [x] 1.4 修改 `twstock/wantgoo.py` `fetch_info()` — 遍歷 `dividend` 陣列全部年度，呼叫 `save_stock_dividends()` 存入歷史股利，`stock_info` 繼續存最新年度（向後相容）

## 2. 營收訊號

- [x] 2.1 在 `api/services/fundamental_signal_service.py` 的 `calculate_signals()` 新增 `revenue_data: pd.DataFrame = None` 參數
- [x] 2.2 實作 5 個營收訊號：營收連續正成長(+12)、營收加速成長(+10)、營收由衰轉增(+15)、營收連續衰退(-12)、營收急凍(-15)，資料不足（<6 個月 yoy_change）時跳過
- [x] 2.3 在 `api/tests/test_fundamental_signals.py` 新增營收訊號單元測試（每個訊號至少觸發 + 不觸發各一個 case、資料不足 case）

## 3. 股利訊號

- [x] 3.1 在 `api/services/fundamental_signal_service.py` 的 `calculate_signals()` 新增 `dividend_history: pd.DataFrame = None` 參數
- [x] 3.2 實作 2 個股利訊號：股利連續成長(+10)、股利大幅削減(-12)，資料不足（<2 年）時跳過
- [x] 3.3 在 `api/tests/test_fundamental_signals.py` 新增股利訊號單元測試

## 4. PSR 估值

- [x] 4.1 在 `api/services/stock_service.py` `get_fundamental_info()` 中計算 PSR = (股價 × 流通股數 × 1000) / (近12月營收累加 × 1000)
- [x] 4.2 在 `api/services/fundamental_signal_service.py` 的 `calculate_signals()` 新增 `psr: float = None` 參數，實作 PSR偏低(+8) / PSR過高(-8) 訊號
- [x] 4.3 在 `api/tests/test_fundamental_signals.py` 新增 PSR 訊號單元測試

## 5. EPS 預測

- [x] 5.1 在 `api/services/fundamental_signal_service.py` 新增 `predict_next_eps(eps_data, revenue_data)` 方法，實作季節性 + 營收年增率調整公式
- [x] 5.2 在 `api/tests/test_fundamental_signals.py` 新增 EPS 預測單元測試（有去年同季、fallback、無營收、資料不足 4 個 case）

## 6. 正規化公式更新

- [x] 6.1 更新 `FundamentalSignalService.normalize_score()` — 新理論範圍 -120 ~ +148，公式 `((raw + 120) / 268) * 100`
- [x] 6.2 更新 `SIGNALS` 字典，加入所有新訊號定義
- [x] 6.3 更新 `docs/SIGNAL_DEFINITIONS.md` 基本面訊號章節 — 新增 9 個訊號定義、更新正規化公式、記錄變更歷史

## 7. API 整合

- [x] 7.1 更新 `api/models/stock.py` `FundamentalInfo` — 新增 `psr`, `eps_prediction`, `dividend_history` 欄位及對應的 Pydantic model（EPSPrediction, DividendDetail）
- [x] 7.2 修改 `api/services/stock_service.py` `get_fundamental_info()` — 載入歷史股利、計算 PSR、計算 EPS 預測、將營收/股利/PSR 傳入 `calculate_signals()`
- [x] 7.3 更新 `api/tests/test_fundamental_signals.py` 中既有測試 — 確保新增參數不破壞現有測試（新參數預設為 None 時行為不變）

## 8. 前端

- [x] 8.1 更新 `dashboard/src/types/stock.ts` — FundamentalInfo 新增 `psr`, `eps_prediction`, `dividend_history` 介面
- [x] 8.2 修改 `dashboard/src/components/FundamentalCard.tsx` 估值指標 tab — 新增 PSR 卡片
- [x] 8.3 修改 `dashboard/src/components/FundamentalCard.tsx` 估值指標 tab — 新增歷史股利趨勢圖
- [x] 8.4 修改 `dashboard/src/components/FundamentalCard.tsx` 獲利能力 tab — EPS 趨勢圖以虛線延伸顯示預測 EPS
