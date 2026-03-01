## Context

後端已用 talib 計算 ADX（`vectorized_indicators.py:92-108`），但未計算 +DI/-DI。`SignalScore` 已定義 `DMI_UP`(+10) / `DMI_DOWN`(-10) 分數常數但偵測邏輯為 TODO。

## Goals / Non-Goals

**Goals:**
- 計算 +DI/-DI 指標並存入 DataFrame
- 實作 DMI向上/DMI向下 訊號偵測
- 更新正規化公式以涵蓋新訊號的分數範圍

**Non-Goals:**
- 不修改資料庫 schema（+DI/-DI 僅存在記憶體 DataFrame，不持久化）
- 不新增 API 端點

## Decisions

### 1. DMI 訊號定義

- **DMI向上** (+10, State): ADX > 25 且 +DI > -DI — 強趨勢且方向向上
- **DMI向下** (-10, State): ADX > 25 且 -DI > +DI — 強趨勢且方向向下

State 型訊號，每天驗證條件。ADX > 25 是業界標準的強趨勢門檻。

### 2. +DI/-DI 僅在 DataFrame 中計算，不存資料庫

理由：+DI/-DI 僅用於訊號偵測，不需要在 API 或前端展示。存入 DataFrame 供 `detect_all_signals()` 使用即可。

### 3. 正規化公式更新

新增 2 個 State 訊號（+10, -10），State 範圍從 -68~+80 變為 -78~+90：
- 新總範圍：-182 ~ +184（原 -172 ~ +174）
- 新公式：`((raw_score + 182) / 366) * 100`

需同步更新 4 處：`vectorized_signals.py`、`signal_updater.py`、`stock_service.py`、`stock_list_cache_updater.py`（間接）

## Risks / Trade-offs

- **分數微幅偏移**：正規化範圍擴大，所有現有股票的 normalized_score 會略降（約 1-2 分）。可接受，不影響排序。→ 不處理。
