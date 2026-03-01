## 1. vectorized_indicators.py — 新增 +DI/-DI 計算

- [x] 1.1 在 `calculate_all_indicators()` 中 ADX 計算後，新增 `talib.PLUS_DI` 和 `talib.MINUS_DI` 計算，存入 `df['plus_di']` 和 `df['minus_di']`

## 2. vectorized_signals.py — 實作 DMI 訊號偵測

- [x] 2.1 `SIGNAL_TYPE_MAP` 新增 `"DMI向上": SIGNAL_TYPE_STATE` 和 `"DMI向下": SIGNAL_TYPE_STATE`
- [x] 2.2 `detect_all_signals()` 取代 TODO 註解，實作 DMI向上（ADX>25 且 +DI>-DI）和 DMI向下（ADX>25 且 -DI>+DI）
- [x] 2.3 `calculate_signal_strength()` 更新正規化公式：min_score=-182, max_score=184

## 3. 正規化公式同步更新

- [x] 3.1 `twstock/signal_updater.py` 更新正規化公式（-182/366）
- [x] 3.2 `api/services/stock_service.py` 的 `normalize_technical_score()` 更新正規化公式（-182/366）

## 4. docs/SIGNAL_DEFINITIONS.md — 更新訊號定義

- [x] 4.1 State 訊號表新增 DMI向上/DMI向下
- [x] 4.2 更新 State 理論範圍、總理論範圍、正規化公式
- [x] 4.3 移除「未實作的訊號」章節中的 DMI 條目
- [x] 4.4 變更歷史新增本次變更記錄
