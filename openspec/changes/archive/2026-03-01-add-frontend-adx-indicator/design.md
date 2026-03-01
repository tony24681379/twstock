## Context

前端 K 線圖表使用 `technicalindicators` npm 庫在客戶端即時計算指標（MA、MACD、KD、RSI、BB），不依賴後端。ADX 需沿用相同模式。

`technicalindicators` 已提供 `ADX` 類別，輸入 high/low/close/period，輸出 `{ adx, pdi, mdi }`（ADX 值、+DI、-DI）。

現有指標分兩類：
- **主圖疊加**：MA、BB（畫在 K 線上）
- **副圖獨立**：MACD、RSI、KD（各自一個 Recharts 圖表）

ADX 屬於副圖類型，需獨立圖表顯示三條線。

## Goals / Non-Goals

**Goals:**
- 在前端新增 ADX 指標切換與副圖顯示
- 顯示 ADX、+DI、-DI 三條線及 ADX=25 參考線
- 沿用現有架構模式，不引入新依賴

**Non-Goals:**
- 不修改後端 API
- 不改動後端 vectorized_indicators.py 的計算邏輯
- 不增加 ADX 相關的技術訊號評分

## Decisions

### 1. 前端即時計算（非後端取資料）

沿用現有模式：前端用 `technicalindicators` 庫從 OHLCV 即時計算 ADX。

理由：與 MA/MACD/KD/RSI/BB 一致，無需後端變更，切換指標無延遲。

### 2. ADX 副圖表設計

ADX 圖表顯示三條線：
- **ADX**（趨勢強度）：實線
- **+DI**（正向方向指標）：實線
- **-DI**（負向方向指標）：實線
- **25 參考線**：虛線（ADX > 25 表示強趨勢）

Y 軸範圍 0-100，與 RSI/KD 同邏輯。高度 120px，與 RSI/KD 圖表一致。

### 3. 預設參數

`period = 14`（業界標準，與後端 talib 計算一致）。

## Risks / Trade-offs

- **計算精度**：前端 `technicalindicators` 與後端 `talib` 的 ADX 結果可能有微小差異（< 0.5%）。現有指標也有此問題，可接受。→ 不處理。
- **資料量不足**：ADX 需要約 28 天暖機（period × 2），短期圖表可能前段無值。→ 前段補 null，與其他指標處理方式一致。
