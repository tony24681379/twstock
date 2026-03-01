## Why

前端 K 線圖表已支援 MA、MACD、KD、RSI、BB 五種技術指標，但缺少 ADX（Average Directional Index）趨勢強度指標。後端已有 ADX 計算與資料庫欄位，但前端完全未整合顯示。ADX 是判斷趨勢強弱的核心指標（ADX > 25 = 強趨勢），對使用者的交易決策有直接幫助。

## What Changes

- 前端 `indicators.ts` 新增 ADX 計算函式（使用 `technicalindicators` npm 庫，與現有指標一致）
- `IndicatorToggles.tsx` 新增 ADX 切換按鈕
- `IndicatorCharts.tsx` 新增 ADX 副圖表（含 ADX 線、+DI、-DI，以及 25 參考線）
- `chartConfig.ts` 新增 ADX 顏色配置
- `StockDetailPage.tsx` 整合 ADX 指標流程（calculateAll → 渲染）

## Capabilities

### New Capabilities

（無新增 capability）

### Modified Capabilities

- `technical-indicators`: 新增前端 ADX/DMI 計算與圖表顯示（spec 已定義 ADX 規格但前端未實現）

## Impact

- **前端程式碼**：`dashboard/src/` 下 5 個檔案需修改
- **後端**：無需變更（OHLCV 資料已足夠前端計算 ADX）
- **依賴**：`technicalindicators` npm 庫已內建 ADX 支援，無需新增依賴
- **相容性**：純前端新增，不影響現有功能
