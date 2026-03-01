## 1. indicators.ts — ADX 計算

- [x] 1.1 從 `technicalindicators` import `ADX`，新增 `calculateADX(highs, lows, closes, period=14)` 方法，回傳 `{ adx, pdi, mdi }`（各為 `(number | null)[]`）
- [x] 1.2 `CalculatedIndicators` 介面新增 `ADX?: { adx, pdi, mdi }` 型別
- [x] 1.3 `calculateAll()` 新增 `ADX` case，傳入 highs/lows/closes

## 2. IndicatorToggles.tsx — ADX 按鈕

- [x] 2.1 indicators 陣列新增 `{ value: 'ADX', label: 'ADX', color: 'bg-teal-500' }`

## 3. IndicatorCharts.tsx — ADX 副圖

- [x] 3.1 props 介面新增 `ADX?: { adx, pdi, mdi }` 型別
- [x] 3.2 chartData 映射新增 adx/pdi/mdi 欄位
- [x] 3.3 新增 ADX 圖表區塊：LineChart，三條線（ADX/+DI/-DI）+ ADX=25 參考虛線，高度 120px，Y 軸 0-100

## 4. chartConfig.ts — ADX 顏色配置

- [x] 4.1 `INDICATOR_CONFIG` 新增 ADX 相關顏色設定
