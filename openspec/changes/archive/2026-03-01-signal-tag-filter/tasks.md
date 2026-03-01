## 1. Signal Catalog

- [x] 1.1 建立 `dashboard/src/constants/signalCatalog.ts`，定義 6 個分類共 67 個訊號，每個訊號含 `name`、`score`，每個分類含 `key`、`label`、`signals`
- [x] 1.2 訊號名稱與 `docs/SIGNAL_DEFINITIONS.md` 及後端程式碼交叉比對，確保完全一致

## 2. SignalFilter Component

- [x] 2.1 建立 `dashboard/src/components/SignalFilter.tsx`，props: `activeSignals`、`onSignalsChange`、`stocks`
- [x] 2.2 實作可展開/收合面板（預設收合），收合時顯示已選數量 badge
- [x] 2.3 實作 6 個分類區域，每類顯示 label + flex-wrap 標籤按鈕
- [x] 2.4 每個標籤顯示訊號名稱 + 觸發股票數（useMemo 計算），0 觸發的標籤灰色 disabled
- [x] 2.5 標籤顏色：正分綠色系、負分紅色系；未選 outline、已選 filled
- [x] 2.6 已選標籤列（頂部）：顯示已選訊號 chip + × 移除按鈕 + 「清除全部」按鈕
- [x] 2.7 支援 dark mode（所有元素使用 `dark:` Tailwind 前綴）

## 3. HomePage Integration

- [x] 3.1 在 `HomePage.tsx` 新增 `activeSignals` state 和 `filteredStocks` useMemo
- [x] 3.2 filteredStocks 實作 AND 邏輯：從 5 個訊號陣列收集 name 成 Set，檢查 `activeSignals.every()`
- [x] 3.3 在 WeightController 下方加入 `<SignalFilter>` 組件
- [x] 3.4 表格 `stocks.map()` 改為 `filteredStocks.map()`
- [x] 3.5 更新計數文字：有篩選時「符合條件: N / total 支股票」，無篩選時維持原樣

## 4. Verification

- [ ] 4.1 啟動前端 `npm run dev`，確認篩選面板出現、收合/展開正常
- [ ] 4.2 點選「多頭排列」→ 表格只顯示有該訊號的股票
- [ ] 4.3 疊加選「大戶急買」→ AND 篩選，結果數量減少
- [ ] 4.4 選擇互斥訊號（「多頭排列」+「空頭排列」）→ 0 筆結果
- [ ] 4.5 清除全部 → 恢復顯示所有股票
- [ ] 4.6 確認 dark mode 下所有元素可讀性正常
