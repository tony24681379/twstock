# 技術指標規格

## ADDED Requirements

### Requirement: 歷史數據載入
系統 SHALL 載入適當天數的歷史數據以支援技術指標計算。

#### Scenario: 載入 90 天歷史數據
- **WHEN** 系統需要計算技術指標
- **THEN** 載入最近 90 天的歷史數據（約 60-65 個交易日）
- **RATIONALE** 90 天足以計算所有技術指標（最長週期 MA60 需要 60 個交易日）並提供 50% 緩衝空間

#### Scenario: 交易日 vs 日曆日
- **WHEN** 使用 SQL `INTERVAL '90 days'` 查詢
- **THEN** 實際返回約 60-65 個交易日（扣除週末和假日）
- **RATIONALE** 日曆日包含非交易日，需考慮此差異以確保足夠數據

#### Scenario: 批次載入優化
- **WHEN** 需要載入多支股票的歷史數據
- **THEN** 使用單一 SQL 查詢批次載入所有股票
- **PERFORMANCE** 2700 支股票載入時間 < 1 秒

### Requirement: 基本技術指標計算
系統 SHALL 計算標準技術分析指標。

#### Scenario: 計算移動平均線
- **WHEN** 有足夠的歷史數據
- **THEN** 計算 MA5、MA10、MA20、MA60（5 日、10 日、20 日、60 日移動平均）
- **IMPLEMENTATION** 使用 pandas `rolling(window=N).mean()` 方法

#### Scenario: 計算 MACD (Moving Average Convergence Divergence)
- **WHEN** 計算 MACD 指標
- **THEN** 計算 DIF (12 日 EMA - 26 日 EMA)、MACD (9 日 DIF EMA)、OSC (DIF - MACD)
- **IMPLEMENTATION** 使用 pandas `ewm(span=N).mean()` 計算指數移動平均

#### Scenario: 計算 KD 隨機指標
- **WHEN** 計算 KD 指標
- **THEN** 計算 RSV = (收盤價 - N 日最低) / (N 日最高 - N 日最低) × 100
- **THEN** K = RSV 的 3 日移動平均，D = K 的 3 日移動平均
- **DEFAULT** N = 9（預設週期）

#### Scenario: 計算 RSI (Relative Strength Index)
- **WHEN** 計算 RSI 指標
- **THEN** 計算 N 日內上漲日平均漲幅 (U) 和下跌日平均跌幅 (D)
- **THEN** RSI = 100 - (100 / (1 + U/D))
- **DEFAULT** N = 6 或 12（常用週期）

#### Scenario: 計算布林通道 (Bollinger Bands)
- **WHEN** 計算布林通道
- **THEN** 中軌 = N 日移動平均，上軌 = 中軌 + k×標準差，下軌 = 中軌 - k×標準差
- **DEFAULT** N = 20，k = 2

#### Scenario: 計算 ADX/DMI
- **WHEN** 計算趨勢強度指標
- **THEN** 後端使用 talib 計算 ADX (timeperiod=14)、+DI (PLUS_DI)、-DI (MINUS_DI)
- **THEN** +DI/-DI SHALL 存在記憶體 DataFrame 中，供訊號偵測使用
- **THEN** 前端 SHALL 使用 `technicalindicators` npm 庫的 `ADX.calculate()` 即時計算
- **THEN** 前端輸入：high、low、close 陣列及 period（預設 14）
- **THEN** 前端輸出：adx（趨勢強度 0-100）、pdi（+DI）、mdi（-DI）
- **USAGE** ADX > 25 表示強趨勢，+DI > -DI 表示上漲趨勢

### Requirement: 線差指標計算（向量化實現）
系統 SHALL 使用向量化方式計算三線乖離和四線乖離指標。

#### Scenario: 計算三線乖離（向量化）
- **WHEN** 計算三線乖離 (three_line_diff)
- **THEN** 使用 numpy 向量化操作計算 (max - min) / mean of MA5, MA10, MA20
- **IMPLEMENTATION** `np.max()`, `np.min()`, `np.mean()` 一次處理整個陣列
- **PERFORMANCE** 單支股票計算時間 < 0.01ms（比 Python 迴圈快 10-15 倍）

#### Scenario: 計算四線乖離（向量化）
- **WHEN** 計算四線乖離 (four_line_diff)
- **THEN** 使用 numpy 向量化操作計算 (max - min) / mean of MA5, MA10, MA20, MA60
- **IMPLEMENTATION** 同樣使用 numpy 向量化函數
- **PERFORMANCE** 單支股票計算時間 < 0.01ms

#### Scenario: 處理 NaN 值
- **WHEN** 計算線差指標遇到 NaN 或除以零
- **THEN** 使用 `np.errstate(divide='ignore', invalid='ignore')` 處理異常
- **THEN** 使用 `np.where()` 將無效結果設為 NaN
- **RATIONALE** 避免警告訊息並確保數據正確性

#### Scenario: 向量化正確性驗證
- **WHEN** 部署向量化實現
- **THEN** 與原始 Python 迴圈版本的結果誤差 < 1e-10
- **TESTING** 使用 `test_vectorization_d1.py` 測試 20 支隨機股票

### Requirement: 技術訊號系統
系統 SHALL 計算標準化的技術買賣訊號。

#### Scenario: 計算買進訊號（8 個）
- **WHEN** 分析股票技術訊號
- **THEN** 計算以下買進訊號：
  1. **多頭排列** (+15分): MA5 > MA10 > MA20
  2. **三線合一向上** (+12分): 三線乖離 < 0.03 且均線上揚
  3. **四線合一向上** (+12分): 四線乖離 < 0.03 且均線上揚
  4. **價在均線上** (+10分): 收盤價 > MA5 > MA10 > MA20
  5. **均線糾結向上** (+8分): 均線收斂且趨勢向上
  6. **突破盤整** (+8分): 突破橫盤區間
  7. **黃金交叉** (+6分): 短期均線向上穿越長期均線
  8. **量價齊揚** (+5分): 價格上漲且成交量放大

#### Scenario: 計算賣出訊號（2 個）
- **WHEN** 分析股票技術訊號
- **THEN** 計算以下賣出訊號：
  1. **空頭排列** (-15分): MA5 < MA10 < MA20
  2. **出貨訊號** (-10分): 價跌量增或價漲量縮

#### Scenario: 計算 DMI 訊號（2 個）
- **WHEN** ADX 和 +DI/-DI 指標已計算完成
- **THEN** 計算以下 DMI 訊號：
  1. **DMI向上** (+10分, State): ADX > 25 且 +DI > -DI（強趨勢且方向向上）
  2. **DMI向下** (-10分, State): ADX > 25 且 -DI > +DI（強趨勢且方向向下）
- **THEN** DMI 訊號 SHALL 為 State 型，每天驗證條件有效性
- **THEN** `detect_all_signals()` 和 `validate_state_signals()` 兩處 SHALL 同時實作 DMI 偵測邏輯

#### Scenario: 訊號強度標準化
- **WHEN** 計算技術訊號總分
- **THEN** 原始分數範圍：-182 ~ +184（crossover -52~+74, state -78~+90, extreme -52~+20）
- **THEN** 標準化為 0-100 分：`(原始分數 + 182) / 366 * 100`
- **RATIONALE** 統一評分標準，方便前端顯示和排序

#### Scenario: 訊號品質評級
- **WHEN** 根據標準化分數評級
- **THEN** 80-100: 強力買進
- **THEN** 60-79: 買進
- **THEN** 40-59: 觀望
- **THEN** 0-39: 風險警示

### Requirement: 批次計算性能
系統 SHALL 在可接受的時間內完成批次技術指標計算。

#### Scenario: 批次分析 2700 支股票
- **WHEN** 執行完整市場掃描
- **THEN** 技術指標計算總時間 < 5 秒
- **THROUGHPUT** 平均 ~500 stocks/sec
- **RATIONALE** 確保每日更新可在合理時間內完成

#### Scenario: 單支股票分析性能
- **WHEN** 計算單支股票所有技術指標
- **THEN** 計算時間 < 5 ms
- **BREAKDOWN** 基本指標 ~3ms，線差指標 ~0.5ms，訊號評分 ~1ms

#### Scenario: 向量化性能提升
- **WHEN** 使用向量化 calc_line_diff()
- **THEN** 執行時間 < 0.01 ms per stock
- **IMPROVEMENT** 比原始 Python 迴圈版本快 10-15 倍
- **TESTING** 使用 `test_vectorization_d1.py` 測試，20 支股票平均 0.37ms

#### Scenario: 數據載入性能
- **WHEN** 批次載入 2700 支股票的 90 天歷史數據
- **THEN** SQL 查詢時間 < 1 秒
- **IMPLEMENTATION** 單一 SQL 查詢使用 `IN` 條件，避免 N+1 query 問題

### Requirement: 測試和驗證
系統 SHALL 提供測試腳本驗證技術指標計算正確性和性能。

#### Scenario: 向量化正確性測試
- **WHEN** 運行 `test_vectorization_d1.py`
- **THEN** 所有測試股票的三線乖離和四線乖離與原始實現誤差 < 1e-10
- **THEN** 測試成功率 = 100%（20/20 stocks passed）

#### Scenario: 性能基準測試
- **WHEN** 測試向量化性能
- **THEN** 平均執行時間 < 0.5 ms per stock
- **THEN** 預估 2700 支股票總時間 < 1.5 秒

#### Scenario: 數據完整性驗證
- **WHEN** 載入歷史數據後
- **THEN** 驗證有足夠的交易日數據（至少 60 個交易日）
- **THEN** 處理 `calc_base()` 的 `dropna(how="any")` 影響
- **RATIONALE** 確保 MA60 等長週期指標有足夠數據

### Requirement: 整合性能指標
系統 SHALL 提供端到端的性能基準。

#### Scenario: 完整系統首次運行
- **WHEN** 從空資料庫開始完整運行
- **THEN** 總時間約 10 分鐘
- **BREAKDOWN** API 更新 6-7 分鐘，籌碼集中度 2-3 分鐘，技術指標 1-2 分鐘

#### Scenario: 完整系統後續運行
- **WHEN** 資料庫已有快取，執行每日更新
- **THEN** 總時間約 2 分鐘
- **BREAKDOWN** API 更新 <10 秒（僅 30-50 支需更新），籌碼 <5 秒（DB 快取），技術指標 1-2 分鐘

#### Scenario: 優化影響評估
- **WHEN** 比較優化前後性能
- **THEN** 數據載入：490 天 → 90 天（減少 82% 載入量，5.4x 提升）
- **THEN** 線差計算：Python 迴圈 → numpy 向量化（10-15x 提升）
- **THEN** API 回應：2-3 秒 → 60-100 ms（詳情頁移除不必要欄位）

### Requirement: 前端 ADX 圖表顯示
前端 SHALL 在 K 線圖下方以獨立副圖顯示 ADX 指標。

#### Scenario: ADX 切換按鈕
- **WHEN** 使用者在指標切換列（IndicatorToggles）點擊 ADX 按鈕
- **THEN** 啟用或停用 ADX 指標顯示
- **THEN** 按鈕樣式 SHALL 與現有指標按鈕一致（啟用時顯示對應顏色）

#### Scenario: ADX 副圖表渲染
- **WHEN** ADX 指標已啟用且有足夠歷史數據
- **THEN** 在副圖區域顯示 ADX 圖表（Recharts LineChart）
- **THEN** 圖表高度 SHALL 為 120px（與 RSI、KD 圖表一致）
- **THEN** 圖表 SHALL 顯示以下線條：
  - ADX 線（趨勢強度）
  - +DI 線（正向方向指標）
  - -DI 線（負向方向指標）
  - ADX = 25 參考虛線（強趨勢分界）
- **THEN** Y 軸範圍 SHALL 為 0-100

#### Scenario: ADX 資料不足處理
- **WHEN** 歷史數據不足以計算 ADX（需約 28 個交易日暖機）
- **THEN** 不足期間的值 SHALL 為 null
- **THEN** 圖表 SHALL 僅顯示有值的區段（與其他指標行為一致）

#### Scenario: ADX Tooltip 顯示
- **WHEN** 使用者將滑鼠懸停在 ADX 圖表上
- **THEN** Tooltip SHALL 顯示 ADX、+DI、-DI 的數值，格式化至小數點第二位
- **THEN** 樣式 SHALL 與現有副圖 Tooltip 一致（支援 dark mode）

#### Scenario: ADX 圖表與下方區塊間距
- **WHEN** ADX 圖表（或任何副圖指標）顯示於頁面上
- **THEN** 副圖區塊與下方基本資訊卡片之間 SHALL 有 `mb-6` 間距
