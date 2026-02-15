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
- **THEN** 計算 +DI、-DI、ADX (Average Directional Index)
- **USAGE** 判斷趨勢強度，ADX > 25 表示強趨勢

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

#### Scenario: 訊號強度標準化
- **WHEN** 計算技術訊號總分
- **THEN** 原始分數範圍：-30 ~ +60
- **THEN** 標準化為 0-100 分：`(原始分數 + 30) / 90 * 100`
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
