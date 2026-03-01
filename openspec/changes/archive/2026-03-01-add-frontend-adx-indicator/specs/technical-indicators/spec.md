## MODIFIED Requirements

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
- **THEN** 前端 SHALL 使用 `technicalindicators` npm 庫的 `ADX.calculate()` 即時計算
- **THEN** 輸入：high、low、close 陣列及 period（預設 14）
- **THEN** 輸出：adx（趨勢強度 0-100）、pdi（+DI）、mdi（-DI）
- **USAGE** 判斷趨勢強度，ADX > 25 表示強趨勢

## ADDED Requirements

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
