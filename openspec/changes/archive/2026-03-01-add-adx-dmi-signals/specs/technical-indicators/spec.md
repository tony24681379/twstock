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
- **THEN** 使用 talib 計算 ADX (Average Directional Index, timeperiod=14)
- **THEN** 使用 talib 計算 +DI (PLUS_DI, timeperiod=14) 和 -DI (MINUS_DI, timeperiod=14)
- **THEN** +DI/-DI SHALL 存在記憶體 DataFrame 中，供訊號偵測使用
- **USAGE** ADX > 25 表示強趨勢，+DI > -DI 表示上漲趨勢

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
- **RATIONALE** 新增 DMI 訊號後 State 範圍擴大，需同步更新正規化公式

#### Scenario: 訊號品質評級
- **WHEN** 根據標準化分數評級
- **THEN** 80-100: 強力買進
- **THEN** 60-79: 買進
- **THEN** 40-59: 觀望
- **THEN** 0-39: 風險警示
