## Why

`SignalScore` 已定義 `DMI_UP`(+10) / `DMI_DOWN`(-10) 但標記為 TODO 未實作（`vectorized_signals.py:310-312`）。後端 `vectorized_indicators.py` 已計算 ADX 值，但缺少 +DI/-DI 指標，導致無法偵測 DMI 訊號。27 個技術訊號中有 2 個長期缺位。

## What Changes

- `vectorized_indicators.py` 新增 +DI/-DI 計算（使用 talib `PLUS_DI` / `MINUS_DI`）
- `vectorized_signals.py` 實作 DMI向上/DMI向下 訊號偵測邏輯
- `SIGNAL_TYPE_MAP` 新增 DMI 訊號映射
- 正規化公式更新（State 範圍 -78~+90，總範圍 -182~+184）
- 同步更新 `signal_updater.py`、`stock_service.py` 中的正規化公式

## Capabilities

### New Capabilities

（無）

### Modified Capabilities

- `technical-indicators`: 新增 +DI/-DI 指標計算、DMI 訊號偵測邏輯、正規化公式更新

## Impact

- **後端計算**：`twstock/vectorized_indicators.py`、`twstock/vectorized_signals.py`
- **正規化公式**：`twstock/vectorized_signals.py`、`twstock/signal_updater.py`、`api/services/stock_service.py`
- **文件**：`docs/SIGNAL_DEFINITIONS.md`
- **資料庫**：`stock_technical_indicators` 表已有 `adx` 欄位，需新增 `plus_di`、`minus_di` 欄位
