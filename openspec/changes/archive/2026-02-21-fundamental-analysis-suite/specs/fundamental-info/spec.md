## ADDED Requirements

### Requirement: FundamentalInfo API 回應擴充
系統 SHALL 在 `GET /api/stocks/{stock_id}/fundamental` 回應中新增以下欄位：
- `psr: float | null` — PSR 股價營收比
- `eps_prediction: EPSPrediction | null` — EPS 預測
- `dividend_history: DividendDetail[]` — 歷史股利（最近 5 年）

#### Scenario: 完整資料
- **WHEN** 股票有營收、EPS、股利歷史資料
- **THEN** 回應包含 psr 數值、eps_prediction 物件、dividend_history 陣列

#### Scenario: 部分資料缺失
- **WHEN** 股票無營收資料
- **THEN** psr = null，eps_prediction = null，其餘欄位正常回傳

### Requirement: 正規化公式更新
系統 SHALL 在新增訊號後更新基本面分數正規化公式。新的理論分數範圍 MUST 涵蓋所有可能觸發的訊號分數總和，並在 `SIGNAL_DEFINITIONS.md` 中記錄。

#### Scenario: 公式涵蓋新訊號
- **WHEN** 新增營收訊號（+12, +10, +15, -12, -15）、股利訊號（+10, -12）、PSR 訊號（+8, -8）
- **THEN** 理論最大分數 = 93（原）+ 12 + 10 + 15 + 10 + 8 = 148，理論最小分數 = -73（原）+ (-12) + (-15) + (-12) + (-8) = -120
- **THEN** 正規化公式更新為 `normalized = ((raw_score + 120) / 268) * 100`

### Requirement: 基本面訊號包含所有新訊號
系統 SHALL 在 `fundamental_signals` 列表中包含所有觸發的訊號（含現有 10 個 + 新增的營收/股利/PSR 訊號）。

#### Scenario: 多種訊號同時觸發
- **WHEN** 某股票同時滿足「EPS連續成長」(+20)、「營收連續正成長」(+12)、「PSR偏低」(+8)
- **THEN** `fundamental_signals` 陣列包含 3 個 ChipSignal 物件，`fundamental_strength` 為 40 的正規化值
