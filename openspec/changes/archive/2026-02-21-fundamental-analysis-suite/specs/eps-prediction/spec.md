## ADDED Requirements

### Requirement: 基礎 EPS 預測計算
系統 SHALL 基於歷史 EPS 季節性模式與營收年增率計算下一季 EPS 預測值。公式：`predicted_eps = 去年同季 EPS × (1 + 近 3 月平均營收年增率 / 100)`。若去年同季 EPS 不存在，fallback 為最近 4 季平均 EPS。

#### Scenario: 有去年同季 EPS 且有營收資料
- **WHEN** 下一季為 Q2，去年 Q2 EPS = 2.0，近 3 月平均營收 yoy_change = +15%
- **THEN** predicted_eps = 2.0 × 1.15 = 2.30

#### Scenario: 無去年同季 EPS，使用 fallback
- **WHEN** 下一季為 Q2，去年 Q2 EPS 不存在，最近 4 季平均 EPS = 1.8，近 3 月平均營收 yoy_change = +10%
- **THEN** predicted_eps = 1.8 × 1.10 = 1.98

#### Scenario: 無營收資料
- **WHEN** 去年 Q2 EPS = 2.0，但無營收資料（revenue_data 為空）
- **THEN** predicted_eps = 2.0（不做營收調整）

#### Scenario: EPS 與營收皆不足
- **WHEN** 歷史 EPS 少於 2 季
- **THEN** predicted_eps = None（無法預測）

### Requirement: 預測值標示
系統 SHALL 在 API 回應中明確標示 EPS 預測值為推估值（`is_prediction: true`），包含預測目標季度（`prediction_quarter`）和計算方法說明（`prediction_method`）。

#### Scenario: 正常預測
- **WHEN** 系統成功計算 EPS 預測
- **THEN** API 回傳 `eps_prediction: { value: 2.30, target_year: 2026, target_quarter: 2, method: "seasonal_revenue_adjusted", is_prediction: true }`

#### Scenario: 無法預測
- **WHEN** 資料不足無法預測
- **THEN** API 回傳 `eps_prediction: null`

### Requirement: EPS 預測不參與訊號評分
EPS 預測值 SHALL NOT 參與 `FundamentalSignalService` 的訊號計算。預測值僅供前端展示參考。

#### Scenario: 預測值不影響訊號
- **WHEN** 預測 EPS 為正值，但最近實際 EPS 為負
- **THEN** 訊號仍基於實際 EPS 計算（可能觸發虧損訊號），不受預測值影響

### Requirement: 前端展示 EPS 預測
系統 SHALL 在前端「獲利能力」tab 的 EPS 趨勢圖中以虛線顯示預測 EPS，並標註「預測」標籤。

#### Scenario: 有預測值
- **WHEN** `eps_prediction` 不為 null
- **THEN** 在 EPS 趨勢圖最右側以虛線延伸顯示預測值，帶「預測」標籤

#### Scenario: 無預測值
- **WHEN** `eps_prediction` 為 null
- **THEN** EPS 趨勢圖僅顯示歷史實際值，不做任何改變
