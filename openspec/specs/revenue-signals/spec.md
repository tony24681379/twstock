## ADDED Requirements

### Requirement: 營收連續正成長訊號
系統 SHALL 在最近 3 個月營收年增率 (yoy_change) 皆 > 0% 時觸發「營收連續正成長」訊號，分數 +12。

#### Scenario: 連續 3 個月年增率為正
- **WHEN** 最近 3 個月的 yoy_change 分別為 +5.2%, +3.1%, +8.7%
- **THEN** 觸發「營收連續正成長」訊號，score = +12

#### Scenario: 其中一個月年增率為負
- **WHEN** 最近 3 個月的 yoy_change 分別為 +5.2%, -1.0%, +8.7%
- **THEN** 不觸發此訊號

### Requirement: 營收加速成長訊號
系統 SHALL 在最近 3 個月平均 yoy_change > 前 3 個月平均 yoy_change，且最近 3 個月平均 yoy_change > 10% 時觸發「營收加速成長」訊號，分數 +10。

#### Scenario: 營收年增率加速
- **WHEN** 最近 3 個月平均 yoy_change = 15%，前 3 個月平均 yoy_change = 8%
- **THEN** 觸發「營收加速成長」訊號，score = +10

#### Scenario: 營收年增率高但減速
- **WHEN** 最近 3 個月平均 yoy_change = 12%，前 3 個月平均 yoy_change = 18%
- **THEN** 不觸發此訊號（年增率雖高但在減速）

### Requirement: 營收由衰轉增訊號
系統 SHALL 在最近 1 個月 yoy_change > 0% 且前 2 個月 yoy_change 皆 < 0% 時觸發「營收由衰轉增」訊號，分數 +15。

#### Scenario: 營收從衰退轉為成長
- **WHEN** 最近 3 個月 yoy_change 分別為 +3.5%, -2.1%, -4.0%（最新在前）
- **THEN** 觸發「營收由衰轉增」訊號，score = +15

#### Scenario: 連續兩個月正成長
- **WHEN** 最近 3 個月 yoy_change 分別為 +3.5%, +1.2%, -4.0%
- **THEN** 不觸發此訊號（前月已正成長，非「由衰轉增」）

### Requirement: 營收連續衰退訊號
系統 SHALL 在最近 3 個月營收年增率 (yoy_change) 皆 < 0% 時觸發「營收連續衰退」訊號，分數 -12。

#### Scenario: 連續 3 個月年增率為負
- **WHEN** 最近 3 個月的 yoy_change 分別為 -3.2%, -5.1%, -1.8%
- **THEN** 觸發「營收連續衰退」訊號，score = -12

### Requirement: 營收急凍訊號
系統 SHALL 在最近 1 個月 yoy_change < -20% 時觸發「營收急凍」訊號，分數 -15。

#### Scenario: 單月營收年增率大幅衰退
- **WHEN** 最近 1 個月 yoy_change = -25.3%
- **THEN** 觸發「營收急凍」訊號，score = -15

#### Scenario: 衰退但未達門檻
- **WHEN** 最近 1 個月 yoy_change = -15.0%
- **THEN** 不觸發此訊號

### Requirement: 營收訊號需要足夠資料
系統 SHALL 在營收資料不足（少於 6 個月有效 yoy_change）時跳過所有營收訊號，不回傳錯誤。

#### Scenario: 營收資料不足
- **WHEN** 只有 4 個月的營收資料
- **THEN** 所有營收訊號皆不觸發，不產生錯誤
