## ADDED Requirements

### Requirement: Signal catalog constant
系統 SHALL 提供一個靜態訊號目錄（`signalCatalog.ts`），包含所有 67 個訊號的名稱、分數、分類資訊，分為 6 個分類：籌碼（10）、技術狀態（11）、技術交叉（10）、極端事件（8）、基本面（18）、可轉債（10）。

#### Scenario: Catalog covers all signal categories
- **WHEN** 開發者引入 signalCatalog
- **THEN** 目錄 MUST 包含 6 個分類，每個分類有 `key`、`label`、`signals` 陣列
- **THEN** 所有訊號名稱 MUST 與後端 `ChipSignal.name` 完全一致

#### Scenario: Each signal has score polarity
- **WHEN** 開發者查詢目錄中的訊號
- **THEN** 每個訊號 MUST 包含 `score` 欄位，正數代表買入訊號、負數代表風險訊號

### Requirement: Signal filter panel UI
系統 SHALL 在總表頁面的權重控制器下方、表格上方提供一個可展開的訊號篩選面板。

#### Scenario: Panel default collapsed
- **WHEN** 使用者首次載入總表頁面
- **THEN** 篩選面板 MUST 預設收合
- **THEN** 面板標題 MUST 顯示「訊號篩選」

#### Scenario: Panel shows active count when collapsed
- **WHEN** 使用者已選擇 N 個訊號且面板收合
- **THEN** 面板標題旁 MUST 顯示已選數量 badge（如「3」）

#### Scenario: Panel expanded shows categorized tags
- **WHEN** 使用者展開篩選面板
- **THEN** 面板 MUST 顯示 6 個分類區域，每個分類有分類名稱和訊號標籤
- **THEN** 每個標籤 MUST 顯示訊號名稱和觸發該訊號的股票數量（如「多頭排列 (142)」）

#### Scenario: Tag with zero matches shown as disabled
- **WHEN** 某訊號目前沒有任何股票觸發
- **THEN** 該標籤 MUST 顯示為灰色 disabled 狀態，數量顯示為 0

### Requirement: Tag selection toggle
使用者 SHALL 能透過點擊標籤來切換該訊號的選擇狀態。

#### Scenario: Select a signal tag
- **WHEN** 使用者點擊一個未選中的訊號標籤
- **THEN** 該標籤 MUST 變為已選中狀態（filled 樣式）
- **THEN** 該訊號名稱 MUST 加入 activeSignals 篩選列表

#### Scenario: Deselect a signal tag
- **WHEN** 使用者點擊一個已選中的訊號標籤
- **THEN** 該標籤 MUST 恢復為未選中狀態（outline 樣式）
- **THEN** 該訊號名稱 MUST 從 activeSignals 篩選列表移除

#### Scenario: Active filter bar shows selected tags
- **WHEN** 有 1 個以上訊號被選中
- **THEN** 面板頂部 MUST 顯示已選標籤列，每個標籤有 × 按鈕可移除
- **THEN** MUST 顯示「清除全部」按鈕

### Requirement: AND logic stock filtering
系統 SHALL 使用 AND 邏輯篩選股票：只顯示同時觸發所有已選訊號的股票。

#### Scenario: Single signal filter
- **WHEN** 使用者選擇「多頭排列」
- **THEN** 表格 MUST 只顯示 `technical_signals` 包含 name="多頭排列" 的股票

#### Scenario: Multiple signal AND filter
- **WHEN** 使用者選擇「多頭排列」和「大戶急買」
- **THEN** 表格 MUST 只顯示同時觸發這兩個訊號的股票（跨分類 AND）

#### Scenario: Contradictory signals result in zero matches
- **WHEN** 使用者選擇「多頭排列」和「空頭排列」
- **THEN** 表格 MUST 顯示 0 筆結果（這兩個訊號不可能同時觸發）

#### Scenario: No filter selected shows all stocks
- **WHEN** activeSignals 為空
- **THEN** 表格 MUST 顯示所有股票（不套用篩選）

#### Scenario: Signal search spans all signal arrays
- **WHEN** 使用者選擇任一訊號
- **THEN** 系統 MUST 在股票的 `chip_signals`、`technical_signals`、`fundamental_signals`、`recent_events`、`cb_signals` 五個陣列中搜尋該訊號名稱

### Requirement: Filter result count display
系統 SHALL 在有篩選條件時顯示篩選結果計數。

#### Scenario: Active filter shows match count
- **WHEN** 有篩選條件且符合 N 支股票
- **THEN** 頁面 MUST 顯示「符合條件: N / {total} 支股票」

#### Scenario: No filter shows total count
- **WHEN** 沒有篩選條件
- **THEN** 頁面 MUST 顯示原有的「共 {total} 支股票（已載入全部）」

### Requirement: Tag color scheme
訊號標籤 SHALL 根據訊號分數正負使用不同顏色。

#### Scenario: Positive score signal tag
- **WHEN** 訊號分數為正數（買入訊號）
- **THEN** 標籤 MUST 使用綠色系樣式

#### Scenario: Negative score signal tag
- **WHEN** 訊號分數為負數（風險訊號）
- **THEN** 標籤 MUST 使用紅色系樣式

### Requirement: Dark mode support
篩選面板 SHALL 支援 dark mode。

#### Scenario: Dark mode rendering
- **WHEN** 系統處於 dark mode
- **THEN** 面板背景、文字、標籤 MUST 使用 `dark:` Tailwind 前綴確保可讀性
