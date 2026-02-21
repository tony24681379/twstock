## ADDED Requirements

### Requirement: 通用批量並發執行
`batch_execute` SHALL 接受一組 item ID 和一個 async worker 函式，以限定的並發數執行所有任務，回傳結果字典。

#### Scenario: 正常批量執行
- **WHEN** 呼叫 `batch_execute(items=["A","B","C"], worker=fn, max_workers=2)`
- **THEN** 最多同時有 2 個 worker 在執行，最終回傳 `{"A": result_a, "B": result_b, "C": result_c}`

#### Scenario: 空列表輸入
- **WHEN** 呼叫 `batch_execute(items=[], worker=fn)`
- **THEN** 立即回傳空字典 `{}`，不執行任何 worker

### Requirement: Semaphore 並發控制
batch_execute SHALL 使用 `asyncio.Semaphore(max_workers)` 控制同時執行的 worker 數量，`max_workers` 預設為 10。

#### Scenario: 並發數限制
- **WHEN** items 有 100 個，`max_workers=5`
- **THEN** 任何時間點最多有 5 個 worker 同時在執行

### Requirement: 進度回報
batch_execute SHALL 在執行過程中定期輸出進度資訊，包含：已完成數/總數、成功數、失敗數、速度（支/秒）、預估剩餘時間。

#### Scenario: 預設進度間隔
- **WHEN** `progress_interval=50` 且已完成 50 個 item
- **THEN** 輸出一行進度訊息，格式包含完成數、速度、剩餘時間

#### Scenario: 自訂標籤
- **WHEN** 呼叫時指定 `label="月營收更新"`
- **THEN** 進度訊息中包含 "月營收更新" 字樣

### Requirement: 錯誤隔離
單個 worker 的失敗 SHALL NOT 影響其他 worker 的執行。失敗的 item 不包含在結果字典中，但會計入失敗計數。

#### Scenario: 部分 worker 失敗
- **WHEN** 3 個 item 中有 1 個 worker 拋出異常
- **THEN** 其餘 2 個 worker 正常完成，結果字典包含 2 個成功項目，失敗計數為 1

#### Scenario: worker 拋出異常
- **WHEN** worker 函式拋出 `Exception`
- **THEN** 記錄錯誤訊息（包含 item ID 和異常內容），繼續處理下一個 item

### Requirement: 執行摘要
batch_execute 完成後 SHALL 輸出摘要，包含：成功數、失敗數、總耗時。

#### Scenario: 全部成功
- **WHEN** 100 個 item 全部成功
- **THEN** 輸出摘要包含 `成功: 100 失敗: 0` 及耗時

#### Scenario: 有失敗項目
- **WHEN** 100 個 item 中 3 個失敗
- **THEN** 輸出摘要包含 `成功: 97 失敗: 3` 及耗時

### Requirement: Worker 函式簽名
worker 參數 SHALL 為 `Callable[[str], Awaitable[tuple[str, Any]]]`，接受一個 item ID（str），回傳 `(item_id, result)` 元組。

#### Scenario: Worker 回傳格式
- **WHEN** worker 處理 item "2330"
- **THEN** worker 回傳 `("2330", data_frame)` 格式的元組
