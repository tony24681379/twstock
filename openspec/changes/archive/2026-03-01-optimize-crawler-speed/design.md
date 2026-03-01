## Context

`update_convertible_bonds()` 在 `main.py` Step 4 執行，此時 `all.get_all_stock_parallel()` 已完成，`all.db_manager.api_latest_date` 已被 HeaderManager 探測並設定（透過 fetcher_manager.py 傳播）。

但 Step 4 建立了一個新的 `DatabaseManager()` 實例（main.py:43），該實例沒有 `api_latest_date`。

現狀：`bulk_check_cb_needs_update()` 已存在但未被呼叫，且用 `db_date < today` 比較——假日/週末會誤判（today 不是交易日，但 DB 最新日期是上一個交易日，永遠 < today）。

## Goals / Non-Goals

**Goals:**
- CB 每日資料更新前，先用 `api_latest_date` 判斷哪些 CB 需要更新
- DB 已有最新交易日資料的 CB 直接跳過 API 呼叫
- 跳過的 CB 仍從 DB 載入資料用於訊號計算

**Non-Goals:**
- 不改變 CB 清單取得邏輯（Step 1 每次都拉，因清單會變動）
- 不改變標的股收盤價取得邏輯（Step 2 每次都需要最新價格）
- 不改變訊號計算邏輯

## Decisions

### 1. api_latest_date 傳遞方式：透過參數傳入

`update_convertible_bonds(db_manager, api_latest_date=None)` 新增可選參數。

main.py 呼叫時傳入 `all.db_manager.api_latest_date`。

**替代方案**：
- 共用 `all.db_manager` → 不行，main.py 的 db_manager 還用於 SignalUpdater，生命週期不同
- CB 自己探測 → 不必要，TPEX 和 TWSE 交易日一致，共用即可

### 2. bulk_check_cb_needs_update 改用 reference_date

```python
async def bulk_check_cb_needs_update(self, bond_ids, reference_date=None):
    compare_date = reference_date or date.today()
    # ...
    needs_update[bid] = db_date < compare_date
```

傳入 `api_latest_date` 時用它比較；未傳入時 fallback 到 `today`（向後相容）。

**替代方案**：
- 硬性要求必須傳 `api_latest_date` → 彈性不夠，單獨跑 CB 更新時可能沒有這個值

### 3. 跳過的 CB 如何取得 daily 資料用於訊號計算

現有流程 `fetch_all_cb_daily()` 回傳 `all_daily`，直接用於訊號計算。改為：
1. `bulk_check_cb_needs_update()` 篩出需要更新的 CB
2. 只對需要更新的 CB 呼叫 `fetch_all_cb_daily()`
3. 從 DB 載入所有 CB 的最新一筆 daily 資料（用於訊號計算）
4. API 回傳的資料覆蓋 DB 資料（確保用最新的）

使用已有的 `bulk_load_convertible_bond_daily()` 載入 DB 資料。

## Risks / Trade-offs

- **[假日 fallback]** → 若 `api_latest_date` 為 None（HeaderManager 探測失敗），fallback 到 `today` 比較，行為與現有一致（偏保守，會多抓一些）
- **[新上市 CB]** → 新 CB 在 DB 中沒有資料，`bulk_check_cb_needs_update()` 會回傳 `True`，自動走 API 路徑，無影響
- **[CB 清單仍每次拉取]** → 這是 1 次 HTTP GET（TPEX OpenAPI），耗時 <1 秒，不值得優化
