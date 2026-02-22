## Context

目前 `all.py` 的 `get_all_stock_parallel()` 依序處理 5 種資料更新，但只有日線資料有正確的智慧快取。其他資料類型的現狀：

| 資料類型 | 檢查位置 | 問題 |
|---------|---------|------|
| 日線 | `database.py:bulk_check_needs_update()` | ✅ 正常（DB date vs API date） |
| 月營收 | `database.py:bulk_check_monthly_revenue_needs_update()` | `db_month < current_month` 永遠 True |
| 集中度 | `all.py:324-335`（inline） | 不在 database.py，無 checked_at 記錄 |
| 基本資訊 | `stock.py:64`（`info_loaded` flag） | 一次性旗標，永不刷新 |

所有 save 方法（`save_stock_info`、`save_monthly_revenue`、EPS INSERT）都使用 `ON CONFLICT DO NOTHING`，即使呼叫了 API 也無法覆蓋舊資料。

## Goals / Non-Goals

**Goals:**
- 統一所有資料類型的快取判斷模式：check → fetch → save → mark_checked
- 修復月營收的永久更新陷阱（每次省 ~2700 次 API 呼叫）
- 將集中度檢查邏輯從 `all.py` 提取到 `database.py`
- 讓基本資訊/EPS/股利能定期刷新（7 天鮮度）
- `ON CONFLICT DO NOTHING` 改為 `DO UPDATE`，讓資料修正能正確覆蓋

**Non-Goals:**
- 不改變日線資料的現有機制（已正常運作）
- 不新增資料庫表（只在 `stock_update_tracker` 加欄位）
- 不改變 API 端點或爬蟲抓取邏輯
- 不改變 `all.py` 的整體步驟順序

## Decisions

### D1: 用 `*_checked_at` 時間戳取代月份比較

**選擇**：在 `stock_update_tracker` 新增 `revenue_checked_at`、`concentration_checked_at`、`info_checked_at` 三個 DateTime 欄位，作為「今天是否已檢查過」的唯一判斷依據。

**理由**：
- 月營收的根本問題是「用月份比較」但資料永遠滯後，無論怎麼修正月份邏輯都無法完美處理公布時間不確定性
- `checked_at >= today` 是最簡單且正確的判斷：每天最多檢查一次 API，不管 API 有沒有新資料
- 日線已經有類似機制（`last_checked_at`），統一使用同樣的 pattern

**替代方案（否決）**：
- 在 `stock_monthly_revenue` 表加 `last_checked_at` 欄位 → 散落在各表，不如集中在 tracker
- 用 `api_latest_date` 機制（像日線一樣先探測最新日期）→ 月營收沒有對應的 API 端點可探測

### D2: `ON CONFLICT DO NOTHING` 改為 `DO UPDATE`

**選擇**：`save_stock_info()`、EPS INSERT、`save_monthly_revenue()` 都改為 UPSERT。

**理由**：
- 月營收數字有時會修正（初估 vs 確定值），`DO NOTHING` 會永遠保留初估值
- EPS 同理，季報公布後可能有調整
- `DO UPDATE` 讓 `updated_at` 能正確反映最後一次有效寫入時間
- 效能差異可忽略（UPSERT vs INSERT 在 PostgreSQL 上幾乎相同）

**不改的**：`save_concentration_data()` 保持 `DO NOTHING`，因為集中度資料為週快照，不會被修正。

### D3: 基本資訊使用 7 天鮮度而非事件驅動

**選擇**：`info_checked_at` 超過 7 天就視為過期，重新從 API 抓取。

**理由**：
- EPS 每季公布（3/5/8/11 月），但公布日不固定
- 股利每年公布，時間也不確定
- 用固定 7 天間隔是最簡單且不浪費的方式（每週檢查一次 × ~2700 支 ≈ 可接受的 API 量）
- 基本資訊的鮮度檢查發生在 `stock.py:load_data()`（逐支股票），不在 `all.py` 的批次流程中

**替代方案（否決）**：
- 30 天間隔 → 可能錯過季度 EPS 公布
- 1 天間隔 → 不必要的 API 浪費（基本資訊不是每天都變）

### D4: 集中度的 bulk_check 使用 `checked_at + 7 天` 雙重判斷

**選擇**：新增 `bulk_check_concentration_needs_update()` 到 `database.py`，判斷邏輯為：
1. DB 無資料 → 需要更新
2. `concentration_checked_at >= today` → 不需要
3. DB 最新資料日期距今 > 7 天 → 需要更新

**理由**：保留現有的「7 天過期」邏輯（符合集中度為週資料的特性），但加上 `checked_at` 防止同日重複檢查。

### D5: `bulk_update_checked_at()` 作為通用方法

**選擇**：在 `database.py` 新增一個通用的批次 UPSERT 方法，接受 field name 參數。用白名單驗證 field name 防 SQL injection。

**理由**：三種資料類型的 checked_at 更新邏輯完全相同，抽成通用方法避免重複。

## Risks / Trade-offs

**[Risk] ALTER TABLE 在生產環境** → `ADD COLUMN IF NOT EXISTS` 是冪等操作且 nullable 欄位不鎖表，在 `init_database()` 中執行安全。Neon PostgreSQL 支持此語法。

**[Risk] `DO UPDATE` 可能覆蓋正確資料** → 月營收和 EPS 的更新值來自同一 API（Wantgoo），用最新 API 值覆蓋舊值是正確行為。不存在「不同來源資料衝突」的問題。

**[Risk] 7 天鮮度可能錯過急迫更新** → 基本資訊（資本額、股數）極少變動；EPS/股利按季/年變動。7 天間隔足夠。如有急需可用 `force_reload=True` 強制刷新。

**[Trade-off] `bulk_update_checked_at` 需要額外一次 SQL** → 每種資料類型更新完後多一次 batch UPSERT（~2700 行），約 50ms，相對於節省的 API 呼叫時間（2-3 分鐘）可忽略。
