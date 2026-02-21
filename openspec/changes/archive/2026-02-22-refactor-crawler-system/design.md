## Context

WantgooFetcher（714 行）混合了 5 種職責：HTTP 請求、資料解析、資料庫持久化、Header 管理、API 日期追蹤。All 類（1577 行）有 3 處幾乎相同的 `Semaphore + as_completed + 進度顯示` 並發模式。ConvertibleBondFetcher 複製了相同的 DB 耦合模式。

現有架構：
```
FetcherManager (全域單例)
  └─ WantgooFetcher (HTTP + 解析 + 持久化 + Header + 日期追蹤)
       ├─ WantgooInitializer (Playwright header 獲取)
       └─ DatabaseManager (直接注入)

Stock → fetcher.fetch_daily(sid, save_to_db=True) → 資料 + 副作用寫 DB
All   → 3 處 Semaphore + as_completed + 進度顯示（API更新/月營收/集中度）
```

## Goals / Non-Goals

**Goals:**
- Fetcher 只負責「HTTP 請求 + 資料解析 + 回傳結果」，不觸碰 DB
- Header 生命週期獨立管理（初始化、快取、刷新、Fallback）
- 3 處重複的批量並發邏輯合併為一個通用元件
- 重試策略改為指數退避
- 所有變更對外部 API 端點透明（行為不變）

**Non-Goals:**
- 不重寫 DatabaseManager 本身（只調整調用方式）
- 不引入 DI 框架（如 injector），用簡單的建構式注入即可
- 不新增第三方重試庫（tenacity），自行實作即可控制
- 不改變 Excel 報表生成邏輯（屬於另一個重構範疇）
- 不改變資料庫 Schema

## Decisions

### D1: Fetcher 回傳純資料，持久化由調用層負責

**選擇**：Fetcher 方法回傳 `dict` / `pd.DataFrame`，移除所有 `save_to_db` 參數。

**替代方案**：
- (A) Repository 模式：引入 `StockRepository` 抽象層 → 過度設計，目前只有一種儲存目標
- (B) 事件回調：`on_data_fetched()` → 增加複雜度，追蹤困難
- **(C) 純回傳（選用）**：最簡單，調用端決定要不要存 → 明確、可測試

**影響**：
- `WantgooFetcher.__init__` 移除 `db_manager` 參數
- `fetch_info()`, `fetch_daily()`, `fetch_concentration_data()`, `fetch_monthly_revenue()` 移除 `save_to_db` 參數，只回傳資料
- `Stock.load_data()` 改為：`data = await fetcher.fetch_daily(...)` → `await db_manager.save_daily_data(sid, data)`
- `All` 類的批量更新同理：fetch 後由 All 自己呼叫 db_manager 存資料

### D2: 提取 HeaderManager 類

**選擇**：建立 `HeaderManager` 類，封裝 Header 的完整生命週期。

```python
class HeaderManager:
    def __init__(self, initializer: WantgooInitializer = None):
        self._headers = DEFAULT_HEADERS.copy()
        self._initializer = initializer
        self._initialized = False
        self._lock = asyncio.Lock()
        self._last_refresh: float = 0

    async def get_headers(self) -> dict:
        """取得有效的 headers，必要時自動初始化"""

    async def refresh(self) -> bool:
        """刷新 headers（401/403 時呼叫）"""

    @property
    def api_latest_date(self) -> date | None:
        """初始化時順便探測的 API 最新日期"""
```

**替代方案**：
- (A) 保持在 WantgooFetcher 內 → 職責混雜，難以測試
- **(B) 獨立類（選用）**→ 可單獨測試、可注入 Mock

**鎖的處理**：目前 `_refresh_lock` 是類級別共享鎖。改為 HeaderManager 實例級別鎖，由 FetcherManager 確保全域只有一個 HeaderManager 實例。

### D3: 統一 BatchExecutor

**選擇**：提取通用 `batch_execute` async 函式。

```python
async def batch_execute(
    items: list[str],
    worker: Callable[[str], Awaitable[tuple[str, Any]]],
    max_workers: int = 10,
    label: str = "處理",
    progress_interval: int = 50,
) -> dict[str, Any]:
    """
    通用批量並發執行器

    Returns:
        {item_id: result} 字典，失敗的項目不包含在結果中
    """
```

**替代方案**：
- (A) 建立 `BatchExecutor` 類 → 沒有必要維護狀態，函式就夠了
- **(B) 通用函式（選用）**→ 簡單、直接、無狀態

**統一的 3 處呼叫**：
1. `All` 的 API 更新（第 216-260 行）
2. `All` 的月營收更新（第 296-353 行）
3. `All` 的集中度更新（第 421-467 行）

每處改為一行呼叫 + 一個 worker 函式定義。

### D4: 指數退避重試

**選擇**：在 `fetch_url()` 中將固定 1 秒等待改為指數退避。

```python
delay = min(base_delay * (2 ** retry_i) + random.uniform(0, 0.5), max_delay)
# base_delay=0.5, max_delay=10
# 第 1 次: ~0.5-1s, 第 2 次: ~1-1.5s, 第 3 次: ~2-2.5s, 第 4 次: ~4-4.5s
```

**替代方案**：
- (A) `tenacity` 庫 → 多一個依賴，此場景太簡單不值得
- **(B) 自行實作（選用）**→ 3 行程式碼，無額外依賴

### D5: 模組結構

**選擇**：在 `twstock/` 下新增模組，不建立子目錄。

```
twstock/
├─ wantgoo.py              # 重構：只保留 HTTP 請求 + 資料解析（移除 DB 邏輯）
├─ wantgoo_initializer.py  # 不變：Playwright header 獲取
├─ header_manager.py       # 新增：Header 生命週期管理
├─ batch_executor.py       # 新增：通用批量並發執行
├─ fetcher_manager.py      # 重構：管理 HeaderManager + Fetcher + DB 初始化
├─ stock.py                # 調整：分開呼叫 fetch + save
├─ all.py                  # 調整：使用 batch_execute + 分開 fetch/save
├─ convertible_fetcher.py  # 調整：移除 db_manager 耦合
└─ database.py             # 不變
```

**替代方案**：
- (A) 建立 `twstock/fetchers/` 子套件 → 移動太多檔案，增加 import 路徑複雜度
- **(B) 扁平結構（選用）**→ 只新增 2 個檔案，改動最小

### D6: FetcherManager 簡化

```python
# 重構後的初始化流程
async def get_global_fetcher() -> WantgooFetcher:
    header_manager = HeaderManager(initializer=WantgooInitializer)
    fetcher = WantgooFetcher(header_manager=header_manager)
    return fetcher

async def get_db_manager() -> DatabaseManager:
    # 獨立管理，不再綁定在 fetcher 上
```

Stock 和 All 類分別注入 fetcher 和 db_manager，不再透過 `fetcher.db_manager` 間接取得。

## Risks / Trade-offs

**[調用端程式碼量增加]** → 每個 fetch 呼叫後需要多一行 save 呼叫。但這使資料流向明確，值得。

**[All 類仍然很大]** → 本次只處理並發邏輯重複。Excel 生成和訊號計算的分離是獨立的重構範疇。

**[向後相容]** → `save_to_db` 參數移除是 **breaking change**（內部 API）。但此參數只在專案內部使用，無外部消費者。一次性替換所有呼叫點即可。

**[HeaderManager 與 WantgooInitializer 的邊界]** → HeaderManager 負責「何時」初始化/刷新，WantgooInitializer 負責「如何」透過 Playwright 取得 headers。兩者職責清楚分離。

**[api_latest_date 的歸屬]** → 移入 HeaderManager，因為它是在初始化 header 時順便探測的。db_manager 不再從 fetcher 取得此值，改由 FetcherManager 初始化時統一設定。
