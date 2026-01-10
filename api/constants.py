"""
API 常量定義
集中管理所有硬編碼的配置值和閾值
"""

# ============================================================
# 風險等級閾值
# ============================================================


class RiskLevel:
    """風險等級定義"""

    HIGH_THRESHOLD = 70  # 訊號強度 >= 70 為低風險
    MEDIUM_THRESHOLD = 40  # 訊號強度 >= 40 為中風險

    # 風險等級標籤
    LOW = "低"
    MEDIUM = "中"
    HIGH = "高"


# ============================================================
# 訊號計算常量
# ============================================================


class SignalConstants:
    """訊號計算相關常量"""

    # 預期報酬率計算係數
    EXPECTED_RETURN_MULTIPLIER = 0.13

    # 歷史勝率基準值
    DEFAULT_WIN_RATE_BASE = 50.0

    # 訊號分數正規化範圍
    MIN_SCORE = 0
    MAX_SCORE = 100

    # 訊號計算所需最小週數
    MIN_WEEKS_FOR_SIGNALS = 10

    # 訊號觸發閾值
    LARGE_HOLDERS_RAPID_BUY_THRESHOLD = 1.0  # 大戶急買閾值（%）
    RETAIL_PANIC_THRESHOLD = 1.0  # 散戶恐慌閾值（%）
    ACCUMULATION_10W_THRESHOLD = 3.0  # 10週持續買閾值（%）


# ============================================================
# 驗證規則
# ============================================================


class ValidationRules:
    """輸入驗證規則"""

    # 股票代碼
    STOCK_ID_MIN_LENGTH = 3
    STOCK_ID_MAX_LENGTH = 6

    # 週期選項
    VALID_PERIODS = {"1M", "3M", "6M", "1Y"}

    # 歷史資料範圍
    MAX_WEEKS = 52
    MIN_WEEKS = 1
    MAX_DAYS = 90
    MIN_DAYS = 1

    # 排序欄位
    VALID_SORT_FIELDS = {
        "stock_id",
        "signal_strength",
        "expected_return",
        "win_rate",
        "signal_count",
    }

    # 排序方向
    VALID_SORT_ORDERS = {"asc", "desc"}


# ============================================================
# HTTP 快取配置
# ============================================================


class CacheDurations:
    """HTTP Cache-Control 快取時間（秒）"""

    STOCK_LIST = 3600  # 1 小時
    CHART_DATA = 7200  # 2 小時
    STOCK_DETAIL = 3600  # 1 小時
    STOCK_HISTORY = 86400  # 24 小時
    CHIPS_DATA = 3600  # 1 小時


# ============================================================
# 資料庫查詢配置
# ============================================================


class QueryConfig:
    """資料庫查詢配置"""

    # 批次查詢大小
    BATCH_SIZE = 1000

    # 籌碼資料預設週數
    DEFAULT_CONCENTRATION_WEEKS = 12

    # 三大法人資料預設天數
    DEFAULT_CHIPS_DAYS = 30


# ============================================================
# 業務邏輯常量
# ============================================================


class BusinessConstants:
    """業務邏輯常量"""

    # 指數代碼過濾規則
    INDEX_PREFIXES = ["^", "0000", "000-"]

    # 預設市場
    DEFAULT_MARKET = "TWSE"
