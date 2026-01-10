"""API 設定檔"""

import os

# API 設定
API_TITLE = "TW Stock Chip Dashboard API"
API_VERSION = "1.0.0"
API_DESCRIPTION = "台灣股市籌碼集中度儀表板 API"

# 資料庫設定
DATABASE_URL = os.getenv(
    "DATABASE_URL",
    "postgresql+asyncpg://twstock_user:twstock_password123@localhost:5432/twstock",
)

# CORS 設定
ALLOWED_ORIGINS = os.getenv(
    "ALLOWED_ORIGINS", "http://localhost:5173,http://localhost:3000"
).split(",")

# 身份驗證設定（可選功能）
REQUIRE_AUTH = os.getenv("REQUIRE_AUTH", "false").lower() == "true"
ALLOWED_EMAILS = (
    os.getenv("ALLOWED_EMAILS", "").split(",") if os.getenv("ALLOWED_EMAILS") else []
)
GOOGLE_CLIENT_ID = os.getenv("GOOGLE_CLIENT_ID", "")

# 分頁設定
DEFAULT_PAGE_SIZE = 100
MAX_PAGE_SIZE = 10000  # 允許一次載入所有股票（約 2700 支）
