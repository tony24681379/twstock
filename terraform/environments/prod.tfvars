# Twstock 正式環境配置

# ⚠️ 修正專案 ID（原本錯誤寫成 "twstock-prod"）
project_id = "twstock-484714"
# ⚠️ 修正 region（實際資源在 asia-northeast1，不是 asia-east1）
region = "asia-northeast1"

# 資料庫與 Redis（需要在 terraform apply 時提供）
# database_url = "postgresql+asyncpg://user:pass@host/db"
# redis_url    = "redis://default:pass@host:port"

# API 配置
api_image_tag = "latest"
# ⚠️ 調整配置確保在免費額度內（Cloud Run 免費額度：180k vCPU-秒/月，360k GiB-秒/月）
api_cpu           = "1"     # 從 2 降為 1（避免超出免費額度）
api_memory        = "768Mi" # 降為 768Mi（512Mi 會 OOM，768Mi 可減少 23% 費用）
api_min_instances = 0       # 必須為 0（按需啟動，避免持續費用）
api_max_instances = 3       # 從 10 降為 3（限制並發，避免突增費用）

# CORS 設定（改為明確的域名列表以提升安全性）
allowed_origins = "https://twstock.changes.live,https://twstock-484714.web.app,http://localhost:5173,http://localhost:3000"

# 前端配置
# ⚠️ 關閉 CDN 避免費用（使用 Firebase Hosting，完全免費）
enable_frontend_cdn = false

# 🔐 身份驗證設定
# ⚠️ 請設定為 true 啟用身份驗證，並設定白名單 email
require_auth = true

# ⚠️ 請替換為你的 Google OAuth Client ID（從 Google Cloud Console 取得）
# 設定方法：https://console.cloud.google.com/apis/credentials
google_client_id = "647618039886-tu8om4vec6cma6kfnjen7v89fcquc2t6.apps.googleusercontent.com"

# ⚠️ 請設定允許訪問的 email 白名單（逗號分隔，無空格）
# 範例：allowed_emails = "user1@gmail.com,user2@gmail.com,user3@gmail.com"
allowed_emails = "yellow24681379@gmail.com,souffle1988@gmail.com,edwardytc@gmail.com"

# 資料保留策略（線上版保留 250 個交易日）
environment_type    = "production"
data_retention_days = 250
cleanup_vacuum      = true
