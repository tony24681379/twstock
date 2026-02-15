# Twstock 開發環境配置

project_id = "twstock-dev"
region     = "asia-east1"

# 資料庫與 Redis（需要在 terraform apply 時提供）
# database_url = "postgresql+asyncpg://user:pass@host/db"
# redis_url    = "redis://default:pass@host:port"

# API 配置（開發環境使用較小資源）
api_image_tag     = "dev"
api_cpu           = "1"
api_memory        = "1Gi"
api_min_instances = 0
api_max_instances = 5

# CORS 設定
allowed_origins = "*"

# 前端配置（開發環境不啟用 CDN）
enable_frontend_cdn = false
