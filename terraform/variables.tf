variable "project_id" {
  description = "GCP 專案 ID"
  type        = string
}

variable "region" {
  description = "GCP 區域"
  type        = string
  default     = "asia-east1"
}

variable "database_url" {
  description = "Neon PostgreSQL 連線字串"
  type        = string
  sensitive   = true
}

variable "redis_url" {
  description = "Upstash Redis 連線字串"
  type        = string
  sensitive   = true
}

variable "api_image_tag" {
  description = "API Docker 映像標籤"
  type        = string
  default     = "latest"
}

variable "api_cpu" {
  description = "API Cloud Run CPU 配置"
  type        = string
  default     = "2"
}

variable "api_memory" {
  description = "API Cloud Run 記憶體配置"
  type        = string
  default     = "2Gi"
}

variable "api_min_instances" {
  description = "API Cloud Run 最小實例數"
  type        = number
  default     = 0
}

variable "api_max_instances" {
  description = "API Cloud Run 最大實例數"
  type        = number
  default     = 10
}

variable "allowed_origins" {
  description = "CORS 允許的來源"
  type        = string
  default     = "*"
}

variable "enable_frontend_cdn" {
  description = "是否啟用前端 CDN"
  type        = bool
  default     = true
}

# 身份驗證設定
variable "require_auth" {
  description = "是否啟用 API 身份驗證（true/false）"
  type        = bool
  default     = false
}

variable "google_client_id" {
  description = "Google OAuth 2.0 Client ID"
  type        = string
  sensitive   = true
  default     = ""
}

variable "allowed_emails" {
  description = "允許訪問的 email 白名單（逗號分隔）"
  type        = string
  default     = ""
}

variable "allowed_members" {
  description = "允許訪問 Cloud Run API 的成員列表（如：user:email@example.com, serviceAccount:sa@project.iam.gserviceaccount.com）"
  type        = list(string)
  default     = []
}

# 資料保留策略設定
variable "environment_type" {
  description = "環境類型（development/production）"
  type        = string
  default     = "production"
}

variable "data_retention_days" {
  description = "資料保留天數（0 表示永久保留）"
  type        = number
  default     = 250
}

variable "cleanup_vacuum" {
  description = "清理時是否執行 VACUUM（釋放磁碟空間）"
  type        = bool
  default     = true
}
