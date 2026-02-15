variable "project_id" {
  description = "GCP 專案 ID"
  type        = string
}

variable "region" {
  description = "GCP 區域"
  type        = string
}

variable "service_name" {
  description = "Cloud Run 服務名稱"
  type        = string
  default     = "twstock-api"
}

variable "environment" {
  description = "環境名稱 (dev, prod)"
  type        = string
  default     = "prod"
}

variable "image_url" {
  description = "Docker 映像 URL（不含標籤）"
  type        = string
}

variable "image_tag" {
  description = "Docker 映像標籤"
  type        = string
  default     = "latest"
}

variable "cpu" {
  description = "CPU 配置"
  type        = string
  default     = "2"
}

variable "memory" {
  description = "記憶體配置"
  type        = string
  default     = "2Gi"
}

variable "min_instances" {
  description = "最小實例數"
  type        = number
  default     = 0
}

variable "max_instances" {
  description = "最大實例數"
  type        = number
  default     = 10
}

variable "database_secret_id" {
  description = "資料庫 Secret 完整 ID"
  type        = string
}

variable "redis_secret_id" {
  description = "Redis Secret 完整 ID"
  type        = string
}

variable "allowed_origins" {
  description = "CORS 允許的來源"
  type        = string
  default     = "*"
}

variable "allow_unauthenticated" {
  description = "是否允許未驗證存取（公開 API）"
  type        = bool
  default     = true
}

variable "vpc_connector" {
  description = "VPC Connector 名稱（可選）"
  type        = string
  default     = null
}

# 身份驗證設定
variable "require_auth" {
  description = "是否啟用 API 身份驗證"
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
