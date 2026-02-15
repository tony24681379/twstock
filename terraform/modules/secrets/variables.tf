variable "project_id" {
  description = "GCP 專案 ID"
  type        = string
}

variable "environment" {
  description = "環境名稱 (dev, prod)"
  type        = string
  default     = "prod"
}

variable "database_url" {
  description = "資料庫連線字串"
  type        = string
  sensitive   = true
}

variable "redis_url" {
  description = "Redis 連線字串"
  type        = string
  sensitive   = true
}

variable "cloud_run_service_account" {
  description = "Cloud Run Service Account Email"
  type        = string
}
