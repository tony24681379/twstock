variable "project_id" {
  description = "GCP 專案 ID"
  type        = string
}

variable "region" {
  description = "GCP 區域"
  type        = string
}

variable "environment" {
  description = "環境名稱 (dev, prod)"
  type        = string
  default     = "prod"
}

variable "force_destroy" {
  description = "是否在刪除 Terraform 資源時強制刪除 Bucket 內容"
  type        = bool
  default     = false
}

variable "enable_cdn" {
  description = "是否啟用 CDN"
  type        = bool
  default     = true
}

variable "cors_origins" {
  description = "CORS 允許的來源"
  type        = list(string)
  default     = ["*"]
}
