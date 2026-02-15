# Bootstrap 變數定義

variable "project_id" {
  description = "GCP 專案 ID"
  type        = string
  default     = "twstock-484714"
}

variable "region" {
  description = "GCP 區域（使用 us-central1 享受免費層級）"
  type        = string
  default     = "us-central1"
}

variable "bucket_location" {
  description = "GCS bucket 位置（使用 us-central1 享受 5GB 免費存儲）"
  type        = string
  default     = "us-central1"
}
