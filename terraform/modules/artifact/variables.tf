variable "project_id" {
  description = "GCP 專案 ID"
  type        = string
}

variable "region" {
  description = "GCP 區域"
  type        = string
}

variable "repository_name" {
  description = "Artifact Registry 倉庫名稱"
  type        = string
  default     = "twstock-docker"
}

variable "environment" {
  description = "環境名稱 (dev, prod)"
  type        = string
  default     = "prod"
}
