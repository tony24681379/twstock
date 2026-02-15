# Terraform Backend Bootstrap
# 此配置用於創建 Terraform state 的 GCS bucket
# 只需要執行一次

terraform {
  required_version = ">= 1.5.0"

  required_providers {
    google = {
      source  = "hashicorp/google"
      version = "~> 5.0"
    }
  }
}

provider "google" {
  project = var.project_id
  region  = var.region
}

# 啟用 Storage API
resource "google_project_service" "storage_api" {
  project = var.project_id
  service = "storage.googleapis.com"

  disable_on_destroy = false
}

# 創建 GCS bucket 用於 Terraform state
# 使用 us-central1 享受 GCP 免費層級（5GB 免費存儲）
resource "google_storage_bucket" "terraform_state" {
  name     = "${var.project_id}-terraform-state"
  location = var.bucket_location
  project  = var.project_id

  # 啟用版本控制以保護 state 歷史
  versioning {
    enabled = true
  }

  # 統一的 bucket 級別訪問控制
  uniform_bucket_level_access = true

  # 生命週期規則：保留最近 10 個版本
  lifecycle_rule {
    condition {
      num_newer_versions = 10
    }
    action {
      type = "Delete"
    }
  }

  # 防止意外刪除（生產環境建議啟用）
  force_destroy = false

  # 加密設定：預設使用 Google 管理的密鑰（無需明確配置）

  # 標籤
  labels = {
    purpose     = "terraform-state"
    environment = "prod"
    managed_by  = "terraform"
  }

  depends_on = [google_project_service.storage_api]
}

# 輸出 bucket 資訊
output "bucket_name" {
  description = "Terraform state bucket 名稱"
  value       = google_storage_bucket.terraform_state.name
}

output "bucket_url" {
  description = "Terraform state bucket URL"
  value       = google_storage_bucket.terraform_state.url
}
