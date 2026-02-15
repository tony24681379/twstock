output "project_id" {
  description = "GCP 專案 ID"
  value       = var.project_id
}

output "region" {
  description = "GCP 區域"
  value       = var.region
}

output "api_url" {
  description = "API Cloud Run URL"
  value       = module.cloud_run.service_url
}

output "frontend_url" {
  description = "前端 Cloud Storage URL"
  value       = module.storage.frontend_url
}

output "frontend_bucket_name" {
  description = "前端 Cloud Storage Bucket 名稱"
  value       = module.storage.bucket_name
}

output "artifact_registry_url" {
  description = "Artifact Registry URL"
  value       = module.artifact.repository_url
}

output "database_secret_name" {
  description = "資料庫 Secret 名稱"
  value       = module.secrets.database_secret_name
}

output "redis_secret_name" {
  description = "Redis Secret 名稱"
  value       = module.secrets.redis_secret_name
}

# Firebase Hosting 相關輸出
output "firebase_dns_records" {
  description = "需要在 GoDaddy 配置的 DNS 記錄"
  value       = module.firebase_hosting.required_dns_records
  sensitive   = false
}

output "firebase_custom_domain_status" {
  description = "Firebase 自定義域名狀態"
  value       = module.firebase_hosting.custom_domain_status
  sensitive   = false
}

output "firebase_site_id" {
  description = "Firebase Hosting Site ID"
  value       = module.firebase_hosting.site_id
}
