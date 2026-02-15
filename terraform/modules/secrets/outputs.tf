output "database_secret_name" {
  description = "資料庫 Secret 名稱"
  value       = google_secret_manager_secret.database_url.secret_id
}

output "database_secret_id" {
  description = "資料庫 Secret 完整 ID"
  value       = google_secret_manager_secret.database_url.id
}

output "redis_secret_name" {
  description = "Redis Secret 名稱"
  value       = google_secret_manager_secret.redis_url.secret_id
}

output "redis_secret_id" {
  description = "Redis Secret 完整 ID"
  value       = google_secret_manager_secret.redis_url.id
}
