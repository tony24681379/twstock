output "bucket_name" {
  description = "Storage Bucket 名稱"
  value       = google_storage_bucket.frontend.name
}

output "bucket_url" {
  description = "Storage Bucket URL"
  value       = google_storage_bucket.frontend.url
}

output "frontend_url" {
  description = "前端 URL"
  value       = var.enable_cdn && length(google_compute_global_forwarding_rule.frontend) > 0 ? "http://${google_compute_global_forwarding_rule.frontend[0].ip_address}" : "https://storage.googleapis.com/${google_storage_bucket.frontend.name}/index.html"
}

output "cdn_enabled" {
  description = "是否啟用 CDN"
  value       = var.enable_cdn
}

output "cdn_ip_address" {
  description = "CDN 外部 IP 位址（如果已啟用）"
  value       = var.enable_cdn && length(google_compute_global_forwarding_rule.frontend) > 0 ? google_compute_global_forwarding_rule.frontend[0].ip_address : null
}
