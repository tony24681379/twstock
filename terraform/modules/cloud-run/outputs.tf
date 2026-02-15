output "service_name" {
  description = "Cloud Run 服務名稱"
  value       = google_cloud_run_v2_service.api.name
}

output "service_url" {
  description = "Cloud Run 服務 URL"
  value       = google_cloud_run_v2_service.api.uri
}

output "service_account_email" {
  description = "Cloud Run Service Account Email"
  value       = google_service_account.cloud_run.email
}

output "service_id" {
  description = "Cloud Run 服務 ID"
  value       = google_cloud_run_v2_service.api.id
}
