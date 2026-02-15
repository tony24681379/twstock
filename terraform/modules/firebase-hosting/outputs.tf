# Firebase Hosting 模組輸出

output "required_dns_records" {
  description = "需要在 GoDaddy 配置的 DNS 記錄"
  value       = try(google_firebase_hosting_custom_domain.custom.required_dns_updates, [])
}

output "custom_domain_status" {
  description = "自定義域名驗證狀態"
  value = {
    custom_domain = google_firebase_hosting_custom_domain.custom.custom_domain
    cert_state    = try(google_firebase_hosting_custom_domain.custom.cert[0].state, "unknown")
    cert_type     = try(google_firebase_hosting_custom_domain.custom.cert[0].type, "unknown")
  }
}

output "site_id" {
  description = "Firebase Hosting Site ID"
  value       = google_firebase_hosting_site.default.site_id
}
