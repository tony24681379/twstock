# Firebase Hosting 自定義域名配置

resource "google_firebase_hosting_site" "default" {
  provider = google-beta
  project  = var.project_id
  site_id  = var.site_id
}

resource "google_firebase_hosting_custom_domain" "custom" {
  provider      = google-beta
  project       = var.project_id
  site_id       = google_firebase_hosting_site.default.site_id
  custom_domain = var.custom_domain

  # 等待 Firebase API 啟用
  depends_on = [google_firebase_hosting_site.default]
}
