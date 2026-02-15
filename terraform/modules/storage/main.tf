# Cloud Storage - 前端靜態網站

# 建立 Storage Bucket
resource "google_storage_bucket" "frontend" {
  name     = "${var.project_id}-frontend"
  location = var.region

  uniform_bucket_level_access = true
  force_destroy               = var.force_destroy

  website {
    main_page_suffix = "index.html"
    not_found_page   = "index.html"
  }

  cors {
    origin          = var.cors_origins
    method          = ["GET", "HEAD", "OPTIONS"]
    response_header = ["*"]
    max_age_seconds = 3600
  }

  labels = {
    app = "twstock"
    env = var.environment
  }
}

# 設定 Bucket 為公開可讀
resource "google_storage_bucket_iam_member" "public_read" {
  bucket = google_storage_bucket.frontend.name
  role   = "roles/storage.objectViewer"
  member = "allUsers"
}

# 可選：啟用 CDN
resource "google_compute_backend_bucket" "frontend" {
  count       = var.enable_cdn ? 1 : 0
  name        = "${var.project_id}-frontend-backend"
  bucket_name = google_storage_bucket.frontend.name
  enable_cdn  = true

  cdn_policy {
    cache_mode        = "CACHE_ALL_STATIC"
    default_ttl       = 3600
    client_ttl        = 3600
    max_ttl           = 86400
    negative_caching  = true
    serve_while_stale = 86400
  }
}

# 可選：Load Balancer URL Map
resource "google_compute_url_map" "frontend" {
  count           = var.enable_cdn ? 1 : 0
  name            = "${var.project_id}-frontend-lb"
  default_service = google_compute_backend_bucket.frontend[0].id
}

# 可選：HTTP Proxy
resource "google_compute_target_http_proxy" "frontend" {
  count   = var.enable_cdn ? 1 : 0
  name    = "${var.project_id}-frontend-proxy"
  url_map = google_compute_url_map.frontend[0].id
}

# 可選：全域轉發規則（分配外部 IP）
resource "google_compute_global_forwarding_rule" "frontend" {
  count       = var.enable_cdn ? 1 : 0
  name        = "${var.project_id}-frontend-lb-rule"
  target      = google_compute_target_http_proxy.frontend[0].id
  port_range  = "80"
  ip_protocol = "TCP"
}
