# Secret Manager - 管理敏感資料

# 資料庫連線字串 Secret
resource "google_secret_manager_secret" "database_url" {
  secret_id = "database_url"

  replication {
    auto {}
  }

  labels = {
    app = "twstock"
    env = var.environment
  }
}

resource "google_secret_manager_secret_version" "database_url" {
  secret      = google_secret_manager_secret.database_url.id
  secret_data = var.database_url

  lifecycle {
    ignore_changes = [secret_data]
  }
}

# Redis 連線字串 Secret
resource "google_secret_manager_secret" "redis_url" {
  secret_id = "redis_url"

  replication {
    auto {}
  }

  labels = {
    app = "twstock"
    env = var.environment
  }
}

resource "google_secret_manager_secret_version" "redis_url" {
  secret      = google_secret_manager_secret.redis_url.id
  secret_data = var.redis_url

  lifecycle {
    ignore_changes = [secret_data]
  }
}

# 授予 Cloud Run Service Account 存取 Secrets 的權限
resource "google_secret_manager_secret_iam_member" "database_url_accessor" {
  secret_id = google_secret_manager_secret.database_url.id
  role      = "roles/secretmanager.secretAccessor"
  member    = "serviceAccount:${var.cloud_run_service_account}"
}

resource "google_secret_manager_secret_iam_member" "redis_url_accessor" {
  secret_id = google_secret_manager_secret.redis_url.id
  role      = "roles/secretmanager.secretAccessor"
  member    = "serviceAccount:${var.cloud_run_service_account}"
}
