# Cloud Run - API 服務

# 建立 Cloud Run Service Account
resource "google_service_account" "cloud_run" {
  account_id   = "twstock-cloud-run"
  display_name = "Twstock Cloud Run Service Account"
  description  = "Service account for Twstock Cloud Run API"
}

# 授予 Service Account 必要權限
resource "google_project_iam_member" "cloud_run_secretmanager" {
  project = var.project_id
  role    = "roles/secretmanager.secretAccessor"
  member  = "serviceAccount:${google_service_account.cloud_run.email}"
}

# Cloud Run 服務
resource "google_cloud_run_v2_service" "api" {
  name     = var.service_name
  location = var.region
  ingress  = "INGRESS_TRAFFIC_ALL"

  template {
    service_account = google_service_account.cloud_run.email

    # 容器配置
    containers {
      image = "${var.image_url}:${var.image_tag}"

      # 資源限制
      resources {
        limits = {
          cpu    = var.cpu
          memory = var.memory
        }

        cpu_idle          = true
        startup_cpu_boost = true
      }

      # 容器端口
      ports {
        container_port = 8000
      }

      # 環境變數
      env {
        name  = "ALLOWED_ORIGINS"
        value = var.allowed_origins
      }

      # 身份驗證設定
      env {
        name  = "REQUIRE_AUTH"
        value = tostring(var.require_auth)
      }

      env {
        name  = "GOOGLE_CLIENT_ID"
        value = var.google_client_id
      }

      env {
        name  = "ALLOWED_EMAILS"
        value = var.allowed_emails
      }

      # 資料保留策略
      env {
        name  = "ENVIRONMENT"
        value = var.environment_type
      }

      env {
        name  = "DATA_RETENTION_DAYS"
        value = tostring(var.data_retention_days)
      }

      env {
        name  = "CLEANUP_VACUUM"
        value = tostring(var.cleanup_vacuum)
      }

      # 從 Secret Manager 取得資料庫連線字串
      env {
        name = "DATABASE_URL"
        value_source {
          secret_key_ref {
            secret  = var.database_secret_id
            version = "latest"
          }
        }
      }

      # 從 Secret Manager 取得 Redis 連線字串
      env {
        name = "REDIS_URL"
        value_source {
          secret_key_ref {
            secret  = var.redis_secret_id
            version = "latest"
          }
        }
      }

      # 健康檢查（增加啟動時間）
      startup_probe {
        http_get {
          path = "/api/health"
          port = 8000
        }
        initial_delay_seconds = 10
        period_seconds        = 15
        timeout_seconds       = 10
        failure_threshold     = 5
      }

      liveness_probe {
        http_get {
          path = "/api/health"
          port = 8000
        }
        period_seconds    = 30
        timeout_seconds   = 5
        failure_threshold = 3
      }
    }

    # 擴展配置
    scaling {
      min_instance_count = var.min_instances
      max_instance_count = var.max_instances
    }

    # 請求超時
    timeout = "300s"

    # VPC Connector (可選)
    # vpc_access {
    #   connector = var.vpc_connector
    #   egress    = "ALL_TRAFFIC"
    # }
  }

  traffic {
    type    = "TRAFFIC_TARGET_ALLOCATION_TYPE_LATEST"
    percent = 100
  }

  labels = {
    app = "twstock"
    env = var.environment
  }

  depends_on = [
    google_project_iam_member.cloud_run_secretmanager
  ]
}

# 允許未驗證存取（公開 API）
resource "google_cloud_run_v2_service_iam_member" "public_access" {
  count    = var.allow_unauthenticated ? 1 : 0
  project  = google_cloud_run_v2_service.api.project
  location = google_cloud_run_v2_service.api.location
  name     = google_cloud_run_v2_service.api.name
  role     = "roles/run.invoker"
  member   = "allUsers"
}
