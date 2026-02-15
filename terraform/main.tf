# Twstock GCP 部署 - 主配置

# 1. Artifact Registry - Docker 映像倉庫
module "artifact" {
  source = "./modules/artifact"

  project_id      = var.project_id
  region          = var.region
  repository_name = "twstock-docker"
  environment     = "prod"
}

# 2. Cloud Run Service Account（需要先建立，供 Secrets 模組使用）
resource "google_service_account" "cloud_run_temp" {
  account_id   = "twstock-cloud-run-temp"
  display_name = "Temporary SA for Secrets"
  description  = "Temporary service account for setting up secrets"
}

# 3. Secret Manager - 管理敏感資料
module "secrets" {
  source = "./modules/secrets"

  project_id                = var.project_id
  environment               = "prod"
  database_url              = var.database_url
  redis_url                 = var.redis_url
  cloud_run_service_account = google_service_account.cloud_run_temp.email

  depends_on = [
    google_service_account.cloud_run_temp
  ]
}

# 4. Cloud Run - API 服務
module "cloud_run" {
  source = "./modules/cloud-run"

  project_id         = var.project_id
  region             = var.region
  service_name       = "twstock-api"
  environment        = "prod"
  image_url          = "${module.artifact.repository_url}/api"
  image_tag          = var.api_image_tag
  cpu                = var.api_cpu
  memory             = var.api_memory
  min_instances      = var.api_min_instances
  max_instances      = var.api_max_instances
  database_secret_id = module.secrets.database_secret_id
  redis_secret_id    = module.secrets.redis_secret_id
  allowed_origins    = var.allowed_origins

  # ⚠️ Cloud Run IAM: 設為 true 允許未驗證存取 Cloud Run 端點
  # ⚠️ 實際的身份驗證由 API 應用層處理（透過 REQUIRE_AUTH 環境變數）
  allow_unauthenticated = true

  # 身份驗證設定（應用層）
  require_auth     = var.require_auth
  google_client_id = var.google_client_id
  allowed_emails   = var.allowed_emails

  # 資料保留策略
  environment_type    = var.environment_type
  data_retention_days = var.data_retention_days
  cleanup_vacuum      = var.cleanup_vacuum

  depends_on = [
    module.artifact,
    module.secrets
  ]
}

# 5. Cloud Storage - 前端靜態網站
module "storage" {
  source = "./modules/storage"

  project_id    = var.project_id
  region        = var.region
  environment   = "prod"
  enable_cdn    = var.enable_frontend_cdn
  force_destroy = false
  cors_origins  = ["*"]
}

# 6. 啟用必要的 API
resource "google_project_service" "required_apis" {
  for_each = toset([
    "run.googleapis.com",
    "cloudbuild.googleapis.com",
    "artifactregistry.googleapis.com",
    "secretmanager.googleapis.com",
    "storage.googleapis.com",
    "compute.googleapis.com",
    "firebase.googleapis.com",
    "firebasehosting.googleapis.com",
  ])

  project = var.project_id
  service = each.value

  disable_on_destroy = false
}

# 7. Firebase Hosting - 自定義域名配置
module "firebase_hosting" {
  source = "./modules/firebase-hosting"

  project_id    = var.project_id
  site_id       = var.project_id # 通常與 project_id 相同
  custom_domain = "twstock.changes.live"

  depends_on = [
    google_project_service.required_apis
  ]
}
