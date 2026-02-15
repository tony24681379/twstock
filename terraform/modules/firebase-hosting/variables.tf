# Firebase Hosting 模組變數

variable "project_id" {
  description = "GCP 專案 ID"
  type        = string
}

variable "site_id" {
  description = "Firebase Hosting site ID（通常與 project_id 相同）"
  type        = string
}

variable "custom_domain" {
  description = "要配置的自定義域名（例如：twstock.changes.live）"
  type        = string
}
