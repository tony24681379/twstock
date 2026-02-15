# Artifact Registry - Docker 映像倉庫

resource "google_artifact_registry_repository" "docker" {
  location      = var.region
  repository_id = var.repository_name
  description   = "Twstock Docker images repository"
  format        = "DOCKER"

  labels = {
    app = "twstock"
    env = var.environment
  }
}
