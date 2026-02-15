output "repository_id" {
  description = "Artifact Registry 倉庫 ID"
  value       = google_artifact_registry_repository.docker.repository_id
}

output "repository_url" {
  description = "Artifact Registry 倉庫 URL"
  value       = "${var.region}-docker.pkg.dev/${var.project_id}/${google_artifact_registry_repository.docker.repository_id}"
}
