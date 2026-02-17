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

  cleanup_policy_dry_run = false

  cleanup_policies {
    id     = "keep-latest-only"
    action = "KEEP"
    most_recent_versions {
      keep_count = 1
    }
  }

  cleanup_policies {
    id     = "delete-untagged"
    action = "DELETE"
    condition {
      tag_state  = "UNTAGGED"
      older_than = "86400s"
    }
  }
}
