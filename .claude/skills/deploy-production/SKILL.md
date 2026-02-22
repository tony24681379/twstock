---
name: deploy-production
description: Deploy twstock to GCP production using Terraform as the core deployment mechanism. Supports full deploy, API-only, frontend-only, crawl-only, and infra-only scopes.
---

Production deployment skill for twstock. Uses Terraform (not `gcloud run deploy`) as the single source of truth for infrastructure.

**Input**: Optionally specify a scope. If omitted, run the full pipeline.

| Scope | What it does |
|-------|-------------|
| (none) | Full pipeline: build → terraform → health check → cleanup → frontend → crawl |
| `api` | Steps 1-5: build image → terraform apply → health check → cleanup |
| `frontend` | Step 6 only: build and deploy frontend to Firebase Hosting |
| `crawl` | Step 7 only: run production crawler via `run-production.sh` |
| `infra` | Step 3 only: terraform apply without rebuilding Docker image |

---

## Constants — always read from source, never hardcode

Before starting, read these values dynamically:

```bash
# Image tag from current git commit
IMAGE_TAG=$(git rev-parse --short HEAD)

# Terraform outputs (after init)
cd terraform && terraform output -json
# Provides: api_url, artifact_registry_url, firebase_site_id, etc.
```

Key files to reference:
- `terraform/environments/prod.tfvars` — production config (region, scaling, auth)
- `terraform/outputs.tf` — output definitions (api_url, artifact_registry_url, etc.)
- `terraform/variables.tf` — variable definitions (api_image_tag at line 24-28)
- `terraform/modules/cloud-run/main.tf` — image reference: `"${var.image_url}:${var.image_tag}"`
- `docker/api/Dockerfile` — multi-stage API container build
- `run-production.sh` — loads `.env.production` + runs crawler
- `COST_WARNING.md` — cost compliance (MUST read before terraform apply)

---

## Step 1: Pre-flight checks

Run these checks and report results. **Stop if any critical check fails.**

```bash
# 1a. Uncommitted changes?
git status --porcelain

# 1b. gcloud authenticated?
gcloud auth print-access-token > /dev/null 2>&1

# 1c. Docker available?
docker info > /dev/null 2>&1

# 1d. Terraform initialized?
ls terraform/.terraform > /dev/null 2>&1
```

Read `COST_WARNING.md` and briefly confirm the deployment stays within free tier limits. Summarize the key constraints to the user:
- Cloud Run: CPU=1, Memory=768Mi, min=0, max=3
- Artifact Registry: clean up old images to stay under 0.5GB
- All services must be free

**Ask user to confirm** before proceeding.

---

## Step 2: Build & Push Docker image

**Skip this step for `infra`, `frontend`, and `crawl` scopes.**

```bash
# Get image tag and registry URL
IMAGE_TAG=$(git rev-parse --short HEAD)

# Read registry from terraform output
cd terraform && REGISTRY_URL=$(terraform output -raw artifact_registry_url) && cd ..

FULL_IMAGE="${REGISTRY_URL}/api:${IMAGE_TAG}"
LATEST_IMAGE="${REGISTRY_URL}/api:latest"

# Build for linux/amd64 (CRITICAL: Apple Silicon will cause exec format error without this)
docker build --platform linux/amd64 \
  -t "${FULL_IMAGE}" \
  -t "${LATEST_IMAGE}" \
  -f docker/api/Dockerfile .

# Push both tags
docker push "${FULL_IMAGE}"
docker push "${LATEST_IMAGE}"
```

Report the image tag and full image URI to the user.

---

## Step 3: Terraform apply

**Skip this step for `frontend` and `crawl` scopes.**

For `infra` scope: use the current `api_image_tag` from `prod.tfvars` (don't override).
For other scopes: override with the newly built image tag.

```bash
cd terraform

# Initialize if needed
terraform init

# Plan
# For infra scope (no image override):
terraform plan -var-file=environments/prod.tfvars

# For api/full scope (with image tag override):
terraform plan -var="api_image_tag=${IMAGE_TAG}" -var-file=environments/prod.tfvars
```

**Show the plan output to the user.** Verify:
- Only the expected changes appear (typically just image tag for api deploys)
- No `google_compute_` resources being created
- No unexpected resource deletions

**Ask user to confirm** the plan before applying.

```bash
# Apply (add -auto-approve only after user confirms)
terraform apply -var="api_image_tag=${IMAGE_TAG}" -var-file=environments/prod.tfvars -auto-approve
```

After apply, capture outputs:
```bash
API_URL=$(terraform output -raw api_url)
```

---

## Step 4: Health check

**Skip this step for `frontend` and `crawl` scopes.**

```bash
API_URL=$(cd terraform && terraform output -raw api_url)
```

Retry up to 5 times with 15-second intervals (Cloud Run cold start can take time):

```bash
for i in 1 2 3 4 5; do
  STATUS=$(curl -s -o /dev/null -w "%{http_code}" "${API_URL}/api/health")
  if [ "$STATUS" = "200" ]; then
    echo "Health check passed on attempt $i"
    break
  fi
  echo "Attempt $i: HTTP $STATUS, retrying in 15s..."
  sleep 15
done
```

If all 5 attempts fail, warn the user and suggest checking Cloud Run logs:
```bash
gcloud run services logs read twstock-api --region=asia-northeast1 --limit=50
```

---

## Step 5: Clean up old Docker images

**Skip this step for `infra`, `frontend`, and `crawl` scopes.**

Keep only the latest image digest. Delete all others.

```bash
REGISTRY_URL=$(cd terraform && terraform output -raw artifact_registry_url)
IMAGE_PATH="${REGISTRY_URL}/api"

# List all image digests (excluding the one we just pushed)
gcloud artifacts docker images list "${IMAGE_PATH}" \
  --include-tags --format="get(version)" --sort-by=~UPDATE_TIME
```

Show the user which images will be deleted. **Ask for confirmation**, then delete old digests:

```bash
# Delete each old digest (the ones NOT tagged with ${IMAGE_TAG} or latest)
gcloud artifacts docker images delete "${IMAGE_PATH}@<OLD_DIGEST>" --quiet --delete-tags
```

Report how many images were cleaned up and the estimated storage savings.

---

## Step 6: Frontend deploy

**Skip this step for `api`, `infra`, and `crawl` scopes.**

```bash
API_URL=$(cd terraform && terraform output -raw api_url)

cd dashboard

# Install dependencies
npm ci

# Build with API URL injected
VITE_API_BASE_URL="${API_URL}" npm run build

# Deploy to Firebase Hosting
firebase deploy --only hosting
```

Report the frontend URL (`https://twstock.changes.live`) to the user.

---

## Step 7: Run production crawler

**Skip this step for `api`, `infra`, and `frontend` scopes.**

```bash
# Verify .env.production exists
ls -la .env.production
```

If `.env.production` doesn't exist, **stop and warn the user**. This file contains production DB/Redis connection strings and is gitignored.

**Ask user to confirm** before running the crawler (it will write to the production database).

```bash
# Execute crawler
./run-production.sh
```

This loads `.env.production` and runs `poetry run python -m twstock.main`, which:
- Fetches latest stock data from Wantgoo APIs
- Updates the production PostgreSQL database
- Takes approximately 10 minutes on first run, 2 minutes on subsequent runs

---

## Step 8: Summary

After all steps complete, provide a concise summary:

```
Deployment Summary
━━━━━━━━━━━━━━━━━
Image:    <registry>/api:<sha>
API:      <api_url> (healthy ✓)
Frontend: https://twstock.changes.live
Crawler:  completed / skipped
Cleanup:  N old images removed
```

---

## Error handling

- **Terraform plan shows unexpected changes**: Stop and ask the user to investigate. Do NOT auto-apply.
- **Docker build fails**: Show the error. Common fix: check Dockerfile and dependency changes.
- **Health check fails**: Show Cloud Run logs. Common causes: missing secrets, OOM (check memory config).
- **Firebase deploy fails**: Check `firebase login` status.
- **Crawler fails**: Check `.env.production` values. Common issues: DB connection timeout, API rate limiting.

## Important rules

1. **NEVER use `gcloud run deploy`** — Terraform is the single source of truth for Cloud Run
2. **NEVER skip user confirmation** before terraform apply or crawler execution
3. **ALWAYS build with `--platform linux/amd64`** on Apple Silicon
4. **ALWAYS read COST_WARNING.md** before terraform operations
5. **ALWAYS clean up old Artifact Registry images** to minimize storage costs
