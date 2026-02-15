#!/bin/bash
# 清理 GCP 資源
#
# ⚠️  警告: 此腳本會刪除所有部署的資源
# 請確保您了解這個操作的後果

set -euo pipefail

PROJECT_ID="${1:-}"
REGION="asia-east1"

if [ -z "$PROJECT_ID" ]; then
    echo "用法: ./scripts/cleanup-gcp.sh <project-id>"
    exit 1
fi

echo "⚠️  警告: 即將刪除專案 $PROJECT_ID 的所有資源"
echo "這包括:"
echo "  - Cloud Run 服務"
echo "  - Artifact Registry 倉庫"
echo "  - Secret Manager secrets"
echo "  - Cloud Storage buckets"
echo ""
echo "輸入 'DELETE' 確認刪除:"
read -r confirmation

if [ "$confirmation" != "DELETE" ]; then
    echo "已取消"
    exit 0
fi

echo "開始清理資源..."

# 刪除 Cloud Run 服務
echo "刪除 Cloud Run 服務..."
gcloud run services delete twstock-api \
    --region="$REGION" \
    --project="$PROJECT_ID" \
    --quiet 2>/dev/null || echo "  (服務不存在或已刪除)"

# 刪除 Artifact Registry
echo "刪除 Artifact Registry..."
gcloud artifacts repositories delete twstock-docker \
    --location="$REGION" \
    --project="$PROJECT_ID" \
    --quiet 2>/dev/null || echo "  (倉庫不存在或已刪除)"

# 刪除 Secrets
echo "刪除 Secrets..."
gcloud secrets delete database_url \
    --project="$PROJECT_ID" \
    --quiet 2>/dev/null || echo "  (database_url 不存在)"
gcloud secrets delete redis_url \
    --project="$PROJECT_ID" \
    --quiet 2>/dev/null || echo "  (redis_url 不存在)"

# 刪除 Storage Bucket（需要先清空）
echo "刪除 Cloud Storage Bucket..."
BUCKET="$PROJECT_ID-frontend"
gsutil -m rm -r "gs://$BUCKET/**" 2>/dev/null || true
gsutil rb "gs://$BUCKET" 2>/dev/null || echo "  (Bucket 不存在或已刪除)"

# 使用 Terraform 清理（如果有 tfstate）
if [ -f "terraform/terraform.tfstate" ]; then
    echo "使用 Terraform 清理..."
    cd terraform
    terraform destroy -auto-approve -var-file=environments/prod.tfvars 2>/dev/null || true
    cd ..
fi

echo ""
echo "✅ 清理完成"
echo ""
echo "注意: Service Account 和 IAM 角色需要手動刪除"
