#!/bin/bash
# 設定 GCP Secrets
set -euo pipefail

PROJECT_ID="${1:-}"
DATABASE_URL="${DATABASE_URL:-}"
REDIS_URL="${REDIS_URL:-}"

if [ -z "$PROJECT_ID" ]; then
    echo "用法: ./scripts/setup-secrets.sh <project-id>"
    exit 1
fi

echo "設定 Secrets 到專案: $PROJECT_ID"

# 建立或更新 database_url secret
if [ -n "$DATABASE_URL" ]; then
    echo "$DATABASE_URL" | gcloud secrets create database_url \
        --data-file=- \
        --project="$PROJECT_ID" 2>/dev/null || \
    echo "$DATABASE_URL" | gcloud secrets versions add database_url \
        --data-file=- \
        --project="$PROJECT_ID"
    echo "✅ database_url secret 已設定"
fi

# 建立或更新 redis_url secret
if [ -n "$REDIS_URL" ]; then
    echo "$REDIS_URL" | gcloud secrets create redis_url \
        --data-file=- \
        --project="$PROJECT_ID" 2>/dev/null || \
    echo "$REDIS_URL" | gcloud secrets versions add redis_url \
        --data-file=- \
        --project="$PROJECT_ID"
    echo "✅ redis_url secret 已設定"
fi

echo "✅ Secrets 設定完成"
