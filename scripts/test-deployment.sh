#!/bin/bash
# 測試部署是否正常運作
set -euo pipefail

PROJECT_ID="${1:-${GCP_PROJECT_ID:-}}"
REGION="asia-east1"

if [ -z "$PROJECT_ID" ]; then
    echo "用法: ./scripts/test-deployment.sh <project-id>"
    exit 1
fi

echo "🔍 測試部署: $PROJECT_ID"

# 取得 API URL
API_URL=$(gcloud run services describe twstock-api \
    --region="$REGION" \
    --project="$PROJECT_ID" \
    --format='value(status.url)')

echo "API URL: $API_URL"

# 測試健康檢查
echo -n "健康檢查... "
if curl -sf "$API_URL/api/health" > /dev/null; then
    echo "✅ 通過"
    curl -s "$API_URL/api/health" | jq '.'
else
    echo "❌ 失敗"
    exit 1
fi

# 測試 Redis 快取統計
echo -n "Redis 快取統計... "
if curl -sf "$API_URL/api/cache/redis/stats" > /dev/null; then
    echo "✅ 通過"
    curl -s "$API_URL/api/cache/redis/stats" | jq '.'
else
    echo "⚠️  無法取得（可能未啟用 Redis）"
fi

# 測試前端
FRONTEND_URL="https://storage.googleapis.com/$PROJECT_ID-frontend/index.html"
echo -n "前端訪問... "
HTTP_CODE=$(curl -s -o /dev/null -w "%{http_code}" "$FRONTEND_URL")
if [ "$HTTP_CODE" = "200" ]; then
    echo "✅ 通過 (HTTP $HTTP_CODE)"
else
    echo "❌ 失敗 (HTTP $HTTP_CODE)"
fi

echo ""
echo "✅ 部署測試完成"
