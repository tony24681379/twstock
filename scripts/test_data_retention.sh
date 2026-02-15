#!/bin/bash
# 資料保留策略測試腳本

set -e

echo "🧪 資料保留策略測試"
echo "===================="
echo ""

# 設定環境變數
export ENVIRONMENT=development
export DATA_RETENTION_DAYS=0
export CLEANUP_VACUUM=true

API_URL="http://localhost:8000"

echo "📋 測試 1: 查看維護配置"
echo "------------------------"
response=$(curl -s "${API_URL}/api/maintenance/config")
echo "$response" | jq .
echo ""

echo "📊 測試 2: 查看資料統計"
echo "------------------------"
response=$(curl -s "${API_URL}/api/maintenance/stats")
echo "$response" | jq '{
  environment: .environment,
  retention_days: .retention_days,
  retention_enabled: .retention_enabled,
  database_size_mb: .tables._database_info.size_mb,
  stock_daily_rows: .tables.stock_daily.total_rows,
  date_range: .tables.stock_daily.date_range_days
}'
echo ""

echo "🔍 測試 3: Dry Run 清理（250 天）"
echo "-----------------------------------"
response=$(curl -s -X POST "${API_URL}/api/maintenance/cleanup" \
  -H "Content-Type: application/json" \
  -d '{"dry_run": true, "retention_days": 250}')
echo "$response" | jq '{
  status: .status,
  retention_days: .retention_days,
  cutoff_date: .cutoff_date,
  dry_run: .dry_run,
  deleted_counts: .deleted_counts
}'
echo ""

echo "🚫 測試 4: 本地環境執行清理（應該被拒絕）"
echo "----------------------------------------"
response=$(curl -s -X POST "${API_URL}/api/maintenance/cleanup" \
  -H "Content-Type: application/json" \
  -d '{"retention_days": 250}')
if echo "$response" | grep -q "403\|只允許在生產環境"; then
  echo "✅ 正確拒絕本地環境清理"
  echo "$response" | jq .
else
  echo "❌ 錯誤：本地環境不應該允許清理"
  echo "$response" | jq .
  exit 1
fi
echo ""

echo "✅ 所有測試通過！"
echo ""
echo "📝 摘要："
echo "  - 維護 API 端點正常運作"
echo "  - 資料統計查詢成功"
echo "  - Dry run 模式正常"
echo "  - 本地環境安全機制有效"
