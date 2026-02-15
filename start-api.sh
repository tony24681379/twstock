#!/bin/bash
# 啟動後端 API

echo "🚀 啟動台股籌碼儀表板 API..."
echo ""
echo "API 將運行在: http://localhost:8000"
echo "API 文件: http://localhost:8000/docs"
echo ""

cd "$(dirname "$0")"
poetry run uvicorn api.main:app --reload --host 0.0.0.0 --port 8000
