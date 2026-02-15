#!/bin/bash

echo "🛑 停止所有服務..."
echo ""

# 停止前端
if [ -f .frontend.pid ]; then
    FRONTEND_PID=$(cat .frontend.pid)
    echo "停止前端（PID: $FRONTEND_PID）..."
    kill $FRONTEND_PID 2>/dev/null && echo "✅ 前端已停止" || echo "⚠️  前端已經停止或 PID 不存在"
    rm .frontend.pid
else
    echo "⚠️  找不到前端 PID 檔案"
fi
echo ""

# 停止後端 API
if [ -f .api.pid ]; then
    API_PID=$(cat .api.pid)
    echo "停止後端 API（PID: $API_PID）..."
    kill $API_PID 2>/dev/null && echo "✅ 後端 API 已停止" || echo "⚠️  後端 API 已經停止或 PID 不存在"
    rm .api.pid
else
    echo "⚠️  找不到 API PID 檔案"
fi
echo ""

# 停止資料庫
echo "停止 PostgreSQL 資料庫..."
docker stop twstock-postgres 2>/dev/null && echo "✅ 資料庫已停止" || echo "⚠️  資料庫已經停止或容器不存在"
echo ""

echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo "✅ 所有服務已停止"
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
