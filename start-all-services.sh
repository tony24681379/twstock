#!/bin/bash

echo "🚀 啟動完整系統..."
echo ""

# 1. 啟動資料庫
echo "📦 步驟 1/3: 啟動 PostgreSQL 資料庫..."
./start-db.sh
if [ $? -ne 0 ]; then
    echo "❌ 資料庫啟動失敗"
    exit 1
fi
echo "✅ 資料庫已啟動"
echo ""

# 等待資料庫準備就緒
echo "⏳ 等待資料庫準備就緒（5 秒）..."
sleep 5
echo ""

# 2. 啟動後端 API
echo "🔧 步驟 2/3: 啟動後端 API（背景執行）..."
poetry run uvicorn api.main:app --reload --host 0.0.0.0 --port 8000 > api.log 2>&1 &
API_PID=$!
echo "✅ 後端 API 已啟動（PID: $API_PID）"
echo "   日誌檔案: api.log"
echo "   API 文件: http://localhost:8000/docs"
echo ""

# 等待 API 啟動
echo "⏳ 等待 API 準備就緒（5 秒）..."
sleep 5
echo ""

# 3. 啟動前端
echo "🎨 步驟 3/3: 啟動前端開發伺服器（背景執行）..."
cd dashboard
npm run dev > ../frontend.log 2>&1 &
FRONTEND_PID=$!
cd ..
echo "✅ 前端已啟動（PID: $FRONTEND_PID）"
echo "   日誌檔案: frontend.log"
echo "   前端網址: http://localhost:5173"
echo ""

# 顯示狀態
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo "🎉 所有服務已啟動！"
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo ""
echo "📊 服務資訊："
echo "  - PostgreSQL:  localhost:5432"
echo "  - 後端 API:    http://localhost:8000"
echo "  - API 文件:    http://localhost:8000/docs"
echo "  - 前端:        http://localhost:5173"
echo ""
echo "📝 日誌檔案："
echo "  - API 日誌:    tail -f api.log"
echo "  - 前端日誌:    tail -f frontend.log"
echo ""
echo "🛑 停止所有服務："
echo "  ./stop-all-services.sh"
echo ""
echo "Process IDs:"
echo "  API_PID=$API_PID"
echo "  FRONTEND_PID=$FRONTEND_PID"
echo ""

# 儲存 PID
echo $API_PID > .api.pid
echo $FRONTEND_PID > .frontend.pid
