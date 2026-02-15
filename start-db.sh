#!/bin/bash

echo "🚀 啟動 PostgreSQL 資料庫..."

# 檢查 Docker 是否運行
if ! docker info > /dev/null 2>&1; then
    echo "❌ Docker 未運行，請先啟動 Docker"
    exit 1
fi

# 停止現有容器（如果存在）
echo "📦 停止現有容器..."
docker-compose down

# 啟動 PostgreSQL
echo "🔄 啟動 PostgreSQL..."
docker-compose up -d postgres

# 等待 PostgreSQL 準備就緒
echo "⏳ 等待 PostgreSQL 啟動..."
for i in {1..30}; do
    if docker-compose exec -T postgres pg_isready -U twstock_user -d twstock > /dev/null 2>&1; then
        echo "✅ PostgreSQL 已準備就緒！"
        break
    fi
    echo -n "."
    sleep 1
done

# 啟動 PgAdmin（可選）
read -p "是否要啟動 PgAdmin？(y/n): " -n 1 -r
echo
if [[ $REPLY =~ ^[Yy]$ ]]; then
    echo "🔄 啟動 PgAdmin..."
    docker-compose up -d pgadmin
    echo "✅ PgAdmin 已啟動在 http://localhost:5050"
    echo "   Email: admin@twstock.local"
    echo "   Password: admin123"
fi

# 顯示連線資訊
echo ""
echo "📊 資料庫連線資訊："
echo "   Host: localhost"
echo "   Port: 5432"
echo "   Database: twstock"
echo "   User: twstock_user"
echo "   Password: twstock_password123"
echo ""
echo "🔗 連線字串："
echo "   postgresql://twstock_user:twstock_password123@localhost:5432/twstock"
echo ""

# 檢查資料庫狀態
echo "📈 資料庫狀態："
docker-compose ps