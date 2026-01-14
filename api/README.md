# TW Stock Chip Dashboard API

台灣股市籌碼集中度儀表板後端 API

## 開發環境啟動

```bash
# 從專案根目錄執行
cd /Users/tony/twstock

# 確保資料庫正在運行
./start-db.sh

# 啟動 API（開發模式）
poetry run uvicorn api.main:app --reload --host 0.0.0.0 --port 8000
```

## API 文件

啟動後造訪：
- Swagger UI: http://localhost:8000/docs
- ReDoc: http://localhost:8000/redoc
- 健康檢查: http://localhost:8000/api/health

## 環境變數

建立 `.env` 檔案（可選）：

```bash
# 資料庫
DATABASE_URL=postgresql+asyncpg://twstock_user:twstock_password123@localhost:5432/twstock

# CORS（允許的前端網域）
ALLOWED_ORIGINS=http://localhost:5173,http://localhost:3000

# 身份驗證（可選，預設關閉）
REQUIRE_AUTH=false
ALLOWED_EMAILS=your-email@gmail.com
GOOGLE_CLIENT_ID=your-client-id.apps.googleusercontent.com
```

## API 端點

### 股票列表
```
GET /api/stocks?sort_by=stock_id&order=asc&limit=100&offset=0
```

### 股票詳細資訊
```
GET /api/stocks/2330
```

### 股票籌碼歷史
```
GET /api/stocks/2330/history?weeks=12
```

### 健康檢查
```
GET /api/health
```

## Docker 部署

### 快速開始

完整部署指南請參閱 [docs/DEPLOYMENT.md](../docs/DEPLOYMENT.md)

```bash
# 1. 設定環境變數
cp .env.production.example .env
# 編輯 .env 檔案，修改密碼與網域

# 2. 建置並啟動所有服務（PostgreSQL + API + Frontend）
docker-compose build
docker-compose up -d

# 3. 檢查服務狀態
docker-compose ps

# 4. 查看 API 日誌
docker-compose logs -f api

# 5. 測試 API
curl http://localhost:8000/api/health
```

### Docker 映像資訊

- **基礎映像**: python:3.12-slim
- **建置方式**: 多階段建置（builder + runtime）
- **映像大小**: ~200MB
- **關鍵特性**:
  - Poetry 2.2.1 依賴管理
  - TA-Lib 0.6.8 預編譯 wheel（無需手動編譯 C library）
  - 非 root 使用者（appuser）
  - 健康檢查機制（每 30 秒）
  - 4 個 Uvicorn workers

### 生產環境注意事項

1. **必須修改的環境變數**：
   - `POSTGRES_PASSWORD` - 資料庫密碼（必須設定強密碼）
   - `DATABASE_URL` - 更新密碼部分
   - `ALLOWED_ORIGINS` - 修改為實際前端網域

2. **效能調校**：
   - 根據 CPU 核心數調整 worker 數量（詳見 DEPLOYMENT.md）
   - 預設 4 個 Uvicorn workers，建議值 = (CPU核心數 × 2) + 1

3. **監控與日誌**：
   - 健康檢查端點：`GET /api/health`
   - 快取統計：`GET /api/cache/stats`
   - 日誌：`docker-compose logs -f api`

4. **備份策略**：
   - 定期備份 PostgreSQL 資料庫
   - 範例：`docker-compose exec -T postgres pg_dump -U twstock_user twstock > backup.sql`

### 疑難排解

常見問題請參閱 [docs/DEPLOYMENT.md - 疑難排解章節](../docs/DEPLOYMENT.md#疑難排解)

- 容器無法啟動 → 檢查日誌與環境變數
- 前端無法連接後端 → 檢查 CORS 設定
- 資料庫連線失敗 → 檢查 DATABASE_URL
- 記憶體不足 → 減少 worker 數量
- TA-Lib 錯誤 → 確認使用 ta-lib 0.6.8
