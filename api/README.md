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
