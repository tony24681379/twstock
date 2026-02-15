# 台股籌碼集中度儀表板 - 部署指南

本文件說明如何使用 Docker Compose 部署完整的台股籌碼集中度分析系統。

## 系統架構

```
┌─────────────┐     ┌─────────────┐     ┌──────────────┐
│   Browser   │────▶│   Nginx     │────▶│   FastAPI    │
│             │     │  (Frontend) │     │   (Backend)  │
└─────────────┘     └─────────────┘     └──────────────┘
                           │                     │
                           │                     ▼
                           │              ┌──────────────┐
                           │              │  PostgreSQL  │
                           │              │  (Database)  │
                           │              └──────────────┘
                           ▼
                    靜態資源 (React SPA)
```

## 系統需求

### 硬體需求
- **CPU**: 2 核心以上
- **記憶體**: 4GB 以上（建議 8GB）
- **硬碟**: 10GB 可用空間

### 軟體需求
- **Docker**: 20.10+
- **Docker Compose**: 2.0+
- **作業系統**: Linux / macOS / Windows (WSL2)

## 快速開始

### 1. 克隆專案

```bash
git clone <repository-url>
cd twstock
```

### 2. 設定環境變數

複製環境變數範例檔案：

```bash
cp .env.production.example .env
```

編輯 `.env` 檔案，修改以下設定：

```bash
# PostgreSQL 密碼（必須修改）
POSTGRES_PASSWORD=your_secure_password_here

# API DATABASE_URL（更新密碼）
DATABASE_URL=postgresql+asyncpg://twstock_user:your_secure_password_here@postgres:5432/twstock

# CORS 來源（生產環境需修改為實際域名）
ALLOWED_ORIGINS=https://yourdomain.com,https://www.yourdomain.com

# 前端 API 基礎 URL（生產環境需修改為實際域名）
VITE_API_BASE_URL=https://api.yourdomain.com
```

### 3. 啟動服務

```bash
# 建置 Docker 映像
docker-compose build

# 啟動所有服務（背景執行）
docker-compose up -d

# 檢查服務狀態
docker-compose ps
```

預期輸出：

```
NAME               STATUS              PORTS
twstock-postgres   Up (healthy)        5432
twstock-api        Up (healthy)        8000
twstock-frontend   Up (healthy)        3000->80
```

### 4. 驗證部署

**測試後端 API**：

```bash
# 健康檢查
curl http://localhost:8000/api/health

# 測試股票列表
curl "http://localhost:8000/api/stocks?limit=5"

# 測試特定股票
curl http://localhost:8000/api/stocks/2330
```

**測試前端**：

開啟瀏覽器訪問 http://localhost:3000，應該可以看到股票列表頁面。

### 5. 初始化資料（首次部署）

```bash
# 進入 API 容器
docker-compose exec api bash

# 執行資料更新腳本
poetry run python update_all_revenue.py

# 或執行完整分析
poetry run python main.py
```

## 服務說明

### PostgreSQL (twstock-postgres)

- **映像**: postgres:16-alpine
- **埠號**: 5432
- **資料卷**: `postgres_data`（持久化儲存）
- **健康檢查**: 每 10 秒檢查一次

### FastAPI Backend (twstock-api)

- **映像**: twstock-api（自建）
- **埠號**: 8000
- **依賴**: PostgreSQL
- **Workers**: 4 個 Uvicorn workers
- **健康檢查**: GET /api/health

**API 端點**：
- `GET /api/health` - 健康檢查
- `GET /api/stocks` - 股票列表
- `GET /api/stocks/{stock_id}` - 股票詳細資訊
- `GET /api/cache/stats` - 快取統計
- `GET /docs` - API 文件 (Swagger UI)

### React Frontend (twstock-frontend)

- **映像**: twstock-frontend（自建）
- **埠號**: 3000 (映射到容器的 80)
- **依賴**: FastAPI Backend
- **Web Server**: Nginx 1.25-alpine

**功能頁面**：
- `/` - 股票列表（含篩選與搜尋）
- `/stock/:id` - 股票詳細頁（K線圖、技術指標、籌碼分析）

## Docker 映像資訊

### API 映像

- **基礎映像**: python:3.12-slim
- **建置方式**: 多階段建置（builder + runtime）
- **映像大小**: ~200MB
- **關鍵特性**:
  - Poetry 2.2.1 依賴管理
  - TA-Lib 0.6.8 預編譯 wheel
  - 非 root 使用者（appuser）
  - 健康檢查機制

### Frontend 映像

- **基礎映像**: node:18-alpine + nginx:1.25-alpine
- **建置方式**: 多階段建置（builder + runtime）
- **映像大小**: ~25MB
- **關鍵特性**:
  - Vite 建置優化
  - Gzip 壓縮（節省 60-70% 傳輸）
  - SPA 路由支援
  - API 代理到後端

## 環境變數說明

### PostgreSQL 環境變數

| 變數名稱 | 說明 | 預設值 | 必填 |
|---------|------|--------|------|
| `POSTGRES_DB` | 資料庫名稱 | twstock | 否 |
| `POSTGRES_USER` | 資料庫使用者 | twstock_user | 否 |
| `POSTGRES_PASSWORD` | 資料庫密碼 | - | **是** |

### API 環境變數

| 變數名稱 | 說明 | 預設值 | 必填 |
|---------|------|--------|------|
| `DATABASE_URL` | PostgreSQL 連線字串 | - | **是** |
| `ALLOWED_ORIGINS` | CORS 允許來源 | http://localhost:3000 | 否 |
| `MAX_WORKERS` | 分析併發數 | 20 | 否 |
| `MAX_API_WORKERS` | API 更新併發數 | 10 | 否 |
| `MAX_CONCENTRATION_WORKERS` | 籌碼分析併發數 | 10 | 否 |
| `ENABLE_CONCENTRATION` | 啟用籌碼分析 | true | 否 |
| `LOG_LEVEL` | 日誌等級 | INFO | 否 |
| `REQUIRE_AUTH` | 啟用身份驗證 | false | 否 |

### Frontend 環境變數

| 變數名稱 | 說明 | 預設值 | 必填 |
|---------|------|--------|------|
| `VITE_API_BASE_URL` | API 基礎 URL | http://localhost:8000 | 否 |
| `VITE_REQUIRE_AUTH` | 啟用身份驗證 | false | 否 |
| `VITE_GOOGLE_CLIENT_ID` | Google OAuth Client ID | - | 否* |

\* 僅在 `VITE_REQUIRE_AUTH=true` 時必填

## 常見操作

### 查看日誌

```bash
# 查看所有服務日誌
docker-compose logs -f

# 查看特定服務日誌
docker-compose logs -f api
docker-compose logs -f frontend
docker-compose logs -f postgres

# 查看最近 100 行日誌
docker-compose logs --tail=100 api
```

### 重啟服務

```bash
# 重啟所有服務
docker-compose restart

# 重啟特定服務
docker-compose restart api
```

### 停止服務

```bash
# 停止所有服務（保留資料）
docker-compose stop

# 停止並移除容器（保留資料卷）
docker-compose down

# 停止並移除容器和資料卷（⚠️ 會刪除資料庫）
docker-compose down -v
```

### 更新服務

```bash
# 拉取最新代碼
git pull origin main

# 重新建置映像
docker-compose build --no-cache

# 重啟服務
docker-compose up -d
```

### 進入容器 Shell

```bash
# 進入 API 容器
docker-compose exec api bash

# 進入 PostgreSQL 容器
docker-compose exec postgres psql -U twstock_user -d twstock

# 進入 Frontend 容器
docker-compose exec frontend sh
```

### 資料備份與還原

**備份資料庫**：

```bash
# 備份到本機檔案
docker-compose exec -T postgres pg_dump -U twstock_user twstock > backup_$(date +%Y%m%d).sql

# 壓縮備份
docker-compose exec -T postgres pg_dump -U twstock_user twstock | gzip > backup_$(date +%Y%m%d).sql.gz
```

**還原資料庫**：

```bash
# 從備份還原
docker-compose exec -T postgres psql -U twstock_user twstock < backup_20260115.sql

# 從壓縮備份還原
gunzip -c backup_20260115.sql.gz | docker-compose exec -T postgres psql -U twstock_user twstock
```

## 效能調校

### API 效能優化

**調整 Worker 數量**（修改 `.env`）：

```bash
# 高效能伺服器（8 核心以上）
MAX_WORKERS=50
MAX_API_WORKERS=20
MAX_CONCENTRATION_WORKERS=20

# 中階伺服器（4 核心）
MAX_WORKERS=20
MAX_API_WORKERS=10
MAX_CONCENTRATION_WORKERS=10

# 低階伺服器（2 核心）
MAX_WORKERS=10
MAX_API_WORKERS=5
MAX_CONCENTRATION_WORKERS=5
```

**調整 Uvicorn Workers**（修改 `docker/api/Dockerfile`）：

```dockerfile
# 根據 CPU 核心數調整，建議值 = (CPU核心數 × 2) + 1
CMD ["uvicorn", "api.main:app", "--host", "0.0.0.0", "--port", "8000", "--workers", "4"]
```

### PostgreSQL 效能優化

**調整連線池大小**（修改 `docker-compose.yml`）：

```yaml
postgres:
  command:
    - postgres
    - -c
    - max_connections=100
    - -c
    - shared_buffers=256MB
    - -c
    - effective_cache_size=1GB
```

### Frontend 快取優化

Nginx 已配置靜態資源快取：
- JS/CSS: 1 年
- 圖片: 30 天
- Gzip 壓縮啟用

## 監控與健康檢查

### Docker Compose 健康檢查

```bash
# 檢查所有服務健康狀態
docker-compose ps

# 檢查特定服務健康狀態
docker inspect twstock-api --format='{{.State.Health.Status}}'
```

### API 健康檢查端點

```bash
# 基本健康檢查
curl http://localhost:8000/api/health

# 快取統計
curl http://localhost:8000/api/cache/stats
```

### 資源使用監控

```bash
# 即時資源使用
docker stats

# 特定容器資源使用
docker stats twstock-api twstock-postgres twstock-frontend
```

## 疑難排解

### 問題：容器無法啟動

**檢查日誌**：

```bash
docker-compose logs api
docker-compose logs postgres
```

**常見原因**：
1. 資料庫連線失敗 → 檢查 `DATABASE_URL` 是否正確
2. 埠號衝突 → 檢查 8000/3000/5432 埠是否被占用
3. 權限問題 → 確認 Docker 有足夠權限

### 問題：前端無法連接後端

**檢查 Nginx 代理配置**：

```bash
# 進入前端容器
docker-compose exec frontend sh

# 測試後端連線
wget -O- http://api:8000/api/health
```

**檢查 CORS 設定**：

確認 `.env` 中的 `ALLOWED_ORIGINS` 包含前端域名。

### 問題：資料庫連線失敗

**檢查 PostgreSQL 狀態**：

```bash
docker-compose exec postgres pg_isready -U twstock_user
```

**重置資料庫密碼**：

```bash
docker-compose exec postgres psql -U twstock_user -d twstock -c "ALTER USER twstock_user WITH PASSWORD 'new_password';"
```

更新 `.env` 中的 `DATABASE_URL` 密碼。

### 問題：記憶體不足

**增加 Docker 記憶體限制**：

Docker Desktop → Settings → Resources → Memory → 增加到 8GB

**減少 Worker 數量**（修改 `.env`）：

```bash
MAX_WORKERS=10
MAX_API_WORKERS=5
MAX_CONCENTRATION_WORKERS=5
```

### 問題：TA-Lib 相關錯誤

確認 Dockerfile 使用正確的 ta-lib 版本：

```dockerfile
# docker/api/Dockerfile 應包含
RUN pip install --no-cache-dir poetry==2.2.1 poetry-plugin-export==1.9.0
```

並且 `pyproject.toml` 中：

```toml
ta-lib = "^0.6.8"
```

## 生產環境部署建議

### 1. 使用 HTTPS

建議使用 Nginx 反向代理 + Let's Encrypt SSL 憑證：

```nginx
server {
    listen 443 ssl http2;
    server_name yourdomain.com;

    ssl_certificate /etc/letsencrypt/live/yourdomain.com/fullchain.pem;
    ssl_certificate_key /etc/letsencrypt/live/yourdomain.com/privkey.pem;

    location / {
        proxy_pass http://localhost:3000;
    }

    location /api/ {
        proxy_pass http://localhost:8000;
    }
}
```

### 2. 設定自動備份

建立 cron job 定期備份資料庫：

```bash
# 每天凌晨 2 點備份
0 2 * * * cd /path/to/twstock && docker-compose exec -T postgres pg_dump -U twstock_user twstock | gzip > /backups/twstock_$(date +\%Y\%m\%d).sql.gz
```

### 3. 啟用日誌輪替

修改 `docker-compose.yml`：

```yaml
services:
  api:
    logging:
      driver: "json-file"
      options:
        max-size: "10m"
        max-file: "3"
```

### 4. 使用環境變數管理敏感資訊

不要將 `.env` 檔案提交到 Git：

```bash
# 確認 .env 在 .gitignore 中
echo ".env" >> .gitignore
```

### 5. 監控與告警

建議使用監控工具：
- **Prometheus + Grafana**: 系統監控
- **Sentry**: 錯誤追蹤
- **Uptime Robot**: 服務可用性監控

## 版本資訊

- **Python**: 3.12
- **Poetry**: 2.2.1
- **FastAPI**: 0.115.14
- **PostgreSQL**: 16
- **Node.js**: 18
- **React**: 18.3.1
- **Nginx**: 1.25
- **TA-Lib**: 0.6.8

## 支援

如遇到問題，請查看：
- API 文件: http://localhost:8000/docs
- GitHub Issues: <repository-url>/issues
- 專案 README: README.md

## 授權

請參閱 LICENSE 檔案。
