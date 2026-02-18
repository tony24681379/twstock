
# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## 🚨 費用警示 - 最高優先級

**鐵律：所有服務必須免費，這是不可違反的基本規則。**

在進行任何基礎設施變更前，**必須**先閱讀 `@COST_WARNING.md`：
- 檢查所有可能產生費用的資源
- 確認配置在免費額度內
- 執行 Terraform 前必須檢查 plan 輸出

**違反費用鐵律的任何變更都必須立即回退。**

相關文件：
- `COST_WARNING.md` - 完整的費用警示和檢查清單
- `terraform/environments/prod.tfvars` - 基礎設施配置（必須符合免費額度）
- `docs/GCP_DEPLOYMENT.md` - GCP 生產環境部署指南（包含 Terraform 最佳實踐和常見問題解決）

## Project Overview

This is **twstock** - a Taiwan Stock Market data fetcher library written in Python. The project fetches real-time and historical stock data from Taiwan Stock Exchange (TWSE) and Taipei Exchange (TPEX).

**專案類型**：Full-stack web application with Python backend and React frontend

**部署環境**：
- **本地開發**：Docker Compose (PostgreSQL + FastAPI + React)
- **生產環境**：GCP Cloud Run (API) + Firebase Hosting (Frontend)
  - 外部服務：Neon PostgreSQL + Upstash Redis
  - 域名：https://twstock.changes.live
  - 基礎設施：Terraform 管理

### Documentation Language Preference

**用中文記錄就好** - When creating or updating project documentation, use Traditional Chinese (繁體中文) as the primary language:
- User-facing documentation (README.md, tutorials, guides)
- Code comments explaining Taiwan stock market specific logic
- OpenSpec proposals and specs (see `openspec/AGENTS.md` for details)
- Git commit messages (optional but encouraged)

Keep technical terms in English:
- Programming language names (Python, TypeScript)
- Framework and library names (FastAPI, Next.js, PostgreSQL, pandas)
- Technical concepts (async/await, ORM, API)
- Code identifiers (class/function/variable names)

Example of good documentation style:
```python
# Good: 計算五日均價，使用 pandas rolling window
def calculate_moving_average(prices: list, days: int = 5) -> list:
    """
    計算移動平均線

    Args:
        prices: 收盤價列表
        days: 計算天數（預設 5 天)

    Returns:
        移動平均值列表
    """
    return pd.Series(prices).rolling(window=days).mean().tolist()
```

## Development Setup

```bash
# Install dependencies using Poetry
poetry install

# Update stock codes (run when using for the first time)
python3 -m twstock -U
```

## Common Development Tasks

### Running Tests
```bash
# Run all tests
python3 -m unittest discover -s test

# Run a specific test file
python3 -m unittest test.test_stock
```

### CLI Commands
```bash
# Run complete analysis and generate Excel report (main.py)
poetry run python main.py

# Run correlation analysis
poetry run python analyze_correlation_advanced.py

# Legacy CLI commands (may not be updated)
twstock -b 2330 6223  # Check Best Four Point
twstock -s 2330 6223  # Get stock information
twstock -r 2330 2337 2409  # Get real-time stock info
twstock -U  # Update stock codes
```

### Environment Variables

```bash
# Database configuration
DATABASE_URL=postgresql+asyncpg://twstock_user:twstock_password123@localhost:5432/twstock

# Redis cache (optional, production only)
REDIS_URL=rediss://default:password@host:6379  # Upstash Redis with TLS

# Performance tuning
MAX_API_WORKERS=10          # API update concurrency (default: 10)
MAX_WORKERS=50              # Analysis concurrency (default: 20)
MAX_CONCENTRATION_WORKERS=10  # Chip concentration concurrency (default: 10)

# Feature flags
ENABLE_CONCENTRATION=true   # Enable chip concentration analysis (default: true)

# Authentication (optional)
REQUIRE_AUTH=false          # Enable Google OAuth 2.0 authentication (default: false)
ALLOWED_EMAILS=             # Email whitelist (comma-separated, empty = all authenticated users)
GOOGLE_CLIENT_ID=           # Google OAuth Client ID

# Data retention (production vs development)
ENVIRONMENT=development     # Environment: development (keep all) or production (cleanup old data)
DATA_RETENTION_DAYS=0       # 0 = keep all (dev), 250 = keep last 250 trading days (prod)
CLEANUP_VACUUM=false        # Run VACUUM after cleanup (prod only)

# CORS settings
ALLOWED_ORIGINS=http://localhost:3000,http://localhost:5173  # Frontend origins
```

## Architecture Overview

### Deployment Architecture

#### 本地開發環境 (Docker Compose)

```
┌─────────────┐     ┌─────────────┐     ┌──────────────┐
│   Browser   │────▶│   Nginx     │────▶│   FastAPI    │
│             │     │  (Frontend) │     │   (Backend)  │
└─────────────┘     └─────────────┘     └──────────────┘
                           │                     │
                           │                     ▼
                           │              ┌──────────────┐
                           │              │  PostgreSQL  │
                           │              │   (Docker)   │
                           │              └──────────────┘
                           ▼
                    React SPA (Vite)
```

**服務**：
- **postgres**：PostgreSQL 16-alpine（埠號 5432）
- **api**：FastAPI + Uvicorn（埠號 8000）
- **frontend**：React + Nginx（埠號 3000）

**部署指令**：
```bash
# 啟動所有服務
docker-compose up -d

# 查看日誌
docker-compose logs -f

# 停止服務
docker-compose down
```

**文件**：`docs/DEPLOYMENT.md`

#### 生產環境 (GCP)

```
用戶
  │
  ├─→ Firebase Hosting (twstock.changes.live)
  │   └─→ React SPA (Vite)
  │
  └─→ Cloud Run (asia-northeast1)
      └─→ FastAPI API
          ├─→ Upstash Redis (asia-east1)
          └─→ Neon PostgreSQL (AWS ap-southeast-1)
```

**基礎設施**：
- **IaC**：Terraform（模組化配置）
- **容器註冊**：GCP Artifact Registry
- **密鑰管理**：GCP Secret Manager
- **API 運算**：Cloud Run（無伺服器容器，自動擴展 0-3 實例）
- **前端託管**：Firebase Hosting（全球 CDN）
- **資料庫**：Neon PostgreSQL（免費 3GB，250 天保留策略）
- **快取**：Upstash Redis（免費 10k 命令/天）

**部署工具**：
```bash
# 後端部署（Docker + gcloud）
./scripts/deploy-gcp.sh

# 前端部署（Firebase）
cd dashboard && firebase deploy --only hosting

# 基礎設施變更（Terraform）
cd terraform && terraform apply -var-file=environments/prod.tfvars
```

**文件**：`docs/GCP_DEPLOYMENT.md`

**成本控制**：
- 所有服務保持在免費額度內（$0/月）✅
- Cloud Run: 200 萬請求/月（使用 <10 萬）
- Neon: 3GB 儲存（使用 ~800MB）
- Upstash: 10k 命令/天（使用 <5k）
- Firebase Hosting: 10GB/月（使用 <1GB）

### Core Components

- **Stock** (`twstock/stock.py`): Main class for fetching and managing individual stock data
  - Uses `WantgooFetcher` for data retrieval
  - Inherits from `Analytics` for analysis features
  - Stores data in PostgreSQL database

- **Analytics** (`twstock/analytics.py`): Base class providing analytical methods
  - Moving averages, bias ratios, pivot points
  - Best Four Point analysis for buy/sell signals

- **All** (`twstock/all.py`): Batch processing for multiple stocks
  - Handles concurrent data fetching with asyncio
  - Batch loading from PostgreSQL (避免 N+1 query)
  - Generates comprehensive Excel reports with technical analysis and chip concentration signals

- **WantgooFetcher** (`twstock/wantgoo.py`): HTTP client for fetching data from Wantgoo APIs
  - Uses httpx with HTTP/2 support for async requests
  - Auto-initializes headers with Playwright for authentication
  - Fetches stock info, daily price data, and chip concentration data
  - Auto-detects API latest date for smart caching

- **DatabaseManager** (`twstock/database.py`): PostgreSQL database layer
  - SQLAlchemy ORM with async support
  - Batch operations for high performance
  - Tables: stock_info, stock_daily, institutional_investors, major_investors, margin_trading, concentration_data
  - Smart update tracking to minimize API calls

### Data Storage

- **PostgreSQL Database**: Primary data storage
  - stock_info - Stock basic information
  - stock_daily - Daily OHLCV data
  - institutional_investors - Foreign/Trust/Dealer holdings
  - major_investors - Major trader data
  - margin_trading - Margin/Short data
  - concentration_data - Weekly chip concentration (moreThan400, moreThan1000, lessThan20)
  - stock_update_tracker - Tracks what needs updating

- **Excel files** - Generated daily reports (e.g., 20260103.xlsx)
  - 技術籌碼 sheet
  - 基本面 sheet
  - 極端漲跌 sheet
  - 每週籌碼變化 sheet (週變化量，漸層顏色標示)
  - 籌碼訊號 sheet (10 個訊號，含預期報酬、勝率)

### Key Features

#### Smart Caching System
- Checks API latest date on initialization
- Only updates stocks with outdated data (DB date < API date)
- Batch SQL queries (single query for all stocks)
- Performance: 2-3 min first run, <2 min subsequent runs

#### Chip Concentration Analysis (New!)
- Fetches weekly chip concentration data from Wantgoo API
- Tracks large holders (>400, >1000 shares) and retail investors (<20 shares)
- 10 automated signals: Perfect Structure, Continuous Buying, Distribution Warning, etc.
- Historical backtesting: Expected return, win rate for each signal
- Visual formatting: Gradient colors for concentration changes (0.2% threshold)

#### Performance Optimizations
- Batch API updates: Only update stocks needing refresh
- Batch DB loading: Single SQL query for all stocks
- Async processing: 10-50 concurrent workers
- Speed: ~24 stocks/sec for analysis, ~20 stocks/sec for chip concentration

#### Authentication System (Optional)

**技術**：Google OAuth 2.0 + JWT Token

**兩層驗證機制**：
1. **Cloud Run IAM**：`allow_unauthenticated = true`（允許 HTTP 請求到達）
2. **應用層驗證**：`REQUIRE_AUTH` 環境變數控制

**配置**（`api/auth.py`）：
- Token 驗證：`google.oauth2.id_token.verify_oauth2_token`
- Email 白名單：`ALLOWED_EMAILS` 環境變數（逗號分隔）
- 前端登入：`AuthContext.tsx` + Google One Tap

**啟用步驟**：
1. 取得 Google OAuth Client ID（Google Cloud Console）
2. 設定 Terraform：`require_auth = true`，`allowed_emails = "user@gmail.com"`
3. 部署後端和前端（環境變數會自動注入）

**文件**：`docs/AUTH_SETUP.md`、`docs/GOOGLE_OAUTH_SETUP.md`

#### Data Retention Policy

**策略**：分層保留，本地完整 vs 線上精簡

| 環境 | 保留策略 | 說明 |
|------|----------|------|
| **本地版** | 永久保留（0 天） | 支援長期回測和研究分析 |
| **線上版** | 250 個交易日 | 節省 Neon 免費額度（3GB） |

**清理範圍**：
- ✅ 清理：`stock_daily`, `institutional_investors`, `major_investors`, `margin_trading`, `concentration_data`, `stock_technical_indicators`（時間序列資料）
- ❌ 保留：`stock_info`, `stock_eps`, `stock_monthly_revenue`, `stock_dividend`（元數據）

**API 端點**：
```bash
# 查看資料統計
GET /api/maintenance/stats

# 查看清理配置
GET /api/maintenance/config

# 模擬清理（Dry Run）
POST /api/maintenance/cleanup
{"dry_run": true}

# 執行清理（僅生產環境）
POST /api/maintenance/cleanup
{"vacuum": true}
```

**自動化**：
- 建議每月 1-2 次清理（凌晨離峰時段）
- 可使用 cron job 或 GitHub Actions 定時觸發

**預估資料量**（線上版 250 天）：
- 時間序列資料：~660 MB
- 元數據：~20 MB
- 索引 + 系統表：~120 MB
- **總計**：~800 MB（遠低於 3GB 免費額度）✅

**文件**：`docs/DATA_RETENTION.md`

### Important Notes

- TWSE/Wantgoo has request limits - the system respects rate limits with semaphores
- The project uses async/await patterns extensively for concurrent operations
- Data is cached in PostgreSQL with smart invalidation
- First run takes ~10 min (fetching data), subsequent runs ~2 min (DB cache)

---

## 詳細架構說明

### 爬蟲架構 (WantgooFetcher)

**核心檔案**：
- `twstock/wantgoo.py` - HTTP 請求、API 調用
- `twstock/wantgoo_initializer.py` - Playwright header 獲取
- `twstock/fetcher_manager.py` - 全局單例管理

**Header 機制** ⭐ 關鍵：
- 使用 Playwright 無頭瀏覽器訪問 Wantgoo
- 攔截 API 請求提取 `x-client-signature` 和 `cookie`
- Fallback：Playwright 失敗時降級為基本 cookies（無 x-client-signature）
- 刷新：401/403 錯誤時自動重新獲取（最少間隔 5 分鐘）

**需要的 Headers**：
- `user-agent`, `cookie`, `x-client-signature`, `referer`

**11 個 API 端點**：
- 股票資訊、EPS、股利、K線、日線、三大法人、主力、融資融券、籌碼集中度、月營收、技術指標

**重要設計**：
- 資料庫儲存張數，API 回傳時計算百分比
- 計算公式：`百分比 = (張數 / (流通股數 / 1000)) * 100`

---

### 前端架構 (React Dashboard)

**技術棧**：
- React 18.3.1 + TypeScript + Vite
- Tailwind CSS + Lightweight Charts + Recharts

**目錄結構**：
- `components/` - 12 個組件（StockList, CandlestickChart, ChipsTabs, etc.）
- `pages/` - HomePage, StockDetailPage
- `lib/` - api.ts, indicators.ts

**核心功能**：
- 股票列表（自訂權重排序）
- K線圖（5 種技術指標）
- 籌碼 Tabs（三大法人/主力/融資融券/集中度）
- 籌碼歷史趨勢圖（12 週）
- 基本面分析（6 個 Tabs）

**關鍵組件**：
- `ChipsTabs.tsx` - 同時顯示張數和百分比
- `CandlestickChart.tsx` - Lightweight Charts K線圖
- `WeightControls.tsx` - 權重設定（localStorage 持久化）

---

### 後端 API 架構 (FastAPI)

**技術棧**：
- FastAPI + Uvicorn
- PostgreSQL + SQLAlchemy (async)
- SignalCacheManager (LRU + TTL)

**目錄結構**：
- `api/main.py` - 應用入口
- `api/routers/` - stocks.py, cache.py
- `api/services/` - stock_service.py (1284 行核心邏輯)
- `api/models/` - Pydantic 模型

**10 個 API 端點**：
- GET `/api/stocks` - 股票列表（支援自訂權重）
- GET `/api/stocks/{id}` - 股票詳情
- GET `/api/stocks/{id}/chart` - K線圖
- GET `/api/stocks/{id}/chips` - 籌碼數據
- GET `/api/stocks/{id}/history` - 籌碼歷史
- GET `/api/stocks/{id}/fundamental` - 基本面
- 4 個快取管理端點

**核心設計**：
- **批次查詢**：避免 N+1 問題（單一 SQL 查詢所有股票）
- **並行計算**：使用 `asyncio.gather` 並行計算訊號
- **快取系統**：LRU + TTL 1小時，提升 50x 性能
- **張數 vs 百分比**：查詢時根據流通股數即時計算百分比

**訊號計算**：
- 10 個籌碼訊號 (SignalService)
- 10 個基本面訊號 (FundamentalSignalService)
- 27 個技術訊號 (VectorizedSignalDetector)
- **完整定義見 [`docs/SIGNAL_DEFINITIONS.md`](docs/SIGNAL_DEFINITIONS.md)**（唯一權威來源，修改分數前必須先更新此文件）

---

## Chip Concentration Signals (10 Signals)

### Buy Signals (Positive Score)
1. **完美結構** (+20): Large holders↑ AND Super large holders↑ AND Retail↓ (same week)
   - Historical: +2.61% avg return, 60.2% win rate
2. **超大戶連買2週** (+15): moreThan1000 increases for 2+ consecutive weeks
3. **大戶連買3週** (+12): moreThan400 increases for 3+ consecutive weeks
4. **大戶急買** (+10): Single week moreThan400 increase >1%
5. **加速集中** (+10): Recent 5-week avg change > Last 5-week avg
6. **10週持續買** (+8): W1-W10 change >3%
7. **散戶連賣3週** (+8): lessThan20 decreases for 3+ consecutive weeks
8. **散戶恐慌** (+6): Single week lessThan20 decrease >1%

### Risk Warnings (Negative Score)
9. **出貨訊號** (-15): Large holders↓ AND Retail↑ (same week)
10. **散戶狂熱** (-10): lessThan20 increases for 3+ consecutive weeks

### Signal Strength Score
- Raw score = Sum of triggered signals
- Normalized: 0-100 scale
- 80-100: Strong buy
- 60-79: Buy
- 40-59: Watch
- 0-39: Risk warning

## Performance Data

### Execution Time Breakdown
```
First Run (~10 min):
- API update: 6-7 min (1000+ stocks)
- Chip concentration: 2-3 min (2600+ stocks)
- Stock analysis: 1-2 min (2700 stocks)

Subsequent Runs (~2 min):
- API update: <10 sec (only 30-50 stocks)
- Chip concentration: <5 sec (DB cache)
- Stock analysis: 1-2 min (2700 stocks)
```

### Database Performance
- Batch loading: 2700 stocks in <1 sec
- Single SQL query with JOINs (no N+1 problem)
- Smart update detection: Compares DB date with API latest date

## Excel Report Sheets

### 1. 技術籌碼 (Technical Analysis)
- All stocks with technical indicators
- Sorted by stock ID

### 2. 基本面 (Fundamentals)
- Stock basic information + selected technical data
- EPS, dividends, outstanding shares
- Sorted by stock ID

### 3. 極端漲跌 (Extreme Moves)
- Stocks with >9% gain or <-9% loss
- Filtered subset of technical analysis
- Sorted by stock ID

### 4. 每週籌碼變化 (Weekly Chip Changes)
- **Columns**: 股票代碼, 股票名稱, 收盤價, 最新日期, 資料週數
- **Changes**: W1→W2 through W9→W10 for 大戶/超大戶/散戶
- **Summary**: 大戶總變化, 散戶總變化
- **Color coding**: Gradient red for increases ≥0.2% (darker for consecutive increases)
- Sorted by stock ID

### 5. 籌碼訊號 (Chip Signals)
- **Columns**: 股票代碼, 股票名稱, 收盤價, 訊號強度, 訊號數量, 主要訊號
- **Performance**: 預期報酬(%), 歷史勝率(%), 風險等級, 訊號品質
- **Data**: 大戶10週變化, 散戶10週變化, 最新大戶占比, 最新散戶占比
- **Color coding**: Green/Yellow/Red based on signal strength
- Sorted by stock ID

## CI/CD & Deployment

### GitHub Actions Workflows

**文件位置**：`.github/workflows/`

#### 1. deploy-api.yml - 後端自動部署
- **觸發**：Push to `main` branch（修改 `api/**`, `twstock/**`, `docker/api/**`）
- **流程**：
  1. 構建 Docker 映像（`--platform linux/amd64`）
  2. 推送到 GCP Artifact Registry
  3. 部署到 Cloud Run（`gcloud run deploy`）
- **密鑰**：`GCP_SA_KEY`（Service Account JSON）

#### 2. deploy-frontend.yml - 前端自動部署
- **觸發**：Push to `main` branch（修改 `dashboard/**`）
- **流程**：
  1. 安裝依賴（`npm ci`）
  2. 構建 Vite 應用（環境變數注入）
  3. 部署到 Firebase Hosting
- **密鑰**：`FIREBASE_TOKEN`

#### 3. terraform.yml - 基礎設施驗證
- **觸發**：PR 或 Push（修改 `terraform/**`）
- **流程**：
  1. Terraform format check（`terraform fmt`）
  2. Terraform validate（`terraform validate`）
  3. Terraform plan（預覽變更）
- **注意**：不會自動 apply，需手動執行部署

### Manual Deployment Scripts

**文件位置**：`scripts/`

| 腳本 | 用途 | 使用時機 |
|------|------|----------|
| `deploy-gcp.sh` | 完整 GCP 部署流程 | 初次部署或大版本更新 |
| `setup-secrets.sh` | 設定 Secret Manager | 環境變數變更 |
| `test-deployment.sh` | 驗證部署結果 | 部署後檢查 |
| `cleanup-gcp.sh` | 清理 GCP 資源 | 除錯或重新部署 |
| `test_data_retention.sh` | 測試資料保留策略 | 驗證清理邏輯 |

**使用範例**：
```bash
# 完整部署到 GCP
./scripts/deploy-gcp.sh

# 僅更新 secrets
./scripts/setup-secrets.sh

# 驗證部署
./scripts/test-deployment.sh
```

### Terraform Infrastructure Management

**目錄結構**：
```
terraform/
├── main.tf                    # 主配置（組合各模組）
├── variables.tf               # 變數定義
├── outputs.tf                 # 輸出定義
├── versions.tf                # Provider 版本
├── environments/
│   └── prod.tfvars           # 生產環境變數（成本控制配置）
└── modules/
    ├── artifact/             # Artifact Registry
    ├── cloud-run/            # Cloud Run 服務
    ├── secrets/              # Secret Manager（含 lifecycle 管理）
    ├── storage/              # Cloud Storage
    └── firebase-hosting/     # Firebase Hosting 配置
```

**關鍵配置**（`environments/prod.tfvars`）：
```hcl
# 成本控制（必須符合免費額度）
api_cpu           = "1"         # CPU 限制
api_memory        = "768Mi"     # 記憶體限制
api_min_instances = 0           # 最小實例（必須為 0）
api_max_instances = 3           # 最大實例

# 認證設定
require_auth     = false        # 啟用 Google OAuth
allowed_emails   = ""           # Email 白名單

# 資料保留
data_retention_days = 250       # 保留 250 個交易日
cleanup_vacuum      = true      # 清理後執行 VACUUM
```

**執行 Terraform**：
```bash
cd terraform

# 初始化
terraform init

# 檢查計劃（執行前必須檢查）
terraform plan -var-file=environments/prod.tfvars

# 套用變更（確認無費用風險後執行）
terraform apply -var-file=environments/prod.tfvars
```

**重要注意事項**：
- ⚠️ 執行前必須閱讀 `@COST_WARNING.md`
- ⚠️ Secret Manager 使用 `lifecycle.ignore_changes` 避免不必要的更新
- ⚠️ 所有配置必須保持在免費額度內（費用鐵律）

### Docker Image Build (本地測試)

**API 映像**：
```bash
# 構建（必須使用 linux/amd64 架構）
docker build \
  --platform linux/amd64 \
  -t asia-northeast1-docker.pkg.dev/twstock-484714/twstock-docker/api:latest \
  -f docker/api/Dockerfile .

# 推送到 Artifact Registry
docker push asia-northeast1-docker.pkg.dev/twstock-484714/twstock-docker/api:latest
```

**Frontend 映像**：
```bash
# 構建
docker build \
  --build-arg VITE_API_BASE_URL=https://api-url \
  -t twstock-frontend:latest \
  -f docker/frontend/Dockerfile .
```

**常見錯誤**：
- ❌ 在 Apple Silicon Mac 上未指定 `--platform` 會導致 Cloud Run `exec format error`
- ✅ 必須明確指定 `--platform linux/amd64`

## Troubleshooting

### Issue: "No module named 'pandas'"
**Solution**: Use `poetry run python main.py` instead of `python main.py`

### Issue: Database connection error
**Solution**:
1. Ensure PostgreSQL is running: `./start-db.sh`
2. Check DATABASE_URL in environment variables

### Issue: Playwright headers initialization fails
**Solution**: Headers will fall back to basic cookies. The system will still work but may have slightly lower success rate.

### Issue: API rate limiting
**Solution**: Reduce MAX_API_WORKERS to 5-10 to respect rate limits

### Issue: Memory usage high
**Solution**: Reduce MAX_WORKERS to 20-30 for lower memory consumption

### Issue: Cloud Run 容器啟動失敗 - exec format error
**症狀**: `ERROR: failed to load /usr/local/bin/uvicorn: exec format error`
**原因**: Docker 映像架構不匹配（在 Apple Silicon 上構建了 ARM 映像）
**Solution**: 構建時明確指定 `--platform linux/amd64`

### Issue: 資料庫連線失敗 - database does not exist
**症狀**: `asyncpg.exceptions.InvalidCatalogNameError: database "neondb\n" does not exist`
**原因**: DATABASE_URL secret 結尾包含換行符號 `\n`
**Solution**:
```bash
# 使用 echo -n（無換行符號）重新建立 secret
echo -n "postgresql://..." | gcloud secrets versions add database_url \
  --project=twstock-484714 \
  --data-file=-
```

### Issue: Redis 連線失敗 - Connection closed by server
**症狀**: `Redis 連線失敗: Connection closed by server.`
**原因**:
1. REDIS_URL 包含換行符號
2. 使用 `redis://` 而非 `rediss://`（Upstash 需要 TLS）
**Solution**: 使用正確的 TLS URL 和 `echo -n`
```bash
echo -n "rediss://default:password@host:6379" | \
  gcloud secrets versions add redis_url --project=twstock-484714 --data-file=-
```

### Issue: Terraform 一直要更新 secrets
**症狀**: `Plan: 2 to add, 0 to change, 2 to destroy` - secrets 持續重建
**原因**: Terraform 無法比較 sensitive 值，每次都認為需要更新
**Solution**: 在 `terraform/modules/secrets/main.tf` 加入 `lifecycle.ignore_changes`
```terraform
resource "google_secret_manager_secret_version" "database_url" {
  lifecycle {
    ignore_changes = [secret_data]
  }
}
```

### Issue: 前端無法連接 API - CORS 錯誤
**症狀**: 瀏覽器 Console 顯示 CORS 錯誤
**原因**: `ALLOWED_ORIGINS` 環境變數未包含前端域名
**Solution**: 更新 `terraform/environments/prod.tfvars`
```hcl
allowed_origins = "https://twstock.changes.live,https://twstock-484714.web.app,http://localhost:5173"
```

### Issue: Google OAuth 登入失敗
**症狀**: 登入後回傳 401/403 錯誤
**原因**:
1. Client ID 不匹配（前後端使用不同的 Client ID）
2. Email 不在白名單中
3. Token 驗證失敗
**Solution**:
1. 確認前後端 `GOOGLE_CLIENT_ID` 一致
2. 檢查 `ALLOWED_EMAILS` 是否包含該 Email（逗號分隔，無空格）
3. 檢查 Google Cloud Console OAuth 設定的授權網域

---

## 相關文件

### 部署相關
- `docs/DEPLOYMENT.md` - Docker Compose 本地部署指南
- `docs/GCP_DEPLOYMENT.md` - GCP 生產環境部署指南（Terraform 最佳實踐、常見問題）
- `COST_WARNING.md` - 費用警示和檢查清單（最高優先級）

### 認證與安全
- `docs/AUTH_SETUP.md` - Google OAuth 2.0 身份驗證設定
- `docs/GOOGLE_OAUTH_SETUP.md` - Google OAuth Client ID 申請流程

### 訊號定義
- `docs/SIGNAL_DEFINITIONS.md` - 訊號定義與評分標準（唯一權威來源）

### 資料管理
- `docs/DATA_RETENTION.md` - 資料保留策略（本地 vs 線上）
- `README_POSTGRES.md` - PostgreSQL 資料庫架構說明

### OpenSpec（實驗性功能）
- `openspec/AGENTS.md` - OpenSpec 代理系統說明
- `openspec/` - 變更提案和規格文件

### 腳本工具
- `scripts/deploy-gcp.sh` - GCP 完整部署腳本
- `scripts/setup-secrets.sh` - Secret Manager 設定
- `scripts/test-deployment.sh` - 部署驗證腳本
- `scripts/cleanup-gcp.sh` - GCP 資源清理
- `scripts/test_data_retention.sh` - 資料保留測試

### 配置文件
- `terraform/environments/prod.tfvars` - 生產環境基礎設施配置
- `.env.production.example` - 生產環境變數範例
- `docker-compose.yml` - 本地開發環境配置
- `docker-compose.prod.yml` - 線上版 Docker 配置（Neon + Upstash）

---

## 專案維護者

- **Primary**: Tony
- **AI Assistant**: Claude Code (claude.ai/code)
- **Last Updated**: 2026-02-15
