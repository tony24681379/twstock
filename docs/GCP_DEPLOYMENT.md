# TWStock GCP 生產環境部署指南

本文件記錄 TWStock 專案在 Google Cloud Platform (GCP) 的生產環境部署經驗與最佳實踐。

## 部署架構

```
用戶
  │
  ├─→ Firebase Hosting (twstock.changes.live)
  │   └─→ React SPA (Vite)
  │
  └─→ Cloud Run (asia-northeast1)
      └─→ FastAPI API
          ├─→ Upstash Redis (快取)
          └─→ Neon PostgreSQL (資料庫)
```

## 技術棧

### 基礎設施
- **IaC**: Terraform
- **容器註冊**: GCP Artifact Registry
- **密鑰管理**: GCP Secret Manager
- **運算平台**: Cloud Run (無伺服器容器)
- **前端託管**: Firebase Hosting
- **資料庫**: Neon PostgreSQL (AWS ap-southeast-1)
- **快取**: Upstash Redis (GCP asia-east1)

### 部署工具
- **Docker**: 容器構建
- **gcloud CLI**: GCP 命令列工具
- **firebase CLI**: Firebase 部署工具
- **Terraform**: 基礎設施管理

## 環境資訊

- **GCP 專案 ID**: twstock-484714
- **區域**: asia-northeast1 (東京)
- **Cloud Run 服務**: twstock-api
- **Firebase 專案**: twstock-484714

## 前置準備

### 1. 安裝必要工具

```bash
# macOS 使用 Homebrew
brew install google-cloud-sdk
brew install terraform
brew install docker
npm install -g firebase-tools

# 驗證安裝
gcloud --version
terraform --version
docker --version
firebase --version
```

### 2. GCP 認證

```bash
# 登入 GCP
gcloud auth login

# 設定專案
gcloud config set project twstock-484714

# 配置 Docker 認證
gcloud auth configure-docker asia-northeast1-docker.pkg.dev
```

### 3. Firebase 認證

```bash
firebase login
```

## Terraform 基礎設施配置

### 目錄結構

```
terraform/
├── main.tf                    # 主配置檔
├── variables.tf               # 變數定義
├── outputs.tf                 # 輸出定義
├── versions.tf                # Provider 版本
├── environments/
│   └── prod.tfvars           # 生產環境變數
└── modules/
    ├── artifact/             # Artifact Registry
    ├── cloud-run/            # Cloud Run 服務
    ├── secrets/              # Secret Manager
    ├── storage/              # Cloud Storage
    └── firebase-hosting/     # Firebase Hosting
```

### 重要配置：Secrets 的 Lifecycle 管理

⚠️ **關鍵修正**：防止 Terraform 不必要的 secret 更新

在 `terraform/modules/secrets/main.tf` 中，必須加入 `lifecycle.ignore_changes` 規則：

```terraform
resource "google_secret_manager_secret_version" "database_url" {
  secret      = google_secret_manager_secret.database_url.id
  secret_data = var.database_url

  lifecycle {
    ignore_changes = [secret_data]
  }
}

resource "google_secret_manager_secret_version" "redis_url" {
  secret      = google_secret_manager_secret.redis_url.id
  secret_data = var.redis_url

  lifecycle {
    ignore_changes = [secret_data]
  }
}
```

**原因**：
- Terraform 無法比較 sensitive 值（secret_data）
- 每次 `terraform plan` 都會嘗試重建 secret versions
- 加入 `ignore_changes` 後，Terraform 只在首次建立 secret，後續不會更新

### 環境變數設定

從 Secret Manager 載入環境變數（避免暴露敏感資料）：

```bash
# 從 Secret Manager 讀取
export DATABASE_URL=$(gcloud secrets versions access latest \
  --secret="database_url" \
  --project=twstock-484714)

export REDIS_URL=$(gcloud secrets versions access latest \
  --secret="redis_url" \
  --project=twstock-484714)
```

### 執行 Terraform

```bash
cd terraform

# 初始化
terraform init

# 檢查計劃
terraform plan \
  -var="database_url=$DATABASE_URL" \
  -var="redis_url=$REDIS_URL" \
  -var-file="environments/prod.tfvars"

# 執行部署
terraform apply \
  -var="database_url=$DATABASE_URL" \
  -var="redis_url=$REDIS_URL" \
  -var-file="environments/prod.tfvars"
```

## Docker 映像構建與部署

### 1. 構建 Docker 映像

⚠️ **重要**：必須構建 **AMD64 架構**的映像，因為 Cloud Run 使用 x86_64 平台。

```bash
# 取得 Git commit SHA 作為標籤
IMAGE_TAG=$(git rev-parse --short HEAD)

# 構建 AMD64 架構的映像
docker build \
  --platform linux/amd64 \
  -t asia-northeast1-docker.pkg.dev/twstock-484714/twstock-docker/api:latest \
  -t asia-northeast1-docker.pkg.dev/twstock-484714/twstock-docker/api:$IMAGE_TAG \
  -f docker/api/Dockerfile .
```

**常見錯誤**：
- ❌ 在 Apple Silicon (ARM) Mac 上構建未指定 `--platform` 會導致 `exec format error`
- ✅ 必須明確指定 `--platform linux/amd64`

### 2. 推送映像到 Artifact Registry

```bash
docker push asia-northeast1-docker.pkg.dev/twstock-484714/twstock-docker/api:latest
docker push asia-northeast1-docker.pkg.dev/twstock-484714/twstock-docker/api:$IMAGE_TAG
```

### 3. 部署到 Cloud Run

```bash
gcloud run services update twstock-api \
  --region=asia-northeast1 \
  --image=asia-northeast1-docker.pkg.dev/twstock-484714/twstock-docker/api:latest \
  --quiet
```

**預期輸出**：
```
Service [twstock-api] revision [twstock-api-00001-xxx] has been deployed
Service URL: https://twstock-api-647618039886.asia-northeast1.run.app
```

## Secret Manager 最佳實踐

### 常見問題：換行符號導致連線失敗

⚠️ **重要發現**：使用 `echo` 建立 secret 會在結尾加入換行符號 `\n`，導致資料庫/Redis 連線失敗。

**錯誤範例**：
```bash
# ❌ 會在結尾加入換行符號
echo "postgresql://user:pass@host/db" | gcloud secrets versions add database_url ...
```

**正確做法**：
```bash
# ✅ 使用 echo -n（無換行符號）
echo -n "postgresql://user:pass@host/db" | gcloud secrets versions add database_url \
  --project=twstock-484714 \
  --data-file=-
```

### 驗證 Secret 內容

檢查 secret 是否包含隱藏字元：

```bash
# 使用 od -c 顯示所有字元（包括換行符號）
gcloud secrets versions access latest \
  --secret="database_url" \
  --project=twstock-484714 | od -c

# 正常輸出應該在結尾沒有 \n
```

### Redis URL 格式

Upstash Redis 需要使用 **TLS 連線** (`rediss://` 而非 `redis://`)：

```bash
# ❌ 錯誤：使用 redis:// 會導致 "Connection closed by server"
redis://default:password@host:6379

# ✅ 正確：使用 rediss:// 進行 TLS 連線
rediss://default:password@host:6379
```

## 前端部署到 Firebase Hosting

### 1. 設定環境變數

```bash
# 取得 Cloud Run API URL
API_URL=$(gcloud run services describe twstock-api \
  --region=asia-northeast1 \
  --format='value(status.url)')

# 設定前端環境變數
export VITE_API_BASE_URL=$API_URL
export VITE_REQUIRE_AUTH=true
export VITE_GOOGLE_CLIENT_ID="647618039886-tu8om4vec6cma6kfnjen7v89fcquc2t6.apps.googleusercontent.com"
```

### 2. 構建前端

```bash
cd dashboard

# 安裝依賴
npm install

# 構建（環境變數會被嵌入到構建產物）
VITE_API_BASE_URL=$API_URL \
VITE_REQUIRE_AUTH=true \
VITE_GOOGLE_CLIENT_ID="647618039886-tu8om4vec6cma6kfnjen7v89fcquc2t6.apps.googleusercontent.com" \
npm run build
```

### 3. 部署到 Firebase

```bash
firebase deploy --only hosting --project=twstock-484714
```

**預期輸出**：
```
✔ Deploy complete!
Hosting URL: https://twstock-484714.web.app
```

## 驗證部署

### 1. API 健康檢查

```bash
curl https://twstock-api-647618039886.asia-northeast1.run.app/api/health
```

**預期輸出**：
```json
{
  "status": "healthy",
  "database": "connected",
  "redis": "connected",
  "version": "1.0.0"
}
```

### 2. 測試 API 端點

```bash
# 取得股票列表
curl "https://twstock-api-647618039886.asia-northeast1.run.app/api/stocks?limit=5"

# 取得特定股票
curl "https://twstock-api-647618039886.asia-northeast1.run.app/api/stocks/2330"
```

### 3. 測試前端

開啟瀏覽器訪問：
- https://twstock-484714.web.app
- https://twstock.changes.live（如已設定自訂域名）

## 常見問題與解決方案

### 問題 1：Terraform 一直要更新 secrets

**症狀**：
```
Plan: 2 to add, 0 to change, 2 to destroy
- module.secrets.google_secret_manager_secret_version.database_url must be replaced
- module.secrets.google_secret_manager_secret_version.redis_url must be replaced
```

**原因**：Terraform 無法比較 sensitive 值，每次都認為需要更新。

**解決方案**：在 secret version 資源中加入 `lifecycle.ignore_changes`（參見上方 Terraform 配置）。

### 問題 2：Cloud Run 容器啟動失敗 - exec format error

**症狀**：
```
ERROR: failed to load /usr/local/bin/uvicorn: exec format error
```

**原因**：Docker 映像架構不匹配（ARM vs AMD64）。

**解決方案**：構建時明確指定 `--platform linux/amd64`。

### 問題 3：資料庫連線失敗 - database "neondb" does not exist

**症狀**：
```
asyncpg.exceptions.InvalidCatalogNameError: database "neondb
" does not exist
```

**原因**：DATABASE_URL secret 結尾包含換行符號 `\n`，導致資料庫名稱被解析為 `"neondb\n"`。

**解決方案**：使用 `echo -n` 重新建立 secret（參見上方 Secret Manager 最佳實踐）。

### 問題 4：Redis 連線失敗 - Connection closed by server

**症狀**：
```
Redis 連線失敗: Connection closed by server.
```

**原因**：
1. REDIS_URL 包含換行符號（同問題 3）
2. 使用 `redis://` 而非 `rediss://`（Upstash 需要 TLS）

**解決方案**：
```bash
# 使用正確的 TLS URL 和 echo -n
echo -n "rediss://default:password@host:6379" | \
  gcloud secrets versions add redis_url --project=twstock-484714 --data-file=-
```

### 問題 5：前端無法連接 API - CORS 錯誤

**症狀**：瀏覽器 Console 顯示 CORS 錯誤。

**原因**：`ALLOWED_ORIGINS` 環境變數未包含前端域名。

**解決方案**：更新 Terraform 配置（`terraform/environments/prod.tfvars`）：
```terraform
allowed_origins = "https://twstock.changes.live,https://twstock-484714.web.app,http://localhost:5173"
```

## 回滾策略

### 回滾 API

```bash
# 列出所有版本
gcloud run revisions list \
  --service=twstock-api \
  --region=asia-northeast1

# 回滾到特定版本
gcloud run services update-traffic twstock-api \
  --to-revisions=twstock-api-00001-xxx=100 \
  --region=asia-northeast1
```

### 回滾前端

```bash
# Firebase 自動保留版本歷史
firebase hosting:rollback
```

### 回滾基礎設施

```bash
cd terraform
git checkout <previous-commit>
terraform apply -auto-approve
```

## 成本控制

**目標**：所有服務保持在免費額度內（$0/月）

| 服務 | 免費額度 | 預估使用 | 狀態 |
|------|----------|----------|------|
| Cloud Run | 200 萬請求/月 | <10 萬 | ✅ |
| Cloud Storage | 5GB | <100MB | ✅ |
| Artifact Registry | 0.5GB | <500MB | ✅ |
| Secret Manager | 6 個 secrets | 2 個 | ✅ |
| Neon PostgreSQL | 0.5GB, 1 專案 | 350MB | ✅ |
| Upstash Redis | 10k 命令/天 | <5k | ✅ |
| Firebase Hosting | 10GB/月 | <1GB | ✅ |

**關鍵配置**（`terraform/environments/prod.tfvars`）：
```terraform
api_cpu           = "1"      # 降低 CPU 避免超支
api_memory        = "768Mi"  # 記憶體控制在合理範圍
api_min_instances = 0        # 必須為 0（按需啟動）
api_max_instances = 3        # 限制最大實例數
```

## 監控與日誌

### Cloud Run 監控

```bash
# 查看日誌
gcloud run services logs read twstock-api \
  --region=asia-northeast1 \
  --limit=50

# 查看錯誤日誌
gcloud run services logs read twstock-api \
  --region=asia-northeast1 \
  --log-filter="severity>=ERROR" \
  --limit=20
```

### GCP Console 監控

訪問 [Cloud Run 控制台](https://console.cloud.google.com/run/detail/asia-northeast1/twstock-api/metrics?project=twstock-484714) 查看：
- 請求數
- 延遲
- 錯誤率
- CPU/記憶體使用

## 安全性最佳實踐

1. ✅ **不提交敏感資料**
   - `.env.production` 在 `.gitignore` 中
   - 所有敏感資料存放在 Secret Manager

2. ✅ **使用 Secret Manager**
   - DATABASE_URL 和 REDIS_URL 通過 Secret Manager 注入
   - Cloud Run 自動掛載 secrets

3. ✅ **啟用身份驗證**（可選）
   - `REQUIRE_AUTH=true` 啟用 Google OAuth
   - `ALLOWED_EMAILS` 白名單限制存取

4. ✅ **CORS 限制**
   - 只允許特定域名跨域請求
   - 避免使用 `*` 萬用字元

5. ✅ **最小權限原則**
   - Service Account 只授予必要權限
   - IAM bindings 精確控制存取

## 部署檢查清單

部署前檢查：
- [ ] Terraform 配置已更新 `lifecycle.ignore_changes`
- [ ] Secrets 使用 `echo -n` 建立（無換行符號）
- [ ] Redis URL 使用 `rediss://`（TLS 連線）
- [ ] Docker 映像使用 `--platform linux/amd64`
- [ ] `prod.tfvars` 配置在免費額度內
- [ ] CORS 設定包含所有前端域名

部署後驗證：
- [ ] API 健康檢查返回 `"status": "healthy"`
- [ ] Database 狀態為 `"connected"`
- [ ] Redis 狀態為 `"connected"`
- [ ] 前端頁面正常載入
- [ ] 前端可以呼叫 API
- [ ] CORS 設定正確（無跨域錯誤）

## 參考資源

- [Cloud Run 文件](https://cloud.google.com/run/docs)
- [Firebase Hosting 文件](https://firebase.google.com/docs/hosting)
- [Terraform GCP Provider](https://registry.terraform.io/providers/hashicorp/google/latest/docs)
- [Neon PostgreSQL](https://neon.tech/docs)
- [Upstash Redis](https://docs.upstash.com/redis)

## 部署歷史

- **2026-02-01**: 首次生產環境部署
  - 修復 Terraform lifecycle 問題
  - 修復 Secret Manager 換行符號問題
  - 修復 Docker 架構不匹配問題
  - 修復 Redis TLS 連線問題
  - 部署版本：twstock-api-00006-74p

---

**維護者**: Claude Code + Tony
**最後更新**: 2026-02-01
