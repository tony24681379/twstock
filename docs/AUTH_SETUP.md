# 🔐 身份驗證設定指南

## 概述

本專案支援 Google OAuth 2.0 身份驗證，可以透過 email 白名單限制訪問權限。

## 架構說明

### 兩層驗證機制

1. **Cloud Run IAM 層**（`allow_unauthenticated = true`）
   - 允許 HTTP 請求到達 API 服務
   - 不處理應用層的身份驗證

2. **應用層驗證**（`REQUIRE_AUTH` 環境變數）
   - 由 `api/auth.py` 實作 Google OAuth token 驗證
   - 支援 email 白名單控制
   - 驗證失敗回傳 401/403 錯誤

## 設定步驟

### 1. 取得 Google OAuth Client ID

1. 前往 [Google Cloud Console](https://console.cloud.google.com/apis/credentials?project=twstock-484714)
2. 選擇專案：`twstock-484714`
3. 點選「建立憑證」→「OAuth 2.0 用戶端 ID」
4. 應用程式類型：選擇「網頁應用程式」
5. 設定允許的 JavaScript 來源：
   ```
   https://twstock.changes.live
   https://twstock-484714.web.app
   http://localhost:5173
   ```
6. 授權重新導向 URI（可選，使用 One Tap 不需要）：
   ```
   https://twstock.changes.live
   ```
7. 複製產生的 Client ID（格式：`xxxx.apps.googleusercontent.com`）

### 2. 設定白名單 Email

編輯 `terraform/environments/prod.tfvars`：

```hcl
# 啟用身份驗證
require_auth = true

# 設定 Google Client ID
google_client_id = "123456789-abc.apps.googleusercontent.com"

# 設定白名單（逗號分隔，無空格）
allowed_emails = "user1@gmail.com,user2@gmail.com,admin@company.com"
```

**重要**：
- Email 必須逗號分隔，不要有空格
- 空字串表示允許所有已驗證的 Google 帳號
- 建議只允許特定管理員訪問

### 3. 部署設定

```bash
cd terraform

# 1. 初始化 Terraform（如果還沒做過）
terraform init

# 2. 檢查變更內容
terraform plan -var-file=environments/prod.tfvars

# 3. 套用變更
terraform apply -var-file=environments/prod.tfvars

# 4. 確認環境變數已設定
gcloud run services describe twstock-api \
  --region=asia-northeast1 \
  --format='value(spec.template.spec.containers[0].env)'
```

### 4. 更新前端環境變數

前端也需要 Google Client ID。編輯 `dashboard/.env.production`：

```bash
# 前端環境變數（需要重新 build）
VITE_REQUIRE_AUTH=true
VITE_GOOGLE_CLIENT_ID=123456789-abc.apps.googleusercontent.com
VITE_API_BASE_URL=https://twstock-api-XXXX-an.a.run.app
```

重新建置並部署前端：

```bash
cd dashboard
npm run build
firebase deploy --only hosting
```

## 驗證設定

### 1. 檢查 API 驗證狀態

```bash
# 健康檢查端點（不需要驗證）
curl https://twstock-api-XXXX-an.a.run.app/api/health

# 檢查驗證配置狀態
curl https://twstock-api-XXXX-an.a.run.app/api/auth/status
```

回應範例：
```json
{
  "require_auth": true,
  "has_client_id": true,
  "whitelist_enabled": true,
  "whitelist_count": 3
}
```

### 2. 測試未驗證存取（應該失敗）

```bash
# 嘗試在不提供 token 的情況下存取 API
curl https://twstock-api-XXXX-an.a.run.app/api/stocks

# 預期回應：401 Unauthorized
{
  "detail": "Authentication required. Please login with your Google account."
}
```

### 3. 測試白名單外的 Email（應該失敗）

使用不在白名單中的 Google 帳號登入，預期錯誤：

```json
{
  "detail": "Email 'unauthorized@gmail.com' is not authorized to access this resource"
}
```

### 4. 測試白名單內的 Email（應該成功）

使用白名單內的帳號登入後，前端應該正常運作。

## 疑難排解

### 問題 1：前端登入後仍無法存取 API

**原因**：CORS 設定或 token 未正確傳遞

**解決方法**：
1. 檢查瀏覽器開發者工具 Network Tab
2. 確認請求 Header 包含 `Authorization: Bearer <token>`
3. 檢查 CORS 設定是否包含前端域名

### 問題 2：Token 驗證失敗

**原因**：Google Client ID 不匹配或 token 過期

**解決方法**：
1. 確認前後端使用相同的 Client ID
2. Token 有效期約 1 小時，過期需重新登入
3. 檢查 Google Cloud Console OAuth 設定

### 問題 3：白名單設定無效

**原因**：環境變數格式錯誤

**解決方法**：
1. Email 之間不要有空格：`user1@gmail.com,user2@gmail.com`
2. 重新部署 Cloud Run 確保環境變數更新
3. 檢查環境變數：
   ```bash
   gcloud run services describe twstock-api \
     --region=asia-northeast1 \
     --format='get(spec.template.spec.containers[0].env)'
   ```

## 關閉驗證（開發環境）

如果需要暫時關閉驗證（例如本地開發）：

```bash
# 本地 API
export REQUIRE_AUTH=false

# 本地前端
VITE_REQUIRE_AUTH=false
```

或在 `terraform/environments/prod.tfvars` 中設定：

```hcl
require_auth = false
```

**⚠️ 警告**：生產環境應該始終啟用驗證！

## 安全建議

1. ✅ 只允許必要的管理員 email
2. ✅ 定期檢查白名單，移除離職人員
3. ✅ 使用公司 Google Workspace 帳號（而非個人 Gmail）
4. ✅ 啟用 Google 兩步驟驗證
5. ✅ 定期輪換 Client ID（如果外洩）
6. ❌ 不要將 Client ID 或白名單 commit 到公開 Git repo

## 相關檔案

- `api/auth.py` - 身份驗證邏輯
- `dashboard/src/contexts/AuthContext.tsx` - 前端驗證 Context
- `dashboard/src/pages/LoginPage.tsx` - 登入頁面
- `terraform/modules/cloud-run/main.tf` - Cloud Run 環境變數設定
- `terraform/environments/prod.tfvars` - 生產環境配置
