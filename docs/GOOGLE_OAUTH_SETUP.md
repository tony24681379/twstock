# Google OAuth 2.0 設定指南

本文件說明如何為台股籌碼集中度儀表板設定 Google OAuth 2.0 身份驗證。

## 📋 前置需求

- Google 帳號
- 專案已部署或在本地開發環境運行

## 🚀 設定步驟

### 步驟 1：前往 Google Cloud Console

1. 開啟瀏覽器，前往 [Google Cloud Console](https://console.cloud.google.com/)
2. 使用你的 Google 帳號登入

### 步驟 2：建立或選擇專案

#### 選項 A：建立新專案
1. 點擊頁面頂部的專案下拉選單
2. 點擊「新增專案」
3. 輸入專案名稱，例如：`twstock-dashboard`
4. （可選）選擇組織
5. 點擊「建立」
6. 等待專案建立完成（約 10-30 秒）

#### 選項 B：使用現有專案
1. 點擊頁面頂部的專案下拉選單
2. 選擇要使用的專案

### 步驟 3：啟用 Google+ API（可選）

> **注意**：新版 Google Identity 可能不需要啟用 API，可跳過此步驟。如果後續設定出現問題再回來啟用。

1. 在左側選單點擊「API 和服務」→「程式庫」
2. 搜尋「Google+ API」
3. 點擊進入 API 詳情頁
4. 點擊「啟用」按鈕

### 步驟 4：設定 OAuth 同意畫面

1. 在左側選單點擊「API 和服務」→「OAuth 同意畫面」
2. 選擇使用者類型：
   - **外部**：任何 Google 帳號都可以使用（推薦用於測試）
   - **內部**：僅限組織內部使用者（需要 Google Workspace）
3. 點擊「建立」

#### 填寫應用程式資訊

**第 1 步：OAuth 同意畫面**
- **應用程式名稱**：`台股籌碼集中度儀表板`
- **使用者支援電子郵件**：選擇你的 email
- **應用程式標誌**：（可選）上傳 logo
- **應用程式首頁**：`http://localhost:5173`（開發環境）
- **應用程式隱私權政策連結**：（可選）
- **應用程式服務條款連結**：（可選）
- **已授權網域**：（開發環境可留空）
- **開發人員聯絡資訊**：輸入你的 email

點擊「儲存並繼續」

**第 2 步：範圍（Scopes）**
- 點擊「新增或移除範圍」
- 選擇以下範圍（或直接點擊「儲存並繼續」使用預設）：
  - `userinfo.email`
  - `userinfo.profile`
  - `openid`
- 點擊「更新」
- 點擊「儲存並繼續」

**第 3 步：測試使用者**（僅限外部應用程式）
- 如果應用程式處於「測試」狀態，需要新增測試使用者
- 點擊「+ ADD USERS」
- 輸入要允許登入的 Google 帳號 email
- 點擊「新增」
- 點擊「儲存並繼續」

**第 4 步：摘要**
- 檢查設定
- 點擊「返回資訊主頁」

### 步驟 5：建立 OAuth 2.0 憑證

1. 在左側選單點擊「API 和服務」→「憑證」
2. 點擊頁面頂部「+ 建立憑證」
3. 選擇「OAuth 用戶端 ID」

#### 設定 OAuth 用戶端

1. **應用程式類型**：選擇「網頁應用程式」

2. **名稱**：輸入識別名稱，例如：`TW Stock Dashboard - Dev`

3. **已授權的 JavaScript 來源**：
   - 點擊「+ 新增 URI」
   - 開發環境：`http://localhost:5173`
   - 生產環境：`https://yourdomain.com`（如果有的話）

   > **重要**：必須是完整的 URL，包含 protocol（http/https）

4. **已授權的重新導向 URI**：
   - 點擊「+ 新增 URI」
   - 開發環境：`http://localhost:5173`
   - （可選）也可以加入：`http://localhost:5173/login`

   > **注意**：使用 @react-oauth/google 時，重新導向由套件處理，通常只需要首頁 URI

5. 點擊「建立」

### 步驟 6：取得憑證資訊

憑證建立後，會出現對話框顯示：

- **用戶端 ID**：類似 `123456789012-abcdefghijklmnopqrstuvwxyz123456.apps.googleusercontent.com`
- **用戶端密鑰**：（後端不需要，前端也不需要）

**重要**：複製「用戶端 ID」，這就是我們需要的 `GOOGLE_CLIENT_ID`！

### 步驟 7：配置環境變數

#### 後端配置 (api/.env)

```bash
# 身份驗證開關
REQUIRE_AUTH=true

# Google OAuth Client ID
GOOGLE_CLIENT_ID=your-client-id-here.apps.googleusercontent.com

# Email 白名單（逗號分隔，允許登入的使用者）
ALLOWED_EMAILS=your-email@gmail.com,another-user@gmail.com
```

#### 前端配置 (dashboard/.env)

```bash
# 身份驗證開關（需與後端一致）
VITE_REQUIRE_AUTH=true

# Google OAuth Client ID（與後端相同）
VITE_GOOGLE_CLIENT_ID=your-client-id-here.apps.googleusercontent.com

# API 後端網址
VITE_API_BASE_URL=http://localhost:8000
```

### 步驟 8：重新啟動服務

```bash
# 停止現有服務（如果正在運行）
# 按 Ctrl+C 或使用 kill 指令

# 重新啟動後端
cd /Users/tony/twstock
poetry run uvicorn api.main:app --reload --port 8000

# 重新啟動前端（新開終端）
cd /Users/tony/twstock/dashboard
npm run dev
```

### 步驟 9：測試登入功能

1. 開啟瀏覽器前往 http://localhost:5173
2. 應該會自動重導向到 `/login` 登入頁面
3. 點擊「使用 Google 帳號登入」按鈕
4. 選擇 Google 帳號
5. 同意授權（第一次會顯示同意畫面）
6. 登入成功後會重導向回首頁

## ⚠️ 常見問題

### 問題 1：「此應用程式尚未通過 Google 驗證」警告

**原因**：應用程式處於測試狀態

**解決方案**：
- 點擊「繼續」或「進階」→「前往應用程式」
- 這是正常的，因為應用程式還在開發階段
- 正式部署時可以提交 Google 審查以移除此警告

### 問題 2：「redirect_uri_mismatch」錯誤

**原因**：重新導向 URI 設定不正確

**解決方案**：
1. 確認瀏覽器 URL 與設定的 URI 完全一致
2. 回到 Google Cloud Console → 憑證 → 編輯 OAuth 用戶端
3. 確保「已授權的 JavaScript 來源」和「已授權的重新導向 URI」包含當前使用的 URL
4. 儲存後等待 1-2 分鐘讓變更生效

### 問題 3：Email 不在白名單

**錯誤訊息**：`Email 'xxx@gmail.com' is not authorized to access this resource`

**解決方案**：
1. 檢查後端 `.env` 的 `ALLOWED_EMAILS` 設定
2. 確保使用的 Google 帳號 email 在白名單中
3. 多個 email 用逗號分隔，不要有空格
4. 重新啟動後端服務

### 問題 4：測試使用者限制（外部應用程式）

**原因**：應用程式處於測試狀態，每次只能新增最多 100 個測試使用者

**解決方案**：
- 在 OAuth 同意畫面新增測試使用者
- 或者發布應用程式（需要 Google 審查）

## 📊 開發 vs 生產環境

### 開發環境
```bash
# 已授權的 JavaScript 來源
http://localhost:5173

# 已授權的重新導向 URI
http://localhost:5173
```

### 生產環境
```bash
# 已授權的 JavaScript 來源
https://yourdomain.com
https://www.yourdomain.com

# 已授權的重新導向 URI
https://yourdomain.com
https://www.yourdomain.com
```

> **重要**：生產環境必須使用 HTTPS！

## 🔐 安全建議

1. **保護 Client ID**：雖然 Client ID 不是機密，但不應公開在 Git repository
2. **使用環境變數**：所有敏感設定都應使用環境變數
3. **白名單管理**：定期檢查並更新 email 白名單
4. **定期審查**：定期檢查 Google Cloud Console 的活動記錄

## 📚 相關連結

- [Google Cloud Console](https://console.cloud.google.com/)
- [Google Identity 官方文件](https://developers.google.com/identity)
- [@react-oauth/google 套件文件](https://www.npmjs.com/package/@react-oauth/google)
- [OAuth 2.0 說明](https://oauth.net/2/)

## 🎯 快速檢查清單

設定完成後，確認以下項目：

- [ ] Google Cloud 專案已建立
- [ ] OAuth 同意畫面已設定
- [ ] OAuth 2.0 憑證已建立
- [ ] 用戶端 ID 已複製
- [ ] JavaScript 來源已正確設定
- [ ] 重新導向 URI 已正確設定
- [ ] 後端 `.env` 已配置 GOOGLE_CLIENT_ID
- [ ] 前端 `.env` 已配置 VITE_GOOGLE_CLIENT_ID
- [ ] ALLOWED_EMAILS 已設定測試帳號
- [ ] REQUIRE_AUTH 已設為 true
- [ ] 服務已重新啟動
- [ ] 登入功能測試成功

---

如有任何問題，請參考常見問題章節或查閱 Google Identity 官方文件。
