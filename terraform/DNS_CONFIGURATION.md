# 🌐 Firebase Hosting 自定義域名 DNS 配置

## 配置狀態

- **域名**: twstock.changes.live
- **Firebase Site ID**: twstock-484714
- **證書狀態**: CERT_VALIDATING（正在驗證 SSL 證書）
- **證書類型**: TEMPORARY（臨時證書，驗證完成後會自動升級為永久證書）

---

## 📋 GoDaddy DNS 配置步驟

### 第一步：登入 GoDaddy DNS 管理

1. 訪問 [GoDaddy](https://www.godaddy.com/)
2. 登入你的帳戶
3. 前往 **My Products** > **DNS**
4. 找到域名 `changes.live`，點擊 **Manage DNS**

---

### 第二步：刪除舊的 A 記錄（如果存在）

在 DNS Records 區域，找到並刪除以下記錄：

| 類型 | 名稱 | 值 | 動作 |
|------|------|------|------|
| A | twstock | 151.101.1.195 | **刪除** |

**如何刪除**：
- 找到該記錄行
- 點擊右側的垃圾桶圖示 🗑️
- 確認刪除

---

### 第三步：添加 CNAME 記錄

在 DNS Records 區域，點擊 **Add** 按鈕，添加以下記錄：

| 類型 | 名稱 | 值 | TTL |
|------|------|------|-----|
| CNAME | twstock | twstock-484714.web.app | 600 秒 |

**填寫說明**：
- **Type**: 選擇 `CNAME`
- **Name**: 輸入 `twstock`（不包含 .changes.live）
- **Value/Points to**: 輸入 `twstock-484714.web.app`
- **TTL**: 選擇 `600 秒`（10 分鐘，方便測試）

**⚠️ 重要提示**：
- CNAME 和 A 記錄不能共存於同一個名稱
- 如果已有 `twstock` 的 CNAME 記錄，請先刪除再添加新的
- Name 欄位只輸入 `twstock`，系統會自動添加 `.changes.live`

---

### 第四步：保存變更

1. 確認所有配置正確
2. 點擊 **Save** 按鈕
3. 等待 DNS 傳播（通常 5-30 分鐘）

---

## 🔍 驗證 DNS 配置

### 方法 1：使用命令列工具

等待 5-10 分鐘後，執行以下命令：

```bash
# 檢查 CNAME 記錄
dig twstock.changes.live

# 預期輸出應包含：
# twstock.changes.live. 600 IN CNAME twstock-484714.web.app.
```

### 方法 2：使用線上工具

訪問以下網站檢查 DNS 傳播狀態：
- [DNS Checker](https://dnschecker.org/)
- [What's My DNS](https://www.whatsmydns.net/)

輸入 `twstock.changes.live` 並選擇 **CNAME** 類型，應該看到：
- Value: `twstock-484714.web.app`

---

## 🕐 等待時間估計

### DNS 傳播時間
- **GoDaddy 內部**：5-10 分鐘
- **全球 DNS 傳播**：10-60 分鐘
- **最長可能**：2 小時

### SSL 證書配置時間
- **DNS 驗證完成後**：5-15 分鐘
- **證書狀態**：CERT_PREPARING → CERT_VALIDATING → CERT_ACTIVE
- **最長可能**：24 小時（極少數情況）

---

## 🔐 SSL 證書狀態追蹤

### 檢查證書狀態

```bash
# 方法 1：使用 Terraform 查詢
cd terraform
terraform refresh -var-file=environments/prod.tfvars
terraform output firebase_custom_domain_status

# 方法 2：訪問 Firebase Console
# https://console.firebase.google.com/project/twstock-484714/hosting/sites
```

### 證書狀態說明

| 狀態 | 說明 | 預計時間 |
|------|------|---------|
| CERT_PREPARING | 準備 SSL 證書 | 1-5 分鐘 |
| CERT_VALIDATING | 正在驗證域名所有權 | 5-15 分鐘（需要 DNS 配置完成） |
| CERT_PROPAGATING | 證書正在傳播到全球節點 | 5-10 分鐘 |
| CERT_ACTIVE | SSL 證書已啟用 ✅ | - |

**當前狀態**: `CERT_VALIDATING`
- Firebase 正在等待 DNS CNAME 記錄生效
- DNS 配置完成後，會自動驗證並啟用 SSL

---

## ✅ 測試與驗證

### 當 SSL 證書狀態變為 CERT_ACTIVE 後

1. **測試 HTTPS 訪問**
   ```bash
   curl -I https://twstock.changes.live
   # 應返回 HTTP 200 OK
   ```

2. **瀏覽器測試**
   - 訪問 https://twstock.changes.live
   - 檢查地址欄是否有綠色鎖頭 🔒
   - 點擊鎖頭查看證書（應為 Let's Encrypt 簽發）

3. **測試 HTTP 重定向**
   ```bash
   curl -I http://twstock.changes.live
   # 應自動重定向到 HTTPS
   ```

4. **功能測試**
   - Google OAuth 登入
   - API 調用（檢查瀏覽器 Console 無 CORS 錯誤）
   - 前端路由（刷新頁面不會 404）

---

## 🚨 故障排除

### 問題 1：DNS 無法解析

**症狀**：`dig twstock.changes.live` 返回 `NXDOMAIN` 或找不到 CNAME

**解決方法**：
1. 確認 GoDaddy DNS 記錄已保存
2. 清除本地 DNS 快取：
   ```bash
   # Mac
   sudo dscacheutil -flushcache
   sudo killall -HUP mDNSResponder
   ```
3. 使用 Google DNS 測試：
   ```bash
   dig @8.8.8.8 twstock.changes.live
   ```
4. 等待更長時間（最多 2 小時）

### 問題 2：SSL 證書停留在 CERT_VALIDATING

**症狀**：證書狀態超過 30 分鐘仍為 CERT_VALIDATING

**可能原因**：
- DNS CNAME 記錄尚未傳播
- DNS 配置錯誤

**解決方法**：
1. 確認 DNS CNAME 已生效（使用 dig 命令）
2. 確認 CNAME 指向正確：`twstock-484714.web.app`
3. 等待 24 小時，Firebase 會自動重試
4. 如仍失敗，檢查是否有 CAA 記錄阻止 Let's Encrypt：
   ```bash
   dig CAA changes.live
   ```

### 問題 3：HTTPS 訪問顯示證書錯誤

**症狀**：瀏覽器顯示 "您的連線不是私人連線"

**可能原因**：
- SSL 證書尚未完全傳播
- 瀏覽器快取舊證書

**解決方法**：
1. 等待 10-15 分鐘
2. 清除瀏覽器快取
3. 使用無痕視窗測試
4. 檢查 Firebase Console 中的證書狀態

---

## 📞 需要協助？

如果遇到問題，請提供以下資訊：

```bash
# 1. DNS 查詢結果
dig twstock.changes.live

# 2. Firebase 證書狀態
cd terraform
terraform output firebase_custom_domain_status

# 3. Firebase DNS 檢查結果
terraform output firebase_dns_records
```

---

## 🎉 配置完成檢查清單

完成以下所有步驟後，配置即完成：

- [ ] 已在 GoDaddy 刪除舊的 A 記錄（151.101.1.195）
- [ ] 已在 GoDaddy 添加 CNAME 記錄（twstock → twstock-484714.web.app）
- [ ] DNS 查詢返回正確的 CNAME 記錄
- [ ] Firebase 證書狀態為 CERT_ACTIVE
- [ ] HTTPS 訪問 https://twstock.changes.live 正常顯示
- [ ] SSL 證書有效（綠色鎖頭）
- [ ] Google OAuth 登入正常
- [ ] API 功能正常（無 CORS 錯誤）
- [ ] 前端路由正常（刷新不會 404）

**全部完成後，自定義域名配置成功！** 🎊
