# 🚨 費用警示 - 鐵律：所有服務必須免費

## ⚠️ 當前會產生費用的資源

### 1. **Cloud Run API** - ✅ 已優化但仍需監控
**當前配置**：
- CPU: 1 核心 ✅
- Memory: 768Mi ✅（已從 1Gi 降低）
- Min instances: 0 ✅
- Max instances: 3 ✅

**免費額度**：
- 每月 2,000,000 次請求
- 180,000 vCPU-秒
- 360,000 GiB-秒（768Mi × 2秒/請求 = 1.5 GiB-秒/請求 → 可支援 240,000 次請求/月）
- 每月 1GB 網路傳輸

**狀態**：已優化，但如果月流量超過 24 萬次請求仍會產生費用
**注意**：512Mi 會導致 OOM，768Mi 是最小可用配置

### 2. **Artifact Registry** - ⚠️ 超出免費額度
**當前使用量**：1.1GB
**免費額度**：0.5GB
**預估費用**：~$0.06/月（$0.10/GB）

**建議**：
- 定期清理舊 Docker 映像
- 只保留最近 2-3 個版本

### 3. **PostgreSQL 資料庫** - ❓ 需要立即確認
**可能產生費用的服務**：
- ❌ Cloud SQL → $7-10/月（最小實例）
- ❌ AWS RDS → $12+/月
- ❌ 自建在 Compute Engine → $5+/月

**免費選項**：
- ✅ Neon Free Tier → 0.5GB 儲存，每月 191 小時計算時間
- ✅ Supabase Free → 500MB 儲存，無限 API 請求
- ✅ Railway Free → $5 免費額度/月
- ✅ ElephantSQL Free → 20MB（極小，不建議）

**立即檢查託管商**：
```bash
# 方法 1：透過 Secret Manager 查看（需要權限）
gcloud secrets versions access latest --secret="database_url" --project=twstock-484714

# 方法 2：從本地環境變數查看（如果有設定）
echo $DATABASE_URL

# 根據主機名稱判斷：
# - neon.tech → Neon（免費）
# - supabase.co → Supabase（免費）
# - railway.app → Railway（免費）
# - cloudsql → Cloud SQL（付費 ⚠️）
```

### 4. **Redis** - ❓ 需要立即確認
**可能產生費用的服務**：
- ❌ Cloud Memorystore → $12+/月（最小實例 1GB）
- ❌ AWS ElastiCache → $12+/月
- ❌ 自建在 Compute Engine → $5+/月

**免費選項**：
- ✅ Upstash Free → 10,000 次命令/天
- ✅ Redis Cloud Free → 30MB 儲存
- ✅ Railway Free → $5 免費額度/月

**立即檢查託管商**：
```bash
# 方法 1：透過 Secret Manager 查看（需要權限）
gcloud secrets versions access latest --secret="redis_url" --project=twstock-484714

# 方法 2：從本地環境變數查看（如果有設定）
echo $REDIS_URL

# 根據主機名稱判斷：
# - upstash.io → Upstash（免費）
# - redislabs.com / redis.io → Redis Cloud（可能免費）
# - railway.app → Railway（免費）
# - memorystore → Cloud Memorystore（付費 ⚠️）
```

**⚠️ 如果發現使用付費服務，必須立即遷移到免費層！**

---

## ✅ 當前免費的資源

### 1. **Firebase Hosting**
- 免費額度：10GB 儲存 + 360MB/天 傳輸
- 當前使用：極少（只有前端靜態檔案）
- 自定義域名：免費

### 2. **Cloud Storage**
- 免費額度：5GB（美國區域）
- 當前使用：862KB
- 狀態：✅ 遠低於免費額度

### 3. **Secret Manager**
- 免費額度：10,000 次訪問/月，6 個活躍版本
- 當前使用：2 個密鑰
- 狀態：✅ 完全免費

### 4. **Cloud Build**
- 免費額度：每天 120 分鐘建置時間
- 狀態：✅ 足夠使用

---

## 🛡️ 強制執行規則

### Terraform 配置必須遵守：

1. **禁止啟用 CDN/Load Balancer**
   ```hcl
   enable_frontend_cdn = false  # 必須為 false
   ```

2. **Cloud Run 必須使用最小配置**
   ```hcl
   api_cpu           = "1"      # 最多 1 核心
   api_memory        = "768Mi"  # 最小可用（512Mi 會 OOM）
   api_min_instances = 0        # 必須為 0（按需啟動）
   api_max_instances = 3        # 不超過 3 個實例
   ```

3. **資料庫必須使用免費層**
   - PostgreSQL: Neon Free Tier / Supabase Free / Railway Free
   - Redis: Upstash Free / Redis Cloud Free

4. **定期清理 Artifact Registry**
   ```bash
   # 每月執行一次，只保留最近 3 個版本
   gcloud artifacts docker images list \
     asia-northeast1-docker.pkg.dev/twstock-484714/twstock-docker/api \
     --sort-by=~CREATE_TIME --limit=999 | tail -n +4 | \
     awk '{print $1}' | xargs -I {} gcloud artifacts docker images delete {} --quiet
   ```

---

## 📊 費用監控

### 檢查當前費用：
```bash
# 查看當前月份費用
gcloud billing accounts list
gcloud billing projects describe twstock-484714

# 或訪問：https://console.cloud.google.com/billing
```

### 設定費用警報：
1. 前往 [Google Cloud Billing](https://console.cloud.google.com/billing)
2. 設定預算警報：當費用 > $1 時發送通知
3. 設定每日費用報告

---

## ⚡ 緊急停止步驟

如果發現費用產生，立即執行：

```bash
# 1. 停止 Cloud Run
gcloud run services update twstock-api \
  --region=asia-northeast1 \
  --min-instances=0 \
  --max-instances=0

# 2. 檢查並刪除 Load Balancer（如果誤建）
gcloud compute forwarding-rules list
gcloud compute forwarding-rules delete [NAME] --global

# 3. 檢查並刪除 Backend Buckets
gcloud compute backend-buckets list
gcloud compute backend-buckets delete [NAME]
```

---

## 📝 每次變更前必須檢查

執行 Terraform 前務必確認：

```bash
# 1. 檢查 plan 輸出，確認不會建立付費資源
terraform plan -var-file=environments/prod.tfvars | grep -E "(compute_|load_balancer|cdn)"

# 2. 確認配置正確
grep -E "(api_cpu|api_memory|enable_cdn)" environments/prod.tfvars

# 3. 確認沒有 Compute Engine 資源
terraform plan | grep "google_compute" | grep "will be created"
# 應該返回空結果
```

**如果看到任何 `google_compute_` 資源將被建立，立即停止！**

---

## 🔒 記住：這是鐵律

- ❌ 不允許任何每月固定費用
- ❌ 不允許超出免費額度
- ❌ 不允許建立 Compute Engine 資源
- ✅ 所有服務必須有免費層
- ✅ 所有配置必須在免費額度內
- ✅ 每次變更前必須檢查費用影響

**違反此鐵律的任何變更都必須立即回退。**
