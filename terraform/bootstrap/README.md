# Terraform Backend Bootstrap

此目錄包含創建 Terraform state backend（GCS bucket）的配置。

## 重要說明：使用免費區域

**此配置使用 `us-central1` 區域享受 GCP Always Free 層級：**

- ✅ 5 GB 標準存儲（完全免費）
- ✅ 5,000 次 Class A 操作/月（寫入）
- ✅ 50,000 次 Class B 操作/月（讀取）
- ✅ Terraform state 通常 < 1 MB，完全在免費額度內

**為什麼不影響應用性能：**
- State bucket 只存放 Terraform 配置狀態
- 實際用戶訪問的服務仍在 `asia-east1`（台灣）
- 只有開發者執行 `terraform` 命令時才會訪問 state bucket
- 跨區域延遲（~150ms）對 Terraform 操作影響可忽略

## 用途

在使用主要的 Terraform 配置之前，需要先執行此 bootstrap 配置來創建：
- GCS bucket 用於存放 Terraform state（位於 us-central1）
- 啟用必要的 GCP API

## 執行步驟

### 1. 初始化 Terraform

```bash
cd terraform/bootstrap
terraform init
```

### 2. 查看執行計劃

```bash
terraform plan
```

預期會創建：
- 1 個 GCS bucket: `twstock-484714-terraform-state`
- 啟用 Storage API

### 3. 執行部署

```bash
terraform apply
```

輸入 `yes` 確認執行。

### 4. 驗證 Bucket

```bash
# 使用 gcloud 查看 bucket
gcloud storage buckets describe gs://twstock-484714-terraform-state

# 或使用 gsutil
gsutil ls -L -b gs://twstock-484714-terraform-state
```

## 完成後

Bootstrap 完成後：

1. **更新主要配置**：編輯 `terraform/versions.tf`，啟用 backend 配置：
   ```hcl
   backend "gcs" {
     bucket = "twstock-484714-terraform-state"
     prefix = "terraform/state"
   }
   ```

2. **初始化主要配置**：
   ```bash
   cd ..  # 回到 terraform/ 目錄
   terraform init
   ```

3. **遷移現有 state**（如果有）：
   ```bash
   terraform init -migrate-state
   ```

## Bucket 配置

創建的 bucket 具有以下特性：

- **名稱**: `twstock-484714-terraform-state`
- **位置**: `us-central1` 🆓（享受 GCP 免費層級）
- **版本控制**: 已啟用（保留歷史版本）
- **訪問控制**: 統一 bucket 級別控制
- **生命週期**: 保留最近 10 個版本
- **防刪除**: 啟用（`force_destroy = false`）

### 成本分析

使用 `us-central1` 的免費層級：

**Storage（存儲）：**
- Terraform state 大小：約 5-10 KB（初始）→ 100 KB（完整部署）
- 免費額度：5 GB = 5,000,000 KB
- **使用率：< 0.01%** ✅

**Operations（操作）：**
- 每次 `terraform plan/apply`：約 2-3 次讀取 + 1 次寫入
- 假設每天執行 10 次：300 次讀取/月 + 100 次寫入/月
- 免費額度：50,000 次讀取 + 5,000 次寫入
- **使用率：< 2%** ✅

**結論：完全在免費層級內，無需付費** 💰

## 安全性注意事項

### Bucket 權限

Bootstrap 執行完成後，建議檢查 bucket 權限：

```bash
# 查看 IAM 綁定
gcloud storage buckets get-iam-policy gs://twstock-484714-terraform-state
```

預設只有專案管理員和運行 Terraform 的服務帳號可以訪問。

### 狀態加密

State 文件使用 Google 管理的加密金鑰自動加密。如需使用客戶管理的金鑰（CMEK），可以修改 `main.tf` 中的 `encryption` 區塊。

## 清理

⚠️ **警告**：刪除 bucket 會導致 Terraform state 遺失！

如果需要清理 bootstrap 資源：

```bash
# 1. 先移除 force_destroy 保護
terraform apply -var="force_destroy=true"

# 2. 執行銷毀
terraform destroy
```

## 故障排除

### 問題：Bucket 名稱已存在

如果 bucket 名稱已被使用，修改 `variables.tf` 中的 bucket 名稱，或刪除現有 bucket。

### 問題：API 未啟用

執行前確保已啟用 Storage API：
```bash
gcloud services enable storage.googleapis.com --project=twstock-484714
```

### 問題：權限不足

確保執行 Terraform 的帳號具有以下權限：
- `storage.buckets.create`
- `storage.buckets.update`
- `serviceusage.services.enable`

或具有以下角色：
- Storage Admin (`roles/storage.admin`)
- Service Usage Admin (`roles/serviceusage.serviceUsageAdmin`)
