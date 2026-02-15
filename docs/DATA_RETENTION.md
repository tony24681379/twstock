# 資料保留策略

本文件說明線上版和本地版的資料保留策略及清理流程。

## 概述

為了節省線上版資料庫空間（Neon Free Tier 3GB），系統實施分層資料保留策略：

- **線上版（Production）**：保留最近 250 個交易日的歷史資料
- **本地版（Development）**：永久保留所有歷史資料

## 環境配置

### 本地版配置

```bash
# .env
ENVIRONMENT=development
DATA_RETENTION_DAYS=0  # 0 表示永久保留
CLEANUP_VACUUM=false
```

### 線上版配置

```bash
# .env.production
ENVIRONMENT=production
DATA_RETENTION_DAYS=250  # 保留 250 個交易日（約 10 個月實際交易日）
CLEANUP_VACUUM=true      # 清理後執行 VACUUM 釋放空間
```

## 清理範圍

### 會清理的表（時間序列資料）

| 表名 | 說明 | 預估大小（250天） |
|------|------|------------------|
| `stock_daily` | 每日 OHLCV 資料 | ~135 MB |
| `institutional_investors` | 三大法人買賣超 | ~135 MB |
| `major_investors` | 主力買賣超 | ~135 MB |
| `margin_trading` | 融資融券 | ~135 MB |
| `concentration_data` | 籌碼集中度（週資料） | ~20 MB |
| `stock_technical_indicators` | 技術指標預計算 | ~100 MB |

**總計**：約 660 MB

### 永久保留的表（元數據）

| 表名 | 說明 | 預估大小 |
|------|------|----------|
| `stock_info` | 股票基本資訊 | ~1 MB |
| `stock_eps` | EPS 財報 | ~5 MB |
| `stock_monthly_revenue` | 月營收 | ~10 MB |
| `stock_update_tracker` | 更新追蹤 | <1 MB |
| `stock_dividend` | 股利資料 | ~2 MB |

**總計**：約 20 MB

## API 使用

### 1. 查看資料統計

```bash
GET /api/maintenance/stats
```

**回應範例**：

```json
{
  "environment": "production",
  "retention_days": 250,
  "retention_enabled": true,
  "tables": {
    "stock_daily": {
      "total_rows": 675000,
      "earliest_date": "2025-03-01",
      "latest_date": "2026-01-31",
      "date_range_days": 250
    },
    "_database_info": {
      "size_bytes": 734003200,
      "size_mb": 700.0
    }
  }
}
```

### 2. 查看維護配置

```bash
GET /api/maintenance/config
```

**回應範例**：

```json
{
  "environment": "production",
  "retention_days": 250,
  "retention_enabled": true,
  "cleanup_vacuum": true,
  "description": "線上版 - 保留最近 250 天資料"
}
```

### 3. 模擬清理（Dry Run）

```bash
POST /api/maintenance/cleanup
Content-Type: application/json

{
  "dry_run": true
}
```

**回應範例**：

```json
{
  "status": "completed",
  "deleted_counts": {
    "stock_daily": 135000,
    "institutional_investors": 135000,
    "major_investors": 135000,
    "margin_trading": 135000,
    "concentration_data": 5400,
    "stock_technical_indicators": 67500
  },
  "retention_days": 250,
  "cutoff_date": "2025-04-25T00:00:00",
  "dry_run": true
}
```

### 4. 執行清理

```bash
POST /api/maintenance/cleanup
Content-Type: application/json

{
  "vacuum": true
}
```

**注意**：
- 本地環境（ENVIRONMENT=development）無法執行實際清理（會回傳 403 錯誤）
- 線上環境（ENVIRONMENT=production）可正常執行
- `vacuum=true` 會釋放磁碟空間但會鎖表，建議離峰時段執行

## 清理策略

### 執行頻率

建議每月執行 1-2 次清理：

```bash
# 每月 1 號執行清理
0 2 1 * * curl -X POST https://your-api.com/api/maintenance/cleanup \
  -H "Content-Type: application/json" \
  -d '{"vacuum": true}'
```

### 清理時機

**建議執行時間**：
- 週末或非交易日
- 凌晨 2:00-4:00（離峰時段）
- 避免在交易時間執行

**執行前檢查**：
1. 確認資料庫備份（如有需要）
2. 執行 dry_run 確認刪除數量
3. 檢查資料庫空間使用率
4. 確認無用戶正在使用系統

### 安全機制

1. **環境檢查**：非生產環境不允許清理
2. **Dry Run**：模擬執行查看刪除量
3. **手動觸發**：避免自動執行誤刪
4. **保留策略**：確保保留足夠歷史資料（250 天）

## 資料量估算

### 線上版（Neon 3GB）

假設 2700 支股票：

- **時間序列資料**（250 天）：660 MB
- **元數據**：20 MB
- **索引**：100 MB
- **PostgreSQL 系統表**：20 MB

**總計**：約 800 MB（遠低於 3GB 免費額度）✅

### 本地版（Docker PostgreSQL）

- **完整歷史資料**（假設 3 年）：約 8 GB
- **儲存空間**：由本地硬碟決定
- **無限制**：可支援長期回測和研究分析

## 清理流程

```
1. 檢查環境變數（ENVIRONMENT, DATA_RETENTION_DAYS）
   ↓
2. 計算截止日期（今天 - retention_days）
   ↓
3. [Dry Run] 統計將刪除的行數
   ↓
4. [實際清理] 執行 DELETE 語句
   ↓
5. [可選] 執行 VACUUM 釋放空間
   ↓
6. 回傳刪除統計資訊
```

## 監控指標

### 資料庫大小查詢

```sql
-- 查看資料庫總大小
SELECT pg_size_pretty(pg_database_size(current_database()));

-- 查看各表大小
SELECT
  schemaname,
  tablename,
  pg_size_pretty(pg_total_relation_size(schemaname||'.'||tablename)) AS size
FROM pg_tables
WHERE schemaname = 'public'
ORDER BY pg_total_relation_size(schemaname||'.'||tablename) DESC;

-- 查看最舊資料日期
SELECT
  'stock_daily' as table_name,
  MIN(date) as earliest_date,
  MAX(date) as latest_date,
  (MAX(date) - MIN(date)) as date_range
FROM stock_daily;
```

### 關鍵指標

定期檢查：
- 資料庫大小是否接近 3GB
- 最舊資料日期是否符合預期（250 天前）
- 各表行數是否合理
- Neon 計算時間使用量（191 小時/月）

## 故障排除

### 問題：本地環境無法清理

**錯誤訊息**：
```
403 Forbidden: 資料清理只允許在生產環境執行
```

**解決方案**：
- 本地環境預設永久保留資料（DATA_RETENTION_DAYS=0）
- 如需測試清理功能，使用 `dry_run=true`
- 或設定 `ENVIRONMENT=production` 進行測試

### 問題：VACUUM 鎖表導致查詢超時

**解決方案**：
- 設定 `vacuum=false` 跳過 VACUUM
- 或在離峰時段執行
- 使用 `VACUUM` 而非 `VACUUM FULL`（但釋放空間較少）

### 問題：刪除數量異常

**檢查步驟**：
1. 確認 `DATA_RETENTION_DAYS` 設定正確
2. 執行 dry_run 查看將刪除的資料量
3. 檢查資料庫日期是否正確（時區問題）

## 備份建議

雖然本系統採用「直接刪除不保留」策略，但建議：

1. **Neon 自動備份**：Neon 提供 7 天自動備份（免費版）
2. **定期導出**：每月導出重要資料到 CSV（可選）
3. **本地版備份**：本地版保留完整歷史資料作為備份

## 後續優化

1. **自動化清理**：新增定時任務每週自動執行
2. **通知功能**：清理完成後發送 Slack/Email 通知
3. **清理日誌**：記錄每次清理的詳細資訊到資料庫
4. **歸檔功能**：清理前導出到 S3/GCS（如有需要）
5. **分區表**：使用 PostgreSQL 分區表提升清理效能

## 注意事項

### ⚠️ 資料安全

- 清理前確認 retention_days 設定正確（建議 250 天）
- 本地版永久保留避免誤刪
- Neon 提供 7 天備份可恢復（免費版）

### 💡 效能優化

- `VACUUM FULL` 會鎖表，建議離峰執行
- Neon 有計算時間限制（191 小時/月）
- 避免頻繁清理，建議每月 1-2 次

### 📊 成本控制

- 250 天資料約 800 MB，遠低於 3GB 免費額度 ✅
- 定期監控資料庫大小
- 確保所有服務維持免費額度內運作（費用鐵律）

## 相關文件

- [COST_WARNING.md](../COST_WARNING.md) - 費用警示和檢查清單
- [README_POSTGRES.md](../README_POSTGRES.md) - PostgreSQL 資料庫說明
- [DEPLOYMENT.md](./DEPLOYMENT.md) - 部署指南
