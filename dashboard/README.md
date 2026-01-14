# 台股籌碼集中度儀表板 - 前端

React + TypeScript + Vite + Tailwind CSS

## 開發環境設置

### 1. 安裝相依套件

```bash
cd dashboard
npm install
```

### 2. 設定環境變數

複製 `.env.example` 為 `.env`：

```bash
cp .env.example .env
```

編輯 `.env`：
```bash
VITE_API_BASE_URL=http://localhost:8000
VITE_REQUIRE_AUTH=false
```

### 3. 啟動開發伺服器

```bash
npm run dev
```

開啟瀏覽器：http://localhost:5173

## 建置正式版本

```bash
npm run build
```

建置結果在 `dist/` 目錄，可部署至任何靜態網頁伺服器。

## 技術堆疊

- **React 18** - UI 框架
- **TypeScript** - 型別安全
- **Vite** - 建置工具
- **Tailwind CSS** - 樣式框架
- **React Router v6** - 路由
- **Recharts** - 圖表庫

## 專案結構

```
dashboard/
├── src/
│   ├── components/     # React 元件
│   │   └── Layout.tsx
│   ├── pages/          # 頁面元件
│   │   ├── HomePage.tsx
│   │   └── StockDetailPage.tsx
│   ├── lib/            # 工具函式
│   │   └── api.ts      # API 客戶端
│   ├── types/          # TypeScript 類型定義
│   │   └── stock.ts
│   ├── App.tsx         # 主應用元件
│   ├── main.tsx        # 入口檔案
│   └── index.css       # 全域樣式
├── index.html
├── package.json
├── vite.config.ts
├── tsconfig.json
└── tailwind.config.js
```

## 環境變數

| 變數名稱 | 說明 | 預設值 |
|---------|------|--------|
| `VITE_API_BASE_URL` | 後端 API 網址 | `http://localhost:8000` |
| `VITE_REQUIRE_AUTH` | 是否需要登入 | `false` |
| `VITE_GOOGLE_CLIENT_ID` | Google OAuth Client ID | - |

## 部署

### Docker 部署（推薦）

完整部署指南請參閱 [docs/DEPLOYMENT.md](../docs/DEPLOYMENT.md)

```bash
# 1. 設定環境變數
cp .env.production.example .env
# 編輯 .env 檔案，修改密碼與網域

# 2. 建置並啟動所有服務（PostgreSQL + API + Frontend）
docker-compose build
docker-compose up -d

# 3. 檢查服務狀態
docker-compose ps

# 4. 訪問前端
open http://localhost:3000
```

### Docker 映像資訊

- **基礎映像**: node:18-alpine（建置）+ nginx:1.25-alpine（運行）
- **建置方式**: 多階段建置（builder + runtime）
- **映像大小**: ~25MB
- **關鍵特性**:
  - Vite 建置優化
  - Gzip 壓縮（節省 60-70% 傳輸）
  - SPA 路由支援（try_files fallback）
  - API 代理到後端（/api/ → http://api:8000）
  - 靜態資源快取（JS/CSS 1年、圖片 30天）

### 記憶體問題解決方案

如果建置時遇到 exit code 137（記憶體不足），專案已設定記憶體優化：

```bash
# package.json 已包含記憶體優化腳本
npm run build:prod  # 使用 NODE_OPTIONS="--max-old-space-size=2048"
```

Docker 建置會自動使用此優化設定。

### 傳統部署方式

#### Nginx（手動部署）

```bash
# 1. 建置（使用記憶體優化）
npm run build:prod

# 2. 複製到 nginx 靜態目錄
cp -r dist/* /var/www/html/

# 3. nginx.conf 設定 SPA fallback
location / {
  try_files $uri $uri/ /index.html;
}

# 4. 啟用 Gzip 壓縮
gzip on;
gzip_types text/plain text/css application/javascript application/json;

# 5. 設定靜態資源快取
location ~* \.(js|css|png|jpg|jpeg|gif|ico|svg|woff|woff2)$ {
  expires 1y;
  add_header Cache-Control "public, immutable";
}
```

#### GitHub Pages / Netlify / Vercel

直接連接 GitHub repository，設定建置命令：
- **Build command**: `npm run build:prod`
- **Publish directory**: `dist`
- **Environment variables**:
  - `VITE_API_BASE_URL`: 後端 API 網址（例如 `https://api.yourdomain.com`）
  - `VITE_REQUIRE_AUTH`: 是否需要登入（`true` 或 `false`）

### 生產環境注意事項

1. **必須修改的環境變數**：
   - `VITE_API_BASE_URL` - 修改為實際後端 API 網址
   - 確保後端 CORS 設定允許前端網域

2. **效能優化建議**：
   - 使用 CDN 加速靜態資源
   - 啟用 Gzip/Brotli 壓縮
   - 設定適當的 Cache-Control headers
   - 使用 HTTP/2

3. **監控與除錯**：
   - 檢查瀏覽器 Console 是否有錯誤
   - 檢查 Network tab 確認 API 請求正常
   - 確認 CORS 設定正確

### 疑難排解

常見問題請參閱 [docs/DEPLOYMENT.md - 疑難排解章節](../docs/DEPLOYMENT.md#疑難排解)

- 前端無法連接後端 → 檢查 VITE_API_BASE_URL 與 CORS 設定
- 建置時記憶體不足 → 使用 `npm run build:prod`
- 路由 404 錯誤 → 檢查 Nginx SPA fallback 設定
- 靜態資源載入失敗 → 檢查 base path 設定
