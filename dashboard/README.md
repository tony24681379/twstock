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

### Nginx

```bash
# 建置
npm run build

# 複製到 nginx 靜態目錄
cp -r dist/* /var/www/html/

# nginx.conf 需設定 SPA fallback
location / {
  try_files $uri $uri/ /index.html;
}
```

### GitHub Pages / Netlify

直接連接 GitHub repository，設定建置命令：
- Build command: `npm run build`
- Publish directory: `dist`
