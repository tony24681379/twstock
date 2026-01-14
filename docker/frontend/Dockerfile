# ========================================
# Stage 1: Builder
# ========================================
FROM node:18-alpine AS builder

WORKDIR /build

# 接收建置參數
ARG VITE_API_BASE_URL=http://localhost:8000
ARG VITE_REQUIRE_AUTH=false
ARG VITE_GOOGLE_CLIENT_ID=""

# 複製 package 檔案（利用 Docker layer cache）
COPY dashboard/package*.json ./

# 安裝依賴（包含 devDependencies，建置需要）
RUN npm ci

# 複製原始碼
COPY dashboard/ .

# 設定環境變數
ENV NODE_ENV=production \
    NODE_OPTIONS="--max-old-space-size=2048" \
    VITE_API_BASE_URL=${VITE_API_BASE_URL} \
    VITE_REQUIRE_AUTH=${VITE_REQUIRE_AUTH} \
    VITE_GOOGLE_CLIENT_ID=${VITE_GOOGLE_CLIENT_ID}

# 建置前端應用
RUN npm run build

# ========================================
# Stage 2: Nginx Runtime
# ========================================
FROM nginx:1.25-alpine

# 複製建置後的靜態檔案
COPY --from=builder /build/dist /usr/share/nginx/html

# 複製自訂 Nginx 配置（從 context 根目錄）
COPY docker/frontend/nginx.conf /etc/nginx/conf.d/default.conf

# 設定檔案權限
RUN chown -R nginx:nginx /usr/share/nginx/html && \
    chmod -R 755 /usr/share/nginx/html

# 暴露 HTTP 埠
EXPOSE 80

# 健康檢查
HEALTHCHECK --interval=30s --timeout=10s --start-period=5s --retries=3 \
    CMD wget --quiet --tries=1 --spider http://localhost:80 || exit 1

# 啟動 Nginx
CMD ["nginx", "-g", "daemon off;"]
