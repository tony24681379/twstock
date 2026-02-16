# ========================================
# Stage 1: Builder
# ========================================
FROM python:3.12-slim AS builder

WORKDIR /build

# 安裝系統依賴（psutil 等套件需要編譯）
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
        build-essential \
        ca-certificates && \
    rm -rf /var/lib/apt/lists/*

# 安裝 Poetry 2.3.2 和 export 插件（與本機環境一致）
RUN pip install --no-cache-dir poetry==2.3.2 poetry-plugin-export==1.10.0

# 複製依賴檔案（利用 Docker layer cache）
COPY pyproject.toml poetry.lock ./

# 匯出 requirements.txt（只匯出主要依賴，不包含開發依賴組）
RUN poetry export -f requirements.txt -o requirements.txt --only main --without-hashes || \
    poetry export -f requirements.txt -o requirements.txt --without-hashes

# 安裝 Python 依賴並建立 wheels
RUN pip wheel --no-cache-dir --wheel-dir /build/wheels -r requirements.txt

# ========================================
# Stage 2: Runtime
# ========================================
FROM python:3.12-slim

# 設定環境變數
ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    TZ=Asia/Taipei

# 安裝運行時依賴
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
        libpq5 \
        curl \
        ca-certificates && \
    rm -rf /var/lib/apt/lists/*

# 設定時區
RUN ln -snf /usr/share/zoneinfo/$TZ /etc/localtime && echo $TZ > /etc/timezone

# 建立非 root 使用者
RUN useradd -m -u 1000 -s /bin/bash appuser

# 設定工作目錄
WORKDIR /app

# 複製 wheels 並安裝 Python 依賴
COPY --from=builder /build/wheels /wheels
COPY --from=builder /build/requirements.txt .
RUN pip install --no-cache-dir /wheels/*.whl && \
    rm -rf /wheels

# 複製應用程式碼
COPY --chown=appuser:appuser . .

# 切換到非 root 使用者
USER appuser

# 暴露 API 埠
EXPOSE 8000

# 健康檢查
HEALTHCHECK --interval=30s --timeout=10s --start-period=40s --retries=3 \
    CMD curl -f http://localhost:8000/api/health || exit 1

# 啟動 FastAPI 應用（4 個 workers）
CMD ["uvicorn", "api.main:app", "--host", "0.0.0.0", "--port", "8000", "--workers", "4"]
