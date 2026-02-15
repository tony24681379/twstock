#!/bin/bash
# Twstock GCP 完整部署腳本
#
# 此腳本執行完整的 GCP 部署流程：
# 1. 檢查環境
# 2. 建立 Terraform 基礎設施
# 3. 構建並推送 Docker 映像
# 4. 部署到 Cloud Run
# 5. 部署前端到 Cloud Storage
#
# 使用方式:
#   ./scripts/deploy-gcp.sh [project-id]
#
# 環境變數:
#   DATABASE_URL - Neon PostgreSQL 連線字串
#   REDIS_URL    - Upstash Redis 連線字串

set -euo pipefail

# 顏色輸出
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# 變數
PROJECT_ID="${1:-${GCP_PROJECT_ID:-}}"
REGION="asia-east1"
SERVICE_NAME="twstock-api"
REPOSITORY="twstock-docker"
IMAGE_NAME="api"

# 函數：印出帶顏色的訊息
info() {
    echo -e "${BLUE}ℹ️  $1${NC}"
}

success() {
    echo -e "${GREEN}✅ $1${NC}"
}

warn() {
    echo -e "${YELLOW}⚠️  $1${NC}"
}

error() {
    echo -e "${RED}❌ $1${NC}"
    exit 1
}

# 函數：檢查必要工具
check_tools() {
    info "檢查必要工具..."

    local tools=("gcloud" "terraform" "docker" "jq")
    local missing=()

    for tool in "${tools[@]}"; do
        if ! command -v "$tool" &> /dev/null; then
            missing+=("$tool")
        fi
    done

    if [ ${#missing[@]} -gt 0 ]; then
        error "缺少必要工具: ${missing[*]}\n請先安裝這些工具"
    fi

    success "必要工具已就緒"
}

# 函數：檢查環境變數
check_env() {
    info "檢查環境變數..."

    if [ -z "$PROJECT_ID" ]; then
        error "請提供 GCP 專案 ID: ./scripts/deploy-gcp.sh <project-id>"
    fi

    if [ -z "${DATABASE_URL:-}" ]; then
        error "請設定環境變數 DATABASE_URL"
    fi

    if [ -z "${REDIS_URL:-}" ]; then
        warn "未設定 REDIS_URL，Redis 快取將停用"
    fi

    success "環境變數檢查完成"
}

# 函數：設定 gcloud
setup_gcloud() {
    info "設定 gcloud CLI..."

    gcloud config set project "$PROJECT_ID"
    gcloud config set run/region "$REGION"

    success "gcloud CLI 已設定（專案: $PROJECT_ID, 區域: $REGION）"
}

# 函數：執行 Terraform
run_terraform() {
    info "執行 Terraform 建立基礎設施..."

    cd terraform

    # 建立 terraform.tfvars
    cat > terraform.tfvars <<EOF
project_id   = "$PROJECT_ID"
region       = "$REGION"
database_url = "$DATABASE_URL"
redis_url    = "${REDIS_URL:-}"
EOF

    terraform init
    terraform plan -var-file=environments/prod.tfvars

    echo -e "\n${YELLOW}是否要執行 Terraform apply? (yes/no)${NC}"
    read -r response

    if [ "$response" = "yes" ]; then
        terraform apply -auto-approve -var-file=environments/prod.tfvars
        success "Terraform 基礎設施已建立"
    else
        warn "跳過 Terraform apply"
    fi

    cd ..
}

# 函數：構建並推送 Docker 映像
build_and_push() {
    info "構建 Docker 映像..."

    # 配置 Docker 認證
    gcloud auth configure-docker "$REGION-docker.pkg.dev"

    # 構建映像
    IMAGE_TAG=$(git rev-parse --short HEAD)
    IMAGE_URL="$REGION-docker.pkg.dev/$PROJECT_ID/$REPOSITORY/$IMAGE_NAME"

    docker build \
        -t "$IMAGE_URL:latest" \
        -t "$IMAGE_URL:$IMAGE_TAG" \
        -f docker/api/Dockerfile .

    success "Docker 映像已構建"

    info "推送 Docker 映像到 Artifact Registry..."
    docker push "$IMAGE_URL:latest"
    docker push "$IMAGE_URL:$IMAGE_TAG"

    success "Docker 映像已推送"
}

# 函數：部署到 Cloud Run
deploy_api() {
    info "部署 API 到 Cloud Run..."

    IMAGE_URL="$REGION-docker.pkg.dev/$PROJECT_ID/$REPOSITORY/$IMAGE_NAME:latest"

    gcloud run deploy "$SERVICE_NAME" \
        --image="$IMAGE_URL" \
        --region="$REGION" \
        --platform=managed \
        --allow-unauthenticated \
        --set-secrets=DATABASE_URL=database_url:latest,REDIS_URL=redis_url:latest \
        --cpu=2 \
        --memory=2Gi \
        --min-instances=0 \
        --max-instances=10 \
        --timeout=300s \
        --quiet

    # 取得服務 URL
    API_URL=$(gcloud run services describe "$SERVICE_NAME" \
        --region="$REGION" \
        --format='value(status.url)')

    success "API 已部署: $API_URL"

    # 健康檢查
    info "執行健康檢查..."
    sleep 5

    if curl -sf "$API_URL/api/health" > /dev/null; then
        success "健康檢查通過"
        curl -s "$API_URL/api/health" | jq '.'
    else
        error "健康檢查失敗"
    fi

    echo "$API_URL" > .api_url
}

# 函數：部署前端
deploy_frontend() {
    info "部署前端到 Cloud Storage..."

    BUCKET_NAME="$PROJECT_ID-frontend"
    API_URL=$(cat .api_url)

    cd dashboard

    # 安裝依賴
    npm ci

    # 構建
    VITE_API_BASE_URL="$API_URL" npm run build

    # 上傳到 Cloud Storage
    gsutil -m rsync -r -d dist/ "gs://$BUCKET_NAME/"

    # 設定快取標頭
    gsutil -m setmeta \
        -h "Cache-Control:public, max-age=31536000" \
        "gs://$BUCKET_NAME/assets/**" || true

    gsutil setmeta \
        -h "Cache-Control:no-cache" \
        "gs://$BUCKET_NAME/*.html" || true

    cd ..

    FRONTEND_URL="https://storage.googleapis.com/$BUCKET_NAME/index.html"
    success "前端已部署: $FRONTEND_URL"
}

# 函數：顯示部署摘要
show_summary() {
    API_URL=$(cat .api_url)
    FRONTEND_URL="https://storage.googleapis.com/$PROJECT_ID-frontend/index.html"

    echo ""
    echo -e "${GREEN}═══════════════════════════════════════${NC}"
    echo -e "${GREEN}  🚀 部署完成！${NC}"
    echo -e "${GREEN}═══════════════════════════════════════${NC}"
    echo ""
    echo -e "📍 專案 ID:  ${BLUE}$PROJECT_ID${NC}"
    echo -e "📍 區域:     ${BLUE}$REGION${NC}"
    echo ""
    echo -e "🔗 API URL:      ${BLUE}$API_URL${NC}"
    echo -e "🔗 前端 URL:     ${BLUE}$FRONTEND_URL${NC}"
    echo ""
    echo -e "📊 下一步:"
    echo -e "  1. 訪問前端: ${BLUE}$FRONTEND_URL${NC}"
    echo -e "  2. 檢查 API: ${BLUE}$API_URL/api/health${NC}"
    echo -e "  3. 查看日誌: ${BLUE}gcloud run services logs read $SERVICE_NAME --region=$REGION${NC}"
    echo ""

    rm -f .api_url
}

# 主流程
main() {
    echo -e "${BLUE}════════════════════════════════════${NC}"
    echo -e "${BLUE}  Twstock GCP 部署腳本${NC}"
    echo -e "${BLUE}════════════════════════════════════${NC}"
    echo ""

    check_tools
    check_env
    setup_gcloud

    echo ""
    echo -e "${YELLOW}準備部署到 GCP 專案: $PROJECT_ID${NC}"
    echo -e "${YELLOW}是否繼續? (yes/no)${NC}"
    read -r response

    if [ "$response" != "yes" ]; then
        warn "部署已取消"
        exit 0
    fi

    run_terraform
    build_and_push
    deploy_api
    deploy_frontend
    show_summary
}

main "$@"
