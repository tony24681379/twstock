#!/bin/bash

# 載入 .env.production 環境變數並執行 twstock.main
echo "🚀 載入生產環境設定並執行 twstock.main..."

# 載入環境變數
set -a
source .env.production
set +a

# 執行 twstock.main
poetry run python -m twstock.main
