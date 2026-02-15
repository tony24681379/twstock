#!/bin/bash

# 載入 .env.production 環境變數並執行 main.py
echo "🚀 載入生產環境設定並執行 main.py..."

# 載入環境變數
set -a
source .env.production
set +a

# 執行 main.py
poetry run python main.py
