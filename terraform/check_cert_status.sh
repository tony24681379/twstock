#!/bin/bash

# Firebase 自定義域名證書狀態檢查腳本
# 每 2 分鐘檢查一次，直到證書啟用或超時

TERRAFORM_DIR="/Users/tony/twstock/terraform"
MAX_CHECKS=20  # 最多檢查 20 次（40 分鐘）
CHECK_INTERVAL=120  # 每 2 分鐘檢查一次

cd "$TERRAFORM_DIR" || exit 1

echo "🔍 開始監控 Firebase 自定義域名證書狀態..."
echo "域名: twstock.changes.live"
echo "最大檢查次數: $MAX_CHECKS (約 $((MAX_CHECKS * CHECK_INTERVAL / 60)) 分鐘)"
echo "檢查間隔: $((CHECK_INTERVAL / 60)) 分鐘"
echo ""

for i in $(seq 1 $MAX_CHECKS); do
    echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
    echo "📊 第 $i 次檢查 ($(date '+%Y-%m-%d %H:%M:%S'))"
    echo ""

    # 刷新 Terraform 狀態
    terraform refresh -var-file=environments/prod.tfvars \
        -target=module.firebase_hosting.google_firebase_hosting_custom_domain.custom \
        > /dev/null 2>&1

    # 取得證書狀態
    CERT_STATUS=$(terraform output -json firebase_custom_domain_status 2>/dev/null)

    if [ $? -eq 0 ]; then
        CERT_STATE=$(echo "$CERT_STATUS" | jq -r '.cert_state')
        CERT_TYPE=$(echo "$CERT_STATUS" | jq -r '.cert_type')

        echo "證書狀態: $CERT_STATE"
        echo "證書類型: $CERT_TYPE"
        echo ""

        # 檢查是否已啟用
        if [ "$CERT_STATE" = "CERT_ACTIVE" ]; then
            echo "✅ 證書已啟用！"
            echo ""
            echo "測試訪問:"
            curl -I https://twstock.changes.live 2>&1 | head -n 5
            echo ""
            echo "🎉 自定義域名配置完成！"
            echo "請訪問: https://twstock.changes.live"
            exit 0
        elif [ "$CERT_STATE" = "CERT_PROPAGATING" ]; then
            echo "⏳ 證書仍在傳播中，繼續等待..."
        elif [ "$CERT_STATE" = "CERT_VALIDATING" ]; then
            echo "🔍 證書正在驗證中..."
        else
            echo "⚠️  未知狀態: $CERT_STATE"
        fi
    else
        echo "❌ 無法取得證書狀態"
    fi

    # 如果不是最後一次檢查，等待後繼續
    if [ $i -lt $MAX_CHECKS ]; then
        echo ""
        echo "等待 $((CHECK_INTERVAL / 60)) 分鐘後進行下一次檢查..."
        sleep $CHECK_INTERVAL
    fi
done

echo ""
echo "⏰ 已達到最大檢查次數 ($MAX_CHECKS 次)"
echo "證書仍未啟用，可能需要更長時間（最長 24 小時）"
echo ""
echo "手動檢查方法:"
echo "  cd $TERRAFORM_DIR"
echo "  terraform refresh -var-file=environments/prod.tfvars > /dev/null 2>&1"
echo "  terraform output firebase_custom_domain_status"
