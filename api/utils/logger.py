"""
結構化日誌工具
提供統一的日誌記錄功能
"""

import logging
import sys
from typing import Optional

# 創建 logger 實例
logger = logging.getLogger("twstock_api")


def setup_logger(log_level: str = "INFO") -> logging.Logger:
    """
    設置日誌記錄器

    Args:
        log_level: 日誌級別 (DEBUG, INFO, WARNING, ERROR, CRITICAL)

    Returns:
        配置好的 logger 實例
    """
    # 設置日誌級別
    logger.setLevel(getattr(logging, log_level.upper()))

    # 如果已經有 handler，不要重複添加
    if logger.handlers:
        return logger

    # 創建 console handler
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setLevel(logging.DEBUG)

    # 創建格式器
    formatter = logging.Formatter(
        fmt="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )
    console_handler.setFormatter(formatter)

    # 添加 handler 到 logger
    logger.addHandler(console_handler)

    return logger


def log_api_call(endpoint: str, params: Optional[dict] = None):
    """
    記錄 API 呼叫

    Args:
        endpoint: API 端點
        params: 請求參數
    """
    logger.debug(f"API Call: {endpoint}", extra={"params": params})


def log_query_performance(
    query_name: str, duration_ms: float, row_count: Optional[int] = None
):
    """
    記錄資料庫查詢性能

    Args:
        query_name: 查詢名稱
        duration_ms: 執行時間（毫秒）
        row_count: 返回行數
    """
    extra = {"duration_ms": duration_ms}
    if row_count is not None:
        extra["row_count"] = row_count

    logger.info(f"Query [{query_name}] completed in {duration_ms:.2f}ms", extra=extra)


def log_service_error(service_name: str, operation: str, error: Exception):
    """
    記錄服務層錯誤

    Args:
        service_name: 服務名稱
        operation: 操作名稱
        error: 異常對象
    """
    logger.error(
        f"Service Error in {service_name}.{operation}: {str(error)}",
        exc_info=True,
        extra={"service": service_name, "operation": operation},
    )


# 初始化 logger（從環境變量讀取日誌級別，預設 INFO）
import os

setup_logger(os.getenv("LOG_LEVEL", "INFO"))
setup_logger(os.getenv("LOG_LEVEL", "INFO"))
