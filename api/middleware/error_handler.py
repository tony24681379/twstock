"""
錯誤處理中間件
提供統一的錯誤響應格式
"""
from datetime import datetime

from fastapi import Request, status
from fastapi.exceptions import RequestValidationError
from fastapi.responses import JSONResponse

from api.exceptions import TwstockAPIException
from api.utils.logger import logger


async def twstock_exception_handler(request: Request, exc: TwstockAPIException) -> JSONResponse:
    """
    處理自定義異常

    Args:
        request: FastAPI Request 對象
        exc: 自定義異常

    Returns:
        統一格式的錯誤響應
    """
    logger.warning(
        f"{exc.__class__.__name__}: {exc.message}",
        extra={
            "path": request.url.path,
            "method": request.method,
            "status_code": exc.status_code
        }
    )

    return JSONResponse(
        status_code=exc.status_code,
        content={
            "error": exc.__class__.__name__,
            "message": exc.message,
            "timestamp": datetime.utcnow().isoformat(),
            "path": str(request.url.path)
        }
    )


async def validation_exception_handler(request: Request, exc: RequestValidationError) -> JSONResponse:
    """
    處理 Pydantic 驗證異常

    Args:
        request: FastAPI Request 對象
        exc: 驗證異常

    Returns:
        統一格式的錯誤響應
    """
    logger.warning(
        f"Validation error: {exc.errors()}",
        extra={"path": request.url.path, "method": request.method}
    )

    return JSONResponse(
        status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
        content={
            "error": "ValidationError",
            "message": "請求參數格式不正確",
            "details": exc.errors(),
            "timestamp": datetime.utcnow().isoformat(),
            "path": str(request.url.path)
        }
    )


async def generic_exception_handler(request: Request, exc: Exception) -> JSONResponse:
    """
    處理未預期的異常

    Args:
        request: FastAPI Request 對象
        exc: 異常對象

    Returns:
        統一格式的錯誤響應
    """
    logger.error(
        f"Unhandled exception: {str(exc)}",
        exc_info=True,
        extra={"path": request.url.path, "method": request.method}
    )

    return JSONResponse(
        status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
        content={
            "error": "InternalServerError",
            "message": "Internal server error occurred",
            "timestamp": datetime.utcnow().isoformat(),
            "path": str(request.url.path)
        }
    )
        }
    )
