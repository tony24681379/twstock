"""FastAPI 主應用程式"""

import logging
import os
import sys
import traceback
from contextlib import asynccontextmanager

from fastapi import FastAPI, Request, status
from fastapi.exceptions import RequestValidationError
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse

# 加入專案根目錄到 Python path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from api.config import ALLOWED_ORIGINS, API_DESCRIPTION, API_TITLE, API_VERSION
from api.routers import cache, convertible, stocks
from twstock.database import DatabaseManager
from api.cache import RedisClient

# 設定日誌
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)

# 全域資料庫管理器
db_manager = None


@asynccontextmanager
async def lifespan(app: FastAPI):
    """應用程式生命週期管理"""
    global db_manager

    # Startup: 初始化資料庫連線
    print("🚀 初始化資料庫連線...")
    db_manager = DatabaseManager()
    await db_manager.init_database()
    print("✅ 資料庫連線成功")

    # Startup: 初始化 Redis 快取
    print("🚀 初始化 Redis 快取...")
    await RedisClient.connect()
    redis_status = "成功" if await RedisClient.health_check() else "停用（REDIS_URL 未設定）"
    print(f"✅ Redis 快取: {redis_status}")

    yield

    # Shutdown: 清理資源
    print("🔌 關閉資料庫連線...")
    if db_manager:
        await db_manager.close()
    print("✅ 資料庫連線已關閉")

    print("🔌 關閉 Redis 連線...")
    await RedisClient.disconnect()
    print("✅ Redis 連線已關閉")

    print("✅ 資源清理完成")


# 建立 FastAPI 應用
app = FastAPI(
    title=API_TITLE, version=API_VERSION, description=API_DESCRIPTION, lifespan=lifespan
)

# CORS 設定
app.add_middleware(
    CORSMiddleware,
    allow_origins=ALLOWED_ORIGINS,
    allow_credentials=True,
    allow_methods=["GET", "POST", "OPTIONS"],
    allow_headers=["*"],
)


# 全域錯誤處理器
@app.exception_handler(RequestValidationError)
async def validation_exception_handler(request: Request, exc: RequestValidationError):
    """處理驗證錯誤"""
    logger.warning(f"驗證錯誤: {request.url} - {exc.errors()}")
    return JSONResponse(
        status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
        content={
            "error": "驗證錯誤",
            "message": "請求參數格式不正確",
            "details": exc.errors(),
        },
    )


@app.exception_handler(Exception)
async def global_exception_handler(request: Request, exc: Exception):
    """處理所有未捕獲的異常"""
    logger.error(f"未處理的異常: {request.url}")
    logger.error(traceback.format_exc())

    return JSONResponse(
        status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
        content={
            "error": "伺服器錯誤",
            "message": "系統發生錯誤，請稍後再試",
            "detail": str(exc) if os.getenv("DEBUG") == "true" else None,
        },
    )


# 註冊路由
app.include_router(stocks.router)
app.include_router(convertible.router)
app.include_router(cache.router)


@app.get("/")
async def root():
    """根端點"""
    return {"name": API_TITLE, "version": API_VERSION, "status": "running"}


@app.get("/api/health")
async def health_check():
    """健康檢查端點"""
    try:
        # 檢查 db_manager 是否初始化
        if not db_manager:
            logger.warning("健康檢查失敗: 資料庫管理器未初始化")
            return JSONResponse(
                status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
                content={
                    "status": "unhealthy",
                    "database": "not initialized",
                    "redis": "unknown",
                    "version": API_VERSION,
                },
            )

        # 測試資料庫連線
        async with db_manager.async_session_factory() as session:
            from sqlalchemy import text

            result = await session.execute(text("SELECT 1"))
            result.scalar()

        # 檢查 Redis 連線
        redis_healthy = await RedisClient.health_check()
        redis_status = "connected" if redis_healthy else "disconnected"

        return {
            "status": "healthy",
            "database": "connected",
            "redis": redis_status,
            "version": API_VERSION,
        }

    except Exception as e:
        logger.error(f"健康檢查失敗: {str(e)}")
        return JSONResponse(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            content={
                "status": "unhealthy",
                "database": "error",
                "redis": "unknown",
                "error": str(e),
                "version": API_VERSION,
            },
        )


def get_db():
    """取得資料庫管理器（Dependency）"""
    return db_manager


if __name__ == "__main__":
    import uvicorn

    uvicorn.run("main:app", host="0.0.0.0", port=8000, reload=True)
