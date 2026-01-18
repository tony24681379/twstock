"""
身份驗證模組

使用 Google OAuth 2.0 + JWT token 驗證
支援 email 白名單控制存取權限
可透過環境變數開關驗證功能
"""

import os
from typing import Optional
from fastapi import Header, HTTPException, status
from jose import jwt, JWTError
import httpx


# 環境變數配置
REQUIRE_AUTH = os.getenv("REQUIRE_AUTH", "false").lower() == "true"
ALLOWED_EMAILS = [email.strip() for email in os.getenv("ALLOWED_EMAILS", "").split(",") if email.strip()]
GOOGLE_CLIENT_ID = os.getenv("GOOGLE_CLIENT_ID", "")

# Google OAuth 2.0 公鑰端點
GOOGLE_CERTS_URL = "https://www.googleapis.com/oauth2/v3/certs"


class AuthUser:
    """驗證後的使用者資訊"""
    def __init__(self, email: str, name: Optional[str] = None, picture: Optional[str] = None):
        self.email = email
        self.name = name
        self.picture = picture

    def __repr__(self):
        return f"AuthUser(email={self.email}, name={self.name})"


async def verify_google_token(token: str) -> dict:
    """
    驗證 Google OAuth JWT token

    Args:
        token: Google OAuth ID token

    Returns:
        解碼後的 token payload

    Raises:
        HTTPException: token 無效或過期
    """
    try:
        # 從 Google 取得公鑰
        async with httpx.AsyncClient() as client:
            response = await client.get(GOOGLE_CERTS_URL)
            response.raise_for_status()
            certs = response.json()

        # 解碼並驗證 token
        # 注意：jose.jwt.decode 不支援 async，但這是 CPU-bound 操作很快
        decoded = jwt.decode(
            token,
            certs,
            algorithms=["RS256"],
            audience=GOOGLE_CLIENT_ID,
            options={
                "verify_signature": True,
                "verify_exp": True,
                "verify_aud": True,
            }
        )

        return decoded

    except JWTError as e:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail=f"Invalid or expired token: {str(e)}",
            headers={"WWW-Authenticate": "Bearer"},
        )
    except httpx.HTTPError as e:
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail=f"Unable to verify token: {str(e)}",
        )


def check_email_whitelist(email: str) -> None:
    """
    檢查 email 是否在白名單中

    Args:
        email: 使用者 email

    Raises:
        HTTPException: email 不在白名單中
    """
    if not ALLOWED_EMAILS:
        # 如果白名單為空，允許所有已驗證的使用者
        return

    if email not in ALLOWED_EMAILS:
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail=f"Email '{email}' is not authorized to access this resource",
        )


async def get_current_user(authorization: Optional[str] = Header(None)) -> Optional[AuthUser]:
    """
    FastAPI dependency：驗證並取得當前使用者

    根據 REQUIRE_AUTH 環境變數決定是否強制驗證：
    - REQUIRE_AUTH=false: 允許匿名存取，回傳 None
    - REQUIRE_AUTH=true: 強制驗證，驗證失敗拋出 HTTPException

    Args:
        authorization: HTTP Authorization header (Bearer token)

    Returns:
        AuthUser 或 None（匿名存取時）

    Raises:
        HTTPException: 驗證失敗（401/403）
    """
    # 如果不需要驗證，直接返回 None（匿名存取）
    if not REQUIRE_AUTH:
        return None

    # 需要驗證但沒有提供 token
    if not authorization:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Authentication required. Please login with your Google account.",
            headers={"WWW-Authenticate": "Bearer"},
        )

    # 提取 Bearer token
    if not authorization.startswith("Bearer "):
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Invalid authentication credentials. Expected 'Bearer <token>'.",
            headers={"WWW-Authenticate": "Bearer"},
        )

    token = authorization.replace("Bearer ", "")

    # 驗證 token
    decoded = await verify_google_token(token)

    # 取得使用者資訊
    email = decoded.get("email")
    if not email:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Token does not contain email address",
            headers={"WWW-Authenticate": "Bearer"},
        )

    # 檢查白名單
    check_email_whitelist(email)

    # 建立使用者物件
    user = AuthUser(
        email=email,
        name=decoded.get("name"),
        picture=decoded.get("picture"),
    )

    return user


def get_auth_status() -> dict:
    """
    取得驗證系統配置狀態（用於健康檢查或除錯）

    Returns:
        驗證系統配置資訊
    """
    return {
        "require_auth": REQUIRE_AUTH,
        "has_client_id": bool(GOOGLE_CLIENT_ID),
        "whitelist_enabled": bool(ALLOWED_EMAILS),
        "whitelist_count": len(ALLOWED_EMAILS),
    }
