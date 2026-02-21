import asyncio
import datetime
import time
from datetime import date
from typing import Optional

import httpx

try:
    from .wantgoo_initializer import WantgooInitializer
except ImportError:
    WantgooInitializer = None

WANTGOO_BASE_URL = "https://www.wantgoo.com/"

DEFAULT_HEADERS = {
    "user-agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/131.0.0.0 Safari/537.36"
    ),
    "accept": "application/json, text/plain, */*",
    "accept-language": "zh-TW,zh;q=0.9,en;q=0.8",
    "cache-control": "no-cache",
    "pragma": "no-cache",
    "referer": "https://www.wantgoo.com/stock/2330",
}


class HeaderManager:
    """管理 Wantgoo HTTP headers 的完整生命週期：初始化、快取、刷新"""

    def __init__(self, initializer=None):
        """
        Args:
            initializer: WantgooInitializer 類別（可選，用於 Playwright 初始化）
        """
        self._initializer = initializer or WantgooInitializer
        self._headers = DEFAULT_HEADERS.copy()
        self._initialized = False
        self._lock = asyncio.Lock()
        self._last_refresh_time: float = 0
        self._api_latest_date: Optional[date] = None

    @property
    def api_latest_date(self) -> Optional[date]:
        return self._api_latest_date

    async def get_headers(self) -> dict:
        """取得有效的 headers，首次呼叫時自動初始化"""
        if not self._initialized:
            await self._initialize()
        return self._headers

    async def refresh(self) -> bool:
        """刷新 headers（401/403 時呼叫）。5 秒內的重複呼叫會被跳過。"""
        async with self._lock:
            # 防抖：5 秒內已刷新則跳過
            if self._last_refresh_time > 0 and time.time() - self._last_refresh_time < 5:
                print("Headers recently refreshed by another request, skipping...")
                return True

            print("Headers expired, refreshing...")

            # Step 1: 簡單 cookie 刷新（保留 x-client-signature）
            try:
                async with httpx.AsyncClient(
                    http2=True, follow_redirects=True, timeout=10.0
                ) as client:
                    temp_headers = DEFAULT_HEADERS.copy()
                    if "x-client-signature" in self._headers:
                        temp_headers["x-client-signature"] = self._headers[
                            "x-client-signature"
                        ]

                    response = await client.get(
                        "https://www.wantgoo.com/stock/2330", headers=temp_headers
                    )

                    if response.cookies:
                        cookie_str = "; ".join(
                            [f"{k}={v}" for k, v in response.cookies.items()]
                        )
                        if cookie_str:
                            self._headers["cookie"] = cookie_str
                            self._last_refresh_time = time.time()
                            print("Cookies refreshed successfully")
                            return True
                    else:
                        print("No cookies received from server")
            except Exception as e:
                print(f"Failed to refresh cookies: {e}")

            # Step 2: Playwright 完整重新獲取
            if self._initializer:
                try:
                    print(
                        "WARNING: Simple refresh failed, using Playwright as last resort..."
                    )
                    headers = await self._initializer.initialize(use_playwright=True)
                    if headers:
                        self._headers.update(headers)
                        self._last_refresh_time = time.time()
                        print("Headers fully refreshed with Playwright")
                        return True
                except Exception as e:
                    print(f"Playwright refresh also failed: {e}")

            return False

    async def _initialize(self):
        """初始化 headers 並探測 API 最新日期"""
        async with self._lock:
            if self._initialized:
                return

            print("Initializing headers and checking API latest date...")

            # Step 1: Playwright 獲取完整 headers
            if self._initializer:
                try:
                    headers = await self._initializer.initialize(use_playwright=True)
                    if headers and "x-client-signature" in headers:
                        self._headers.update(headers)
                        self._initialized = True
                        print("Headers initialized with x-client-signature")
                except Exception as e:
                    print(f"WantgooInitializer failed: {e}")

            # Step 2: Fallback 到基本 cookies
            if not self._initialized:
                try:
                    async with httpx.AsyncClient(
                        http2=True, follow_redirects=True, timeout=10.0
                    ) as client:
                        response = await client.get(
                            "https://www.wantgoo.com/stock/2330", headers=self._headers
                        )
                        if response.cookies:
                            cookie_str = "; ".join(
                                [f"{k}={v}" for k, v in response.cookies.items()]
                            )
                            if cookie_str:
                                self._headers["cookie"] = cookie_str
                                print("Headers initialized with basic cookies")
                except Exception as e:
                    print(f"Failed to get basic cookies: {e}")

            self._initialized = True

            # Step 3: 探測 API 最新日期
            await self._probe_api_latest_date()

    async def _probe_api_latest_date(self):
        """透過 2330 candlestick API 探測最新資料日期"""
        try:
            print("Fetching test data from API to check latest date...")
            params = {
                "before": int(time.time()) * 1000,
                "top": 5,
            }
            async with httpx.AsyncClient(
                http2=True, timeout=httpx.Timeout(30.0)
            ) as client:
                response = await client.get(
                    WANTGOO_BASE_URL + "investrue/2330/historical-daily-candlesticks",
                    params=params,
                    headers=self._headers,
                )
                if response.status_code == 200:
                    test_data = response.json()
                    if test_data and len(test_data) > 0:
                        latest_timestamp = test_data[0]["tradeDate"] / 1000
                        self._api_latest_date = datetime.datetime.fromtimestamp(
                            latest_timestamp
                        ).date()
                        print(f"✓ API 最新資料日期: {self._api_latest_date}")
                    else:
                        print("⚠️  無法從 API 取得測試資料，將使用預設更新邏輯")
                else:
                    print(f"⚠️  API 回應狀態碼: {response.status_code}")
        except Exception as e:
            print(f"⚠️  確認 API 最新日期時發生錯誤: {e}")
            print("   將使用預設更新邏輯")
