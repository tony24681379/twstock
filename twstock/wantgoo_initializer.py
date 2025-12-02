# -*- coding: utf-8 -*-
"""
Wantgoo headers 初始化器
確保在首次請求前正確獲取所有必要的 headers
"""

import asyncio
from typing import Dict

import httpx

try:
    from playwright.async_api import async_playwright
    PLAYWRIGHT_AVAILABLE = True
except ImportError:
    PLAYWRIGHT_AVAILABLE = False
    print(
        "Warning: Playwright not installed. "
        "Install with: pip install playwright && playwright install chromium"
    )


class WantgooInitializer:
    """專門用於初始化 Wantgoo headers"""

    REQUIRED_HEADERS = ["user-agent", "cookie", "x-client-signature", "referer"]

    @classmethod
    async def initialize_with_playwright(cls, timeout: int = 30000) -> Dict[str, str]:
        """
        使用 Playwright 獲取完整的 headers，包含 x-client-signature
        """
        if not PLAYWRIGHT_AVAILABLE:
            raise ImportError("Playwright is required for initialization")

        print("Initializing Wantgoo headers with Playwright...")
        headers = {}
        browser = None

        try:
            async with async_playwright() as p:
                # 啟動瀏覽器
                browser = await p.chromium.launch(
                    headless=True,
                    args=[
                        "--disable-blink-features=AutomationControlled",
                        "--disable-dev-shm-usage",
                        "--no-sandbox",
                        "--disable-web-security",
                        "--disable-features=IsolateOrigins,site-per-process",
                    ],
                )

                context = await browser.new_context(
                    user_agent="Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36",
                    viewport={"width": 1920, "height": 1080},
                    locale="zh-TW",
                    ignore_https_errors=True,
                )

                page = await context.new_page()
                page.set_default_timeout(timeout)

                captured_headers = {}
                signature_found = False

                # 攔截所有請求
                async def handle_request(request):
                    nonlocal signature_found

                    # 檢查是否為 Wantgoo API 請求
                    if "wantgoo.com" in request.url:
                        # 優先尋找帶有 x-client-signature 的請求
                        if any(
                            api in request.url
                            for api in [
                                "/investrue/",
                                "/stock/",
                                "/company-profile",
                                "/financial-statements/",
                                "/dividend-policy/",
                                "/institutional-investors/",
                                "/major-investors/",
                                "/margin-trading/",
                                "/advertisement/",
                            ]
                        ):
                            try:
                                request_headers = await request.all_headers()

                                # 如果找到 x-client-signature，優先保存
                                if "x-client-signature" in request_headers:
                                    print(
                                        f"Found x-client-signature in request to: {request.url}"
                                    )
                                    captured_headers.update(request_headers)
                                    signature_found = True
                                elif not signature_found:
                                    # 如果還沒找到 signature，保存任何 API 請求的 headers
                                    captured_headers.update(request_headers)
                            except Exception:
                                pass

                # 攔截所有回應
                async def handle_response(response):
                    # 記錄 API 回應狀態
                    if "wantgoo.com" in response.url and "/investrue/" in response.url:
                        print(
                            f"API response: {response.url} - Status: {response.status}"
                        )

                page.on("request", handle_request)
                page.on("response", handle_response)

                # 訪問頁面
                print("Navigating to https://www.wantgoo.com/stock/2330...")
                try:
                    await page.goto(
                        "https://www.wantgoo.com/stock/2330",
                        wait_until="domcontentloaded",
                    )

                    # 等待頁面載入並發出 API 請求
                    print("Waiting for API requests...")

                    # 等待特定的 API 請求或元素
                    try:
                        # 等待股票資料元素出現
                        await page.wait_for_selector(
                            'div[class*="stock"]', timeout=5000
                        )
                    except Exception:
                        pass

                    # 額外等待以確保 API 請求發出
                    await page.wait_for_timeout(5000)

                    # 如果還沒找到 signature，嘗試觸發更多 API 請求
                    if not signature_found:
                        print("Trying to trigger more API requests...")
                        try:
                            # 嘗試點擊或滾動來觸發更多請求
                            await page.evaluate(
                                "window.scrollTo(0, document.body.scrollHeight)"
                            )
                            await page.wait_for_timeout(2000)
                        except Exception:
                            pass

                except Exception as e:
                    print(f"Navigation error: {e}")
                    # 即使導航失敗，可能已經捕獲到一些 headers

                # 從捕獲的 headers 中提取必要的欄位
                if captured_headers:
                    for header in cls.REQUIRED_HEADERS:
                        if header in captured_headers:
                            headers[header] = captured_headers[header]

                # 獲取 cookies
                cookies = await context.cookies()
                if cookies and "cookie" not in headers:
                    cookie_str = "; ".join(
                        [f"{c['name']}={c['value']}" for c in cookies]
                    )
                    if cookie_str:
                        headers["cookie"] = cookie_str

                # 確保有 referer
                if "referer" not in headers:
                    headers["referer"] = "https://www.wantgoo.com/stock/2330"

                # 確保有 user-agent
                if "user-agent" not in headers:
                    headers["user-agent"] = context._options.get(
                        "user_agent",
                        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36",
                    )

                await browser.close()

        except Exception as e:
            if browser:
                await browser.close()
            raise e

        # 驗證是否獲取到所有必要的 headers
        missing_headers = [h for h in cls.REQUIRED_HEADERS if h not in headers]
        if missing_headers:
            print(f"Warning: Missing required headers: {missing_headers}")
            if "x-client-signature" in missing_headers:
                print("Critical: x-client-signature not found. API requests may fail.")
        else:
            print(f"Successfully obtained all required headers")
            print(
                f"x-client-signature: {headers.get('x-client-signature', 'N/A')[:20]}..."
            )

        return headers

    @classmethod
    async def initialize_fallback(cls) -> Dict[str, str]:
        """
        備用方案：使用 httpx 獲取基本 headers（可能缺少 x-client-signature）
        """
        print("Using fallback initialization (may not include x-client-signature)...")

        headers = {
            "user-agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36",
            "accept": "application/json, text/plain, */*",
            "accept-language": "zh-TW,zh;q=0.9,en;q=0.8",
            "referer": "https://www.wantgoo.com/stock/2330",
        }

        try:
            async with httpx.AsyncClient(follow_redirects=True, timeout=10.0) as client:
                response = await client.get(
                    "https://www.wantgoo.com/stock/2330", headers=headers
                )

                if response.cookies:
                    cookie_str = "; ".join(
                        [f"{k}={v}" for k, v in response.cookies.items()]
                    )
                    if cookie_str:
                        headers["cookie"] = cookie_str

                print("Fallback: obtained basic headers (without x-client-signature)")
        except Exception as e:
            print(f"Fallback initialization failed: {e}")

        return headers

    @classmethod
    async def initialize(cls, use_playwright: bool = True) -> Dict[str, str]:
        """
        主要初始化方法
        """
        if use_playwright and PLAYWRIGHT_AVAILABLE:
            try:
                return await cls.initialize_with_playwright()
            except Exception as e:
                print(f"Playwright initialization failed: {e}")
                return await cls.initialize_fallback()
        else:
            return await cls.initialize_fallback()


# 測試用主程式
async def main():
    """測試 header 初始化"""
    headers = await WantgooInitializer.initialize()

    print("\nObtained headers:")
    for key, value in headers.items():
        if key in ["cookie", "x-client-signature"]:
            print(f"{key}: {value[:50]}..." if len(value) > 50 else f"{key}: {value}")
        else:
            print(f"{key}: {value}")

    # 驗證必要的 headers
    required = WantgooInitializer.REQUIRED_HEADERS
    missing = [h for h in required if h not in headers]

    if missing:
        print(f"\n⚠️  Missing required headers: {missing}")
    else:
        print(f"\n✅ All required headers obtained successfully!")

    return headers


if __name__ == "__main__":
    asyncio.run(main())
