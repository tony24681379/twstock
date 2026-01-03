import asyncio
import datetime
import time
from collections import namedtuple
from typing import Optional

import httpx
import pandas as pd

try:
    from json.decoder import JSONDecodeError
except ImportError:
    JSONDecodeError = ValueError

try:
    from .wantgoo_initializer import WantgooInitializer
except ImportError:
    WantgooInitializer = None

try:
    from .database import DatabaseManager
except ImportError:
    DatabaseManager = None

WANTGOO_BASE_URL = "https://www.wantgoo.com/"
DATATUPLE = namedtuple("Data", ["date", "volume", "open", "high", "low", "close"])


class BaseFetcher:
    def fetch(self, year, month, sid, retry):
        pass

    def _make_datatuple(self, data):
        pass

    def purify(self, original_data):
        pass


class WantgooFetcher(BaseFetcher):
    REPORT_URL = WANTGOO_BASE_URL
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

    # 類級別的鎖，確保只有一個 Playwright 實例在初始化 headers
    _refresh_lock = asyncio.Lock()
    _last_refresh_time = None  # 類級別的上次刷新時間

    def __init__(self, db_manager: Optional[DatabaseManager] = None):
        super().__init__()
        self.headers = self.DEFAULT_HEADERS.copy()
        self.initialized = False
        self.db_manager = db_manager  # 資料庫管理器
        self.api_latest_date = None  # API 最新資料日期

    async def _initialize_headers(self):
        """初始化 headers（只在第一次使用時）並確認 API 最新資料日期"""
        async with WantgooFetcher._refresh_lock:
            if self.initialized:
                return

            print("Initializing headers and checking API latest date...")

            # 優先使用 WantgooInitializer 獲取完整 headers
            if WantgooInitializer:
                try:
                    headers = await WantgooInitializer.initialize(use_playwright=True)
                    if headers and "x-client-signature" in headers:
                        self.headers.update(headers)
                        self.initialized = True
                        print("Headers initialized with x-client-signature")
                        # 繼續確認 API 最新日期
                except Exception as e:
                    print(f"WantgooInitializer failed: {e}")

            # 如果還沒初始化，使用備用方法獲取 cookies
            if not self.initialized:
                try:
                    async with httpx.AsyncClient(
                        http2=True, follow_redirects=True, timeout=10.0
                    ) as client:
                        response = await client.get(
                            "https://www.wantgoo.com/stock/2330", headers=self.headers
                        )
                        if response.cookies:
                            cookie_str = "; ".join(
                                [f"{k}={v}" for k, v in response.cookies.items()]
                            )
                            if cookie_str:
                                self.headers["cookie"] = cookie_str
                                print("Headers initialized with basic cookies")
                except Exception as e:
                    print(f"Failed to get basic cookies: {e}")

            self.initialized = True

            # 抓取測試股票（2330）來確認 API 最新資料日期
            try:
                print("Fetching test data from API to check latest date...")
                params = {
                    "before": int(time.time()) * 1000,
                    "top": 5,  # 只抓最近 5 筆就夠了
                }
                test_data = await self.fetch_url(
                    self.REPORT_URL + "investrue/2330/historical-daily-candlesticks",
                    params,
                )

                if test_data and len(test_data) > 0:
                    # 取得最新的資料日期
                    latest_timestamp = test_data[0]["tradeDate"] / 1000
                    self.api_latest_date = datetime.datetime.fromtimestamp(
                        latest_timestamp
                    ).date()
                    print(f"✓ API 最新資料日期: {self.api_latest_date}")

                    # 如果有資料庫管理器，將最新日期傳遞給它
                    if self.db_manager:
                        self.db_manager.api_latest_date = self.api_latest_date
                else:
                    print("⚠️  無法從 API 取得測試資料，將使用預設更新邏輯")
            except Exception as e:
                print(f"⚠️  確認 API 最新日期時發生錯誤: {e}")
                print("   將使用預設更新邏輯")

    async def _refresh_headers(self):
        """當 headers 失效時更新（只在 API 返回 401/403 時）"""
        async with WantgooFetcher._refresh_lock:
            # 檢查是否已經被其他請求更新過了
            if WantgooFetcher._last_refresh_time is not None:
                import time

                if time.time() - WantgooFetcher._last_refresh_time < 5:  # 5 秒內已刷新
                    print("Headers recently refreshed by another request, skipping...")
                    return True

            print("Headers expired, refreshing...")

            # 重要：更新時不使用 Playwright，因為太慢
            # 只使用簡單的 httpx 方法重新獲取 cookies
            # x-client-signature 通常不會變，只有 cookies 會過期

            try:
                async with httpx.AsyncClient(
                    http2=True, follow_redirects=True, timeout=10.0
                ) as client:
                    # 保留現有的 x-client-signature，只更新 cookies
                    temp_headers = self.DEFAULT_HEADERS.copy()
                    if "x-client-signature" in self.headers:
                        temp_headers["x-client-signature"] = self.headers[
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
                            # 只更新 cookie，保留其他 headers
                            self.headers["cookie"] = cookie_str

                            WantgooFetcher._last_refresh_time = time.time()
                            print("Cookies refreshed successfully")
                            return True
                    else:
                        print("No cookies received from server")
            except Exception as e:
                print(f"Failed to refresh cookies: {e}")

            # 如果簡單方法失敗，最後才考慮使用 Playwright（但通常不應該發生）
            if WantgooInitializer:
                try:
                    print(
                        "WARNING: Simple refresh failed, using Playwright as last resort..."
                    )
                    headers = await WantgooInitializer.initialize(use_playwright=True)
                    if headers:
                        self.headers.update(headers)
                        import time

                        WantgooFetcher._last_refresh_time = time.time()
                        print("Headers fully refreshed with Playwright")
                        return True
                except Exception as e:
                    print(f"Playwright refresh also failed: {e}")

            return False

    async def fetch_url(self, url: str, params: dict = None, retry: int = 5):
        # 確保初始化（只在第一次）
        if not self.initialized:
            await self._initialize_headers()

        async with httpx.AsyncClient(http2=True, timeout=httpx.Timeout(30.0)) as client:
            headers_refreshed = False  # 記錄是否已經刷新過 headers

            for retry_i in range(retry):
                try:
                    response = await client.get(
                        url, params=params, headers=self.headers
                    )
                except httpx.TimeoutException:
                    print(f"Timeout on attempt {retry_i + 1} for {url}")
                    await asyncio.sleep(1)
                    continue
                except Exception as e:
                    print(f"Error on attempt {retry_i + 1} for {url}: {e}")
                    await asyncio.sleep(1)
                    continue

                try:
                    # 在 400/401/403 時更新 headers（且只更新一次）
                    if response.status_code in [400, 401, 403]:
                        if not headers_refreshed:
                            print(f"Got {response.status_code}, refreshing headers...")
                            success = await self._refresh_headers()
                            headers_refreshed = True
                            if success:
                                print(f"Headers refreshed, retrying {url}")
                                continue  # 用新 headers 重試
                            else:
                                print(f"Failed to refresh headers for {url}")
                        else:
                            print(
                                f"Still getting {response.status_code} after refresh for {url}"
                            )

                        # 如果更新失敗或已經更新過，繼續重試但不再更新
                        await asyncio.sleep(2)  # 稍微等待長一點
                        continue
                    elif response.status_code != 200:
                        print(f"Got status {response.status_code} for {url}")
                        await asyncio.sleep(1)
                        continue

                    response = response.json()

                except JSONDecodeError:
                    # JSON 解析失敗，可能是 headers 失效
                    if not headers_refreshed and retry_i == 0:
                        print(f"JSON decode error, trying to refresh headers...")
                        success = await self._refresh_headers()
                        headers_refreshed = True
                        if success:
                            continue

                    print(f"JSON decode error for {url}")
                    if retry_i < retry - 1:
                        await asyncio.sleep(0.5)
                    continue
                else:
                    # 成功獲取資料
                    break
            else:
                print(f"{url} fail after {retry} attempts")
                raise Exception(f"{url} fail after {retry} attempts")

        return response

    async def fetch_info(self, sid: str, save_to_db: bool = True):
        sid = sid.lower()  # 統一使用小寫 ID
        company_profile = self.fetch_url(
            self.REPORT_URL + "stock/" + sid + "/company-profile-data"
        )
        eps = self.fetch_url(
            self.REPORT_URL + "stock/" + sid + "/financial-statements/eps-data"
        )
        dividend = self.fetch_url(
            self.REPORT_URL + "stock/" + sid + "/dividend-policy/ex-dividend-data"
        )
        company_profile, eps, dividend = await asyncio.gather(
            company_profile, eps, dividend
        )

        outstanding_shares = (
            company_profile["outstandingShares"]
            if type(company_profile) is dict
            else 1.0
        )
        eps = pd.Series(
            {str(e["year"]) + "/" + str(e["season"]) + "Q": e["beps"] for e in eps}
        )

        cash_dividend = 0
        stock_dividend = 0
        if len(dividend) > 0:
            if (
                dividend[0]["period"] is not None
                and str(datetime.datetime.now().year - 2011) in dividend[0]["period"]
            ):
                cash_dividend = round(dividend[0]["cashDividend"], 2)
                stock_dividend = round(dividend[0]["stockDividend"], 2)

        self.info = {
            "id": sid,
            "capital": 1.0,
            "outstanding_shares": outstanding_shares,
            "PER": eps.iloc[0] if len(eps) >= 1 else None,
            "cash_dividend": cash_dividend,
            "stock_dividend": stock_dividend,
        }
        self.info.update(eps)
        self.info = pd.Series(self.info)

        # 儲存到資料庫
        if save_to_db and self.db_manager:
            try:
                await self.db_manager.save_stock_info(sid, self.info)
                print(f"Stock info for {sid} saved to database")
            except Exception as e:
                print(f"Error saving stock info to database: {e}")

        return self.info

    async def fetch_daily(
        self, sid: str, num: int, total_stock: int, save_to_db: bool = True
    ):
        sid = sid.lower()  # 統一使用小寫 ID
        self.total_stock = total_stock
        params = {
            "before": int(time.mktime(datetime.datetime.now().timetuple())) * 1000,
            "top": num,
        }

        try:
            candlesticks = self.fetch_url(
                self.REPORT_URL + "investrue/" + sid + "/historical-daily-candlesticks",
                params,
            )

            institutional_investors = self.fetch_url(
                self.REPORT_URL
                + "stock/"
                + sid
                + "/institutional-investors/trend-data?topdays=20"
            )
            major_investors = self.fetch_url(
                self.REPORT_URL + "stock/" + sid + "/major-investors/main-trend-data"
            )

            lending = self.fetch_url(
                self.REPORT_URL
                + "stock/"
                + sid
                + "/margin-trading/historical-lending-balance"
            )

            borrowing = self.fetch_url(
                self.REPORT_URL
                + "stock/"
                + sid
                + "/margin-trading/historical-borrowing-balance",
            )
            (
                candlesticks,
                major_investors,
                institutional_investors,
                lending,
                borrowing,
            ) = await asyncio.gather(
                candlesticks,
                major_investors,
                institutional_investors,
                lending,
                borrowing,
            )
        except Exception as e:
            # 如果是股票不存在或已下市，返回空資料
            if "fail after" in str(e):
                print(f"Stock {sid} data unavailable, returning empty dataset")
                return pd.DataFrame()
            else:
                raise e

        daily_data = self.purify(
            candlesticks, major_investors, institutional_investors, lending, borrowing
        )

        # 儲存到資料庫
        if save_to_db and self.db_manager and not daily_data.empty:
            try:
                await self.db_manager.save_daily_data(sid, daily_data)
                print(
                    f"Daily data for {sid} saved to database ({len(daily_data)} records)"
                )
            except Exception as e:
                print(f"Error saving daily data to database: {e}")

        return daily_data

    async def fetch_concentration_data(
        self, sid: str, weeks: int = 10, save_to_db: bool = True
    ):
        """
        抓取股票籌碼集中度數據

        Args:
            sid: 股票代碼
            weeks: 取得幾週的數據（預設 10 週）
            save_to_db: 是否儲存到資料庫

        Returns:
            pd.DataFrame: 包含近 N 週的籌碼集中度數據

        注意：fetch_url() 會自動套用 self.headers（包含 x-client-signature 和 cookies）
        不需要額外傳遞 headers 參數
        """
        sid = sid.lower()  # 統一使用小寫 ID

        try:
            # fetch_url() 內部會自動處理 headers 和重試邏輯
            data = await self.fetch_url(
                self.REPORT_URL + "stock/" + sid + "/major-investors/concentration-data"
            )
        except Exception as e:
            if "fail after" in str(e):
                print(f"Stock {sid} concentration data unavailable")
                return pd.DataFrame()
            else:
                raise e

        # 轉換數據
        records = [
            {
                "date": datetime.datetime.fromtimestamp(item["date"] / 1000),
                "moreThan400": item.get("moreThan400", 0),
                "moreThan1000": item.get("moreThan1000", 0),
                "lessThan20": item.get("lessThan20", 0),
                "close": item.get("close", 0),
                "directorRatio": item.get("directorRatio", 0),
                "rateOfForeignHolding": item.get("rateOfForeignHolding", 0),
                "rateOfINGHolding": item.get("rateOfINGHolding", 0),
                "rateOfDealerHolding": item.get("rateOfDealerHolding", 0),
            }
            for item in data
        ]

        df = pd.DataFrame(records).sort_values("date", ascending=False)

        # 儲存到資料庫（儲存所有資料）
        if save_to_db and self.db_manager and not df.empty:
            try:
                await self.db_manager.save_concentration_data(sid, df)
            except Exception as e:
                print(f"Error saving concentration data to database: {e}")

        # API 返回的資料已經是週資料，直接取前 N 週
        weekly_data = df.head(weeks)

        return weekly_data

    def purify(
        self, candlesticks, major_investors, institutional_investors, lending, borrowing
    ):
        # Import db_utils for consistent date conversion
        from .db_utils import to_python_datetime

        candlesticks_data = pd.DataFrame(
            candlesticks, columns=["volume", "open", "close", "high", "low"]
        )
        # 確保所有日期都是 Python datetime，避免 Pandas Timestamp
        candlesticks_data["date"] = [
            to_python_datetime(datetime.datetime.fromtimestamp(d["tradeDate"] / 1000))
            for d in candlesticks
        ]

        institutional_investors_data = pd.DataFrame(
            institutional_investors,
            columns=[
                "date",
                "sumForeignNoDealer",
                "sumForeignWithDealer",
                "sumING",
                "sumDealerBySelf",
                "sumDealerHedging",
                "sumHoldingRate",
                "foreignHoldingRate",
                "ingHolding",
            ],
        ).rename(
            columns={
                "sumING": "investment_trust",
                "sumHoldingRate": "sum_holding_rate",
                "foreignHoldingRate": "foreign_holding_rate",
            }
        )

        # 確保所有日期都是 Python datetime，避免 Pandas Timestamp
        institutional_investors_data["date"] = [
            to_python_datetime(datetime.datetime.fromtimestamp(d["date"] / 1000))
            for d in institutional_investors
        ]

        major_investors_data = pd.DataFrame(
            major_investors,
            columns=["date", "stockAgentMainPower", "stockAgentDiff", "skp5", "skp20"],
        ).rename(
            columns={
                "stockAgentMainPower": "major_investors",
                "stockAgentDiff": "agent_diff",
            }
        )
        # 確保所有日期都是 Python datetime，避免 Pandas Timestamp
        major_investors_data["date"] = [
            to_python_datetime(datetime.datetime.fromtimestamp(d["date"] / 1000))
            for d in major_investors
        ]

        lending_data = pd.DataFrame(
            lending, columns=["date", "lendingBalance", "limit"]
        ).rename(
            columns={"lendingBalance": "lending_balance", "limit": "balance_limit"}
        )
        # 確保所有日期都是 Python datetime，避免 Pandas Timestamp
        lending_data["date"] = [
            to_python_datetime(datetime.datetime.fromtimestamp(d["date"] / 1000))
            for d in lending
        ]

        borrowing_data = pd.DataFrame(
            borrowing, columns=["date", "borrowingBalance"]
        ).rename(columns={"borrowingBalance": "borrowing_balance"})
        # 確保所有日期都是 Python datetime，避免 Pandas Timestamp
        borrowing_data["date"] = [
            to_python_datetime(datetime.datetime.fromtimestamp(d["date"] / 1000))
            for d in borrowing
        ]

        data = pd.merge(
            candlesticks_data, institutional_investors_data, how="left", on=["date"]
        )
        data = pd.merge(data, major_investors_data, how="left", on=["date"])
        data = pd.merge(data, lending_data, how="left", on=["date"])
        data = pd.merge(data, borrowing_data, how="left", on=["date"])

        data = data.assign(
            investment_trust_holding_rate=round(
                data["ingHolding"].astype(float) / self.total_stock * 100, 2
            )
        )
        data = data.assign(
            dealer_holding_rate=round(
                data["sum_holding_rate"].astype(float)
                - data["foreign_holding_rate"].astype(float)
                - data["investment_trust_holding_rate"].astype(float),
                2,
            ),
            foreign=round(
                (data["sumForeignNoDealer"] + data["sumForeignWithDealer"]).astype(
                    float
                )
                / self.total_stock
                * 100,
                2,
            ),
            investment_trust=round(
                data["investment_trust"].astype(float) / self.total_stock * 100, 2
            ),
            dealer=round(
                (data["sumDealerBySelf"] + data["sumDealerHedging"]).astype(float)
                / self.total_stock
                * 100,
                2,
            ),
        )

        # 設定 pandas 選項避免 FutureWarning
        import warnings

        with warnings.catch_warnings():
            warnings.filterwarnings("ignore", category=FutureWarning)
            data = data.fillna(0.0)

        data = data.infer_objects(copy=False)

        return data[
            [
                "date",
                "volume",
                "open",
                "close",
                "high",
                "low",
                "foreign",
                "investment_trust",
                "dealer",
                "sum_holding_rate",
                "foreign_holding_rate",
                "investment_trust_holding_rate",
                "dealer_holding_rate",
                "major_investors",
                "agent_diff",
                "skp5",
                "skp20",
                "lending_balance",
                "borrowing_balance",
                "balance_limit",
            ]
        ]

    async def get_all_stock_list(self, retry: int = 5) -> list[str]:
        data = await self.fetch_url(self.REPORT_URL + "investrue/all-alive")

        filtered = filter(lambda l: l["type"] in ["Index", "Stock", "ETF"], data)
        filtered = filter(lambda l: l["country"] in ["TW"], filtered)
        return list(filtered)
