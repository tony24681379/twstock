import asyncio
import datetime
import random
import time
from collections import namedtuple

import httpx
import pandas as pd

try:
    from json.decoder import JSONDecodeError
except ImportError:
    JSONDecodeError = ValueError

from .header_manager import HeaderManager

WANTGOO_BASE_URL = "https://www.wantgoo.com/"
DATATUPLE = namedtuple("Data", ["date", "volume", "open", "high", "low", "close"])


class WantgooFetcher:
    REPORT_URL = WANTGOO_BASE_URL

    def __init__(self, header_manager: HeaderManager):
        self.header_manager = header_manager

    async def fetch_url(self, url: str, params: dict = None, retry: int = 5):
        headers = await self.header_manager.get_headers()

        async with httpx.AsyncClient(http2=True, timeout=httpx.Timeout(30.0)) as client:
            headers_refreshed = False

            for retry_i in range(retry):
                delay = min(0.5 * (2 ** retry_i) + random.uniform(0, 0.5), 10)

                try:
                    response = await client.get(url, params=params, headers=headers)
                except httpx.TimeoutException:
                    print(f"Timeout on attempt {retry_i + 1} for {url}")
                    await asyncio.sleep(delay)
                    continue
                except Exception as e:
                    print(f"Error on attempt {retry_i + 1} for {url}: {e}")
                    await asyncio.sleep(delay)
                    continue

                try:
                    if response.status_code in [400, 401, 403]:
                        if not headers_refreshed:
                            print(f"Got {response.status_code}, refreshing headers...")
                            success = await self.header_manager.refresh()
                            headers_refreshed = True
                            if success:
                                headers = await self.header_manager.get_headers()
                                print(f"Headers refreshed, retrying {url}")
                                continue
                            else:
                                print(f"Failed to refresh headers for {url}")
                        else:
                            print(
                                f"Still getting {response.status_code} after refresh for {url}"
                            )

                        await asyncio.sleep(delay)
                        continue
                    elif response.status_code != 200:
                        print(f"Got status {response.status_code} for {url}")
                        await asyncio.sleep(delay)
                        continue

                    response = response.json()

                except JSONDecodeError:
                    if not headers_refreshed and retry_i == 0:
                        print("JSON decode error, trying to refresh headers...")
                        success = await self.header_manager.refresh()
                        headers_refreshed = True
                        if success:
                            headers = await self.header_manager.get_headers()
                            continue

                    print(f"JSON decode error for {url}")
                    if retry_i < retry - 1:
                        await asyncio.sleep(delay)
                    continue
                else:
                    break
            else:
                print(f"{url} fail after {retry} attempts")
                raise Exception(f"{url} fail after {retry} attempts")

        return response

    async def fetch_info(self, sid: str):
        """抓取股票基本資訊、EPS、股利

        Returns:
            tuple: (info: pd.Series, dividends_history: list[dict])
        """
        sid = sid.lower()
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
        dividends_history = []
        if len(dividend) > 0:
            if (
                dividend[0]["period"] is not None
                and str(datetime.datetime.now().year - 2011) in dividend[0]["period"]
            ):
                cash_dividend = round(dividend[0]["cashDividend"], 2)
                stock_dividend = round(dividend[0]["stockDividend"], 2)

            for d in dividend:
                if d.get("period") is not None:
                    try:
                        year = int(d["period"].replace("年", "").strip())
                        dividends_history.append({
                            "year": year,
                            "cash_dividend": round(d.get("cashDividend", 0), 2),
                            "stock_dividend": round(d.get("stockDividend", 0), 2),
                        })
                    except (ValueError, TypeError):
                        pass

        info = {
            "id": sid,
            "capital": 1.0,
            "outstanding_shares": outstanding_shares,
            "PER": eps.iloc[0] if len(eps) >= 1 else None,
            "cash_dividend": cash_dividend,
            "stock_dividend": stock_dividend,
        }
        info.update(eps)
        info = pd.Series(info)

        return info, dividends_history

    async def fetch_daily(self, sid: str, num: int, outstanding_shares: float = 1.0):
        """抓取每日交易資料

        Args:
            sid: 股票代碼
            num: 抓取天數
            outstanding_shares: 流通股數（用於計算持股率，預設 1.0）

        Returns:
            pd.DataFrame: 每日交易資料
        """
        sid = sid.lower()
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
            if "fail after" in str(e):
                print(f"Stock {sid} data unavailable, returning empty dataset")
                return pd.DataFrame()
            else:
                raise e

        daily_data = self.purify(
            candlesticks, major_investors, institutional_investors, lending, borrowing,
            outstanding_shares=outstanding_shares,
        )

        return daily_data

    async def fetch_concentration_data(self, sid: str, weeks: int = 10):
        """抓取股票籌碼集中度數據

        Returns:
            pd.DataFrame: 包含近 N 週的籌碼集中度數據
        """
        sid = sid.lower()

        try:
            data = await self.fetch_url(
                self.REPORT_URL + "stock/" + sid + "/major-investors/concentration-data"
            )
        except Exception as e:
            if "fail after" in str(e):
                print(f"Stock {sid} concentration data unavailable")
                return pd.DataFrame()
            else:
                raise e

        if not data:
            return pd.DataFrame()

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
            if "date" in item
        ]

        if not records:
            return pd.DataFrame()

        df = pd.DataFrame(records).sort_values("date", ascending=False)
        weekly_data = df.head(weeks)

        return weekly_data

    async def fetch_monthly_revenue(self, sid: str, months: int = 12) -> pd.DataFrame:
        """抓取股票月營收資料

        Returns:
            pd.DataFrame: 包含近 N 個月的月營收數據
        """
        sid = sid.lower()

        try:
            data = await self.fetch_url(
                self.REPORT_URL
                + "stock/"
                + sid
                + "/financial-statements/monthly-revenue-data"
            )
        except Exception as e:
            if "fail after" in str(e):
                print(f"Stock {sid} monthly revenue data unavailable")
                return pd.DataFrame()
            else:
                raise e

        if not data:
            return pd.DataFrame()

        records = []
        for item in data:
            dt = datetime.datetime.fromtimestamp(item["date"] / 1000)
            records.append(
                {
                    "year": dt.year,
                    "month": dt.month,
                    "revenue": item.get("monthRevenue", 0),
                    "mom_change": item.get("preMonthRevenueDiff"),
                    "yoy_change": item.get("preYearMonthRevenueDiff"),
                    "cumulative_revenue": item.get("monthTotalRevenue"),
                    "cumulative_yoy_change": item.get("preTotalRevenueDiff"),
                }
            )

        df = (
            pd.DataFrame(records)
            .sort_values(["year", "month"], ascending=False)
            .head(months)
        )

        return df

    def purify(
        self, candlesticks, major_investors, institutional_investors, lending, borrowing,
        outstanding_shares: float = 1.0,
    ):
        from .db_utils import to_python_datetime

        candlesticks_data = pd.DataFrame(
            candlesticks, columns=["volume", "open", "close", "high", "low"]
        )
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
        major_investors_data["date"] = [
            to_python_datetime(datetime.datetime.fromtimestamp(d["date"] / 1000))
            for d in major_investors
        ]

        lending_data = pd.DataFrame(
            lending, columns=["date", "lendingBalance", "limit"]
        ).rename(
            columns={"lendingBalance": "lending_balance", "limit": "balance_limit"}
        )
        lending_data["date"] = [
            to_python_datetime(datetime.datetime.fromtimestamp(d["date"] / 1000))
            for d in lending
        ]

        borrowing_data = pd.DataFrame(
            borrowing, columns=["date", "borrowingBalance"]
        ).rename(columns={"borrowingBalance": "borrowing_balance"})
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
                data["ingHolding"].astype(float) / outstanding_shares * 100, 2
            )
        )
        data = data.assign(
            dealer_holding_rate=round(
                data["sum_holding_rate"].astype(float)
                - data["foreign_holding_rate"].astype(float)
                - data["investment_trust_holding_rate"].astype(float),
                2,
            ),
            foreign_shares=round(
                (data["sumForeignNoDealer"] + data["sumForeignWithDealer"]).astype(
                    float
                ),
                2,
            ),
            investment_trust_shares=round(data["investment_trust"].astype(float), 2),
            dealer_shares=round(
                (data["sumDealerBySelf"] + data["sumDealerHedging"]).astype(float),
                2,
            ),
        )

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
                "foreign_shares",
                "investment_trust_shares",
                "dealer_shares",
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
