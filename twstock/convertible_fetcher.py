"""可轉換公司債資料爬蟲 - 使用 TPEX (櫃買中心) API

資料來源:
- 可轉債清單 + 基本資訊: TPEX OpenAPI bond_ISSBD5_data
- 每日交易資料: TPEX cbDayQry POST endpoint
"""

import asyncio
import logging
import os
from datetime import datetime, timedelta
from typing import Optional

import httpx

logger = logging.getLogger(__name__)

TPEX_OPENAPI = "https://www.tpex.org.tw/openapi/v1/bond_ISSBD5_data"
TPEX_CB_DAY_QRY = "https://www.tpex.org.tw/www/zh-tw/bond/cbDayQry"


class ConvertibleBondFetcher:
    """可轉債爬蟲，使用 TPEX 櫃買中心 API 取得資料"""

    def __init__(self, db_manager=None):
        self.db_manager = db_manager
        # TPEX 有安全機制，併發不能太高
        self._semaphore = asyncio.Semaphore(2)
        self._client: Optional[httpx.AsyncClient] = None
        self._request_delay = 0.3  # 每次請求間隔秒數

    async def _get_client(self) -> httpx.AsyncClient:
        if self._client is None or self._client.is_closed:
            self._client = httpx.AsyncClient(
                timeout=httpx.Timeout(15.0),
                follow_redirects=False,
                headers={
                    "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                    "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
                    "Referer": "https://www.tpex.org.tw/zh-tw/bond/info/statistics-cb/day-quotes.html",
                    "X-Requested-With": "XMLHttpRequest",
                },
            )
        return self._client

    async def close(self):
        if self._client and not self._client.is_closed:
            await self._client.aclose()

    async def get_convertible_bond_list(self) -> list[dict]:
        """取得所有可轉債清單與基本資訊

        資料來源: TPEX OpenAPI bond_ISSBD5_data (轉(交)換債發行資料)
        篩選條件: 公開發行 (OfferingMethod=7) 且尚未到期

        Returns:
            list[dict]: 每筆含 bond_id, name, underlying_stock_id,
                        conversion_price, issue_date, maturity_date 等
        """
        logger.info("📡 取得可轉債清單 (TPEX OpenAPI bond_ISSBD5_data)...")

        async with httpx.AsyncClient(timeout=httpx.Timeout(30.0)) as client:
            resp = await client.get(
                TPEX_OPENAPI,
                headers={"User-Agent": "Mozilla/5.0"},
            )
            resp.raise_for_status()
            data = resp.json()

        if not data:
            logger.warning("⚠️ 無法取得可轉債清單")
            return []

        now = datetime.now()
        bonds = []
        for row in data:
            # 僅公開發行 (OfferingMethod=7) 的可轉債才有交易資料
            if row.get("OfferingMethod") != "7":
                continue

            # 跳過已到期的
            maturity = _parse_date_yyyymmdd(row.get("MaturityDate"))
            if maturity and maturity < now:
                continue

            bond_id = row.get("BondCode", "").strip()
            if not bond_id:
                continue

            bonds.append({
                "bond_id": bond_id,
                "name": row.get("ShortName", "").strip(),
                "underlying_stock_id": row.get("IssuerCode", "").strip(),
                "conversion_price": _safe_float(
                    row.get("Conversion/ExchangePriceAtIssuance")
                ),
                "issue_date": _parse_date_yyyymmdd(row.get("IssueDate")),
                "maturity_date": maturity,
                "put_date": _parse_date_yyyymmdd(row.get("PutOptionDate")),
                "put_price": _safe_float(row.get("PutOptionPrice")),
                "coupon_rate": _safe_float(row.get("CouponRate")) or 0.0,
                "issued_amount": _safe_float(row.get("IssueAmount")) or 0.0,
                "outstanding_amount": _safe_float(row.get("OutstandingAmount")) or 0.0,
                "is_active": True,
                "updated_at": now,
            })

        logger.info(f"✅ 取得 {len(bonds)} 檔可轉債")
        return bonds

    async def fetch_convertible_bond_daily(
        self,
        bond_id: str,
        start_date: str = "",
        underlying_close_map: Optional[dict] = None,
        conversion_price: Optional[float] = None,
    ) -> list[dict]:
        """取得單支可轉債的每日交易資料

        資料來源: TPEX cbDayQry POST endpoint (每次回傳一個月的資料)

        Args:
            bond_id: 可轉債代碼 (如 13166)
            start_date: 起始日期 (YYYY-MM-DD)
            underlying_close_map: {date_str: close_price} 標的股收盤價映射
            conversion_price: 轉換價格（用於計算衍生欄位）
        """
        if not start_date:
            start_date = (datetime.now() - timedelta(days=30)).strftime("%Y-%m-%d")

        start_dt = datetime.strptime(start_date[:10], "%Y-%m-%d")
        end_dt = datetime.now()

        # cbDayQry 每次回傳一個月，需逐月查詢
        records = []
        current = start_dt.replace(day=1)
        while current <= end_dt:
            date_param = f"{current.year}/{current.month:02d}/01"
            month_records = await self._fetch_cb_month(
                bond_id, date_param, underlying_close_map, conversion_price
            )
            records.extend(month_records)
            # 移到下個月
            if current.month == 12:
                current = current.replace(year=current.year + 1, month=1)
            else:
                current = current.replace(month=current.month + 1)

        # 篩選 start_date 之後的資料
        return [r for r in records if r["date"] and r["date"] >= start_dt]

    async def _fetch_cb_month(
        self,
        bond_id: str,
        date_param: str,
        underlying_close_map: Optional[dict] = None,
        conversion_price: Optional[float] = None,
    ) -> list[dict]:
        """查詢單月 CB 交易資料"""
        async with self._semaphore:
            await asyncio.sleep(self._request_delay)
            client = await self._get_client()
            resp = await client.post(
                TPEX_CB_DAY_QRY,
                data={
                    "date": date_param,
                    "code": bond_id,
                    "id": "",
                    "response": "json",
                },
                headers={
                    "Content-Type": "application/x-www-form-urlencoded; charset=UTF-8",
                },
            )
            if resp.status_code == 302:
                logger.warning(f"⚠️ TPEX 安全限制 (302)，等待後重試 {bond_id}")
                await asyncio.sleep(5)
                resp = await client.post(
                    TPEX_CB_DAY_QRY,
                    data={
                        "date": date_param,
                        "code": bond_id,
                        "id": "",
                        "response": "json",
                    },
                    headers={
                        "Content-Type": "application/x-www-form-urlencoded; charset=UTF-8",
                    },
                )
            if resp.status_code != 200:
                logger.warning(f"⚠️ cbDayQry 回傳 {resp.status_code} for {bond_id}")
                return []
            data = resp.json()

        if data.get("stat") != "ok":
            return []

        records = []
        tables = data.get("tables", [])
        if not tables or not tables[0].get("data"):
            return []

        # 回傳格式: date 是 JSON 頂層的 "date" (YYYYMMDD)，但資料中的日期是民國
        for row in tables[0]["data"]:
            # row = [日期(民國), 交易模式, 收市價, 漲跌, 開市價, 最高價, 最低價, 成交筆數, 單位, 成交金額, 平均價]
            if len(row) < 11:
                continue

            trade_mode = row[1].strip() if row[1] else ""
            # 只取等價交易資料（主要交易管道）
            if trade_mode != "等價":
                continue

            cb_close = _safe_float(row[2])
            if cb_close is None:
                continue

            trade_date = _parse_roc_date(row[0])
            if not trade_date:
                continue

            date_str = trade_date.strftime("%Y-%m-%d")

            underlying_close = None
            if underlying_close_map and date_str in underlying_close_map:
                underlying_close = underlying_close_map[date_str]

            conversion_value = None
            premium_rate = None
            arbitrage_spread = None
            if conversion_price and conversion_price > 0 and underlying_close:
                # 面額 100,000 元 / 轉換價格 = 可轉換股數
                conversion_shares = 100000 / conversion_price
                # 轉換價值 = 可轉換股數 * 標的股價 / 1000 (換回百元面額基準)
                conversion_value = round(
                    conversion_shares * underlying_close / 1000, 2
                )
                if conversion_value > 0:
                    premium_rate = round(
                        (cb_close - conversion_value) / conversion_value * 100, 2
                    )
                    # 套利空間 = -溢價率 - 交易成本(約0.6%)
                    arbitrage_spread = round(-premium_rate - 0.6, 2)

            records.append({
                "bond_id": bond_id,
                "date": trade_date,
                "open": _safe_float(row[4]),
                "high": _safe_float(row[5]),
                "low": _safe_float(row[6]),
                "close": cb_close,
                "volume": _safe_float(row[8]) or 0,
                "underlying_close": underlying_close,
                "conversion_value": conversion_value,
                "premium_rate": premium_rate,
                "arbitrage_spread": arbitrage_spread,
            })

        return records

    async def fetch_all_cb_daily(
        self, bonds: list[dict], stock_daily_map: Optional[dict] = None
    ) -> list[dict]:
        """批次取得所有可轉債的每日交易資料

        Args:
            bonds: get_convertible_bond_list() 回傳的 CB 清單
            stock_daily_map: {stock_id: {date_str: close}} 標的股收盤價
        """
        all_records = []
        total = len(bonds)
        traded_count = 0

        for i, bond in enumerate(bonds):
            bond_id = bond["bond_id"]
            underlying_id = bond.get("underlying_stock_id", "")
            conversion_price = bond.get("conversion_price")

            underlying_close_map = None
            if stock_daily_map and underlying_id in stock_daily_map:
                underlying_close_map = stock_daily_map[underlying_id]

            records = await self.fetch_convertible_bond_daily(
                bond_id=bond_id,
                underlying_close_map=underlying_close_map,
                conversion_price=conversion_price,
            )
            all_records.extend(records)
            if records:
                traded_count += 1

            if (i + 1) % 50 == 0 or (i + 1) == total:
                logger.info(
                    f"  📊 CB 每日資料: {i + 1}/{total} (有交易: {traded_count})"
                )

        await self.close()
        return all_records


def _safe_float(value) -> Optional[float]:
    if value is None or value == "":
        return None
    if isinstance(value, str):
        value = value.strip().replace(",", "")
        if not value:
            return None
    try:
        return float(value)
    except (ValueError, TypeError):
        return None


def _parse_date_yyyymmdd(value) -> Optional[datetime]:
    """解析 YYYYMMDD 格式日期"""
    if not value or not str(value).strip():
        return None
    s = str(value).strip()
    if len(s) < 8:
        return None
    try:
        return datetime.strptime(s[:8], "%Y%m%d")
    except (ValueError, TypeError):
        return None


def _parse_roc_date(value) -> Optional[datetime]:
    """解析民國日期 (如 1150102 = 2026/01/02)"""
    if not value:
        return None
    s = str(value).strip()
    if len(s) < 7:
        return None
    try:
        roc_year = int(s[:-4])
        month = int(s[-4:-2])
        day = int(s[-2:])
        return datetime(roc_year + 1911, month, day)
    except (ValueError, TypeError):
        return None
