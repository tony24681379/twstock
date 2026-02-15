"""
TWSE 警示股票資料爬蟲

從台灣證券交易所取得注意股和處置股公告資料
"""

import asyncio
from datetime import datetime
from typing import Dict, List, Optional

import httpx


class TWSEAlertFetcher:
    """TWSE 警示股票資料爬蟲"""

    BASE_URL = "https://www.twse.com.tw/announcement"
    TIMEOUT = 30.0
    MAX_RETRIES = 3

    def __init__(self):
        """初始化爬蟲"""
        self.headers = {
            "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36",
            "Accept": "application/json, text/javascript, */*; q=0.01",
            "Accept-Language": "zh-TW,zh;q=0.9,en-US;q=0.8,en;q=0.7",
        }

    async def _fetch_with_retry(
        self,
        url: str,
        params: Dict,
        max_retries: int = MAX_RETRIES,
    ) -> Optional[Dict]:
        """
        HTTP 請求（含重試機制）

        Args:
            url: API URL
            params: 查詢參數
            max_retries: 最大重試次數

        Returns:
            JSON 回應資料，失敗返回 None
        """
        for attempt in range(max_retries):
            try:
                async with httpx.AsyncClient(
                    headers=self.headers,
                    timeout=self.TIMEOUT,
                    follow_redirects=True,
                ) as client:
                    response = await client.get(url, params=params)
                    response.raise_for_status()

                    data = response.json()
                    return data

            except httpx.HTTPStatusError as e:
                print(f"⚠️  HTTP error {e.response.status_code} on attempt {attempt + 1}/{max_retries}: {url}")
                if attempt == max_retries - 1:
                    print(f"❌ Failed to fetch {url} after {max_retries} attempts")
                    return None
                await asyncio.sleep(2 ** attempt)  # 指數退避

            except httpx.RequestError as e:
                print(f"⚠️  Request error on attempt {attempt + 1}/{max_retries}: {e}")
                if attempt == max_retries - 1:
                    print(f"❌ Failed to fetch {url} after {max_retries} attempts")
                    return None
                await asyncio.sleep(2 ** attempt)

            except Exception as e:
                print(f"❌ Unexpected error fetching {url}: {e}")
                return None

        return None

    async def fetch_attention_stocks(self, date: Optional[str] = None) -> List[Dict]:
        """
        取得注意股清單

        Args:
            date: YYYYMMDD 格式，預設今日

        Returns:
            [
                {
                    'stock_id': '1471',
                    'name': '首利',
                    'count': 3,
                    'reason': '股價波動過度劇烈',
                    'date': datetime(2026, 1, 31)
                },
                ...
            ]
        """
        if date is None:
            date = datetime.now().strftime("%Y%m%d")

        url = f"{self.BASE_URL}/notice"
        params = {"response": "json", "date": date}

        data = await self._fetch_with_retry(url, params)

        if not data or data.get("stat") != "OK":
            print(f"⚠️  Failed to fetch attention stocks for date {date}")
            return []

        # 解析資料
        attention_stocks = []
        rows = data.get("data", [])

        for row in rows:
            # TWSE API 回傳格式：
            # ['1', '1471', '首利', '3', '股價波動過度劇烈', '2026/01/31', '23.50', '15.2']
            # [編號, 證券代號, 證券名稱, 累計次數, 注意交易資訊, 日期, 收盤價, 本益比]
            if len(row) < 6:
                continue

            stock_id = str(row[1]).strip()
            name = str(row[2]).strip()
            count = int(row[3]) if row[3] else 1
            reason = str(row[4]).strip()
            date_str = str(row[5]).strip()

            # 解析日期 (2026/01/31 -> datetime)
            try:
                date_obj = datetime.strptime(date_str, "%Y/%m/%d")
            except ValueError:
                date_obj = datetime.now()

            attention_stocks.append({
                "stock_id": stock_id,
                "name": name,
                "count": count,
                "reason": reason,
                "date": date_obj,
            })

        return attention_stocks

    async def fetch_disposal_stocks(self, date: Optional[str] = None) -> List[Dict]:
        """
        取得處置股清單

        Args:
            date: YYYYMMDD 格式，預設今日

        Returns:
            [
                {
                    'stock_id': '1471',
                    'name': '首利',
                    'announced_date': datetime(2026, 1, 31),
                    'start_date': datetime(2026, 2, 1),
                    'end_date': datetime(2026, 2, 10),
                    'type': '第一次處置',
                    'condition': '連續三次',
                    'measure': '預收款券',
                    'content': '...'
                },
                ...
            ]
        """
        if date is None:
            date = datetime.now().strftime("%Y%m%d")

        url = f"{self.BASE_URL}/punish"
        params = {"response": "json", "date": date}

        data = await self._fetch_with_retry(url, params)

        if not data or data.get("stat") != "OK":
            print(f"⚠️  Failed to fetch disposal stocks for date {date}")
            return []

        # 解析資料
        disposal_stocks = []
        rows = data.get("data", [])

        for row in rows:
            # TWSE API 實際回傳格式（不同於文件）：
            # row[0]: 序號
            # row[1]: 公布日期 (YYYY/MM/DD)
            # row[2]: 證券代號
            # row[3]: 證券名稱
            # row[4]: 累計次數
            # row[5]: 處置條件
            # row[6]: 處置措施（實際包含處置類型）
            # row[7]: 處置內容（包含處置期間、原因等詳細說明）
            if len(row) < 8:
                continue

            announced_date_str = str(row[1]).strip()
            stock_id = str(row[2]).strip()
            name = str(row[3]).strip()
            cumulative = str(row[4]).strip()
            condition = str(row[5]).strip()
            duration_str = str(row[6]).strip()  # 處置期間 (e.g., "115/01/29～115/02/11")
            disposal_type = str(row[7]).strip()   # 處置類型 (e.g., "第一次處置")

            # 解析公布日期
            try:
                announced_date = datetime.strptime(announced_date_str, "%Y/%m/%d")
            except ValueError:
                announced_date = datetime.now()

            # 解析處置期間 (民國年格式: 115/01/29～115/02/11)
            start_date = None
            end_date = None
            if "～" in duration_str or "~" in duration_str:
                try:
                    # 處理全形和半形波浪號
                    duration_str = duration_str.replace("～", "~")
                    parts = duration_str.split("~")
                    if len(parts) == 2:
                        # 轉換民國年為西元年
                        # 115/01/29 -> 2026/01/29 (115 + 1911 = 2026)
                        start_str = parts[0].strip()
                        end_str = parts[1].strip()

                        # 解析民國年
                        start_parts = start_str.split("/")
                        end_parts = end_str.split("/")

                        if len(start_parts) == 3:
                            roc_year = int(start_parts[0])
                            ad_year = roc_year + 1911
                            start_date = datetime(ad_year, int(start_parts[1]), int(start_parts[2]))

                        if len(end_parts) == 3:
                            roc_year = int(end_parts[0])
                            ad_year = roc_year + 1911
                            end_date = datetime(ad_year, int(end_parts[1]), int(end_parts[2]))
                except (ValueError, IndexError):
                    pass

            disposal_stocks.append({
                "stock_id": stock_id,
                "name": name,
                "announced_date": announced_date,
                "start_date": start_date,
                "end_date": end_date,
                "type": disposal_type,
                "condition": condition,
                "measure": duration_str,  # 保留原始期間字串
                "content": disposal_type,  # 保留處置類型
            })

        return disposal_stocks

    async def detect_system_warnings(
        self,
        stock_id: str,
        daily_data: "pd.DataFrame",  # type: ignore
        margin_data: "pd.DataFrame",  # type: ignore
    ) -> Dict:
        """
        內部風險檢測（輔助功能，可選）

        檢測條件：
        - 異常放量（成交量 > 20日均量 5倍）
        - 融資過高（融資使用率 > 80%）
        - 連續大漲（3日累計 > 20%）
        - 波動異常（10日標準差 > 60日標準差 2倍）

        Args:
            stock_id: 股票代號
            daily_data: 日線資料 DataFrame
            margin_data: 融資融券資料 DataFrame

        Returns:
            {
                'warning_score': 60,
                'warning_reasons': ['異常放量', '融資過高']
            }
        """
        warning_reasons = []
        warning_score = 0

        # TODO: 實作系統預警邏輯（未來版本）
        # 需要 pandas 和相關資料才能計算

        return {
            "warning_score": warning_score,
            "warning_reasons": warning_reasons,
        }


# 測試用主程式
async def main():
    """測試 TWSE API 爬蟲"""
    fetcher = TWSEAlertFetcher()

    print("📢 測試注意股 API...")
    attention_stocks = await fetcher.fetch_attention_stocks()
    print(f"✓ 注意股數量: {len(attention_stocks)}")
    if attention_stocks:
        print(f"範例: {attention_stocks[0]}")

    print("\n📢 測試處置股 API...")
    disposal_stocks = await fetcher.fetch_disposal_stocks()
    print(f"✓ 處置股數量: {len(disposal_stocks)}")
    if disposal_stocks:
        print(f"範例: {disposal_stocks[0]}")


if __name__ == "__main__":
    asyncio.run(main())
