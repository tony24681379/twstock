# -*- coding: utf-8 -*-

import statistics
import sys
from datetime import datetime

import pandas as pd
import talib
from sqlalchemy import text

try:
    from . import analytics
except ImportError as e:
    if e.name == "lxml":
        # Fix #69
        raise e
    import analytics

try:
    from .db_utils import safe_float, to_python_datetime
except ImportError:
    from db_utils import safe_float, to_python_datetime


class Stock(analytics.Analytics):
    def __init__(self, sid: str, fetcher=None, db_manager=None):
        self.sid = sid
        # 使用提供的 fetcher 和 db_manager，或者為 None（稍後初始化）
        self.fetcher = fetcher
        self.db_manager = db_manager
        self.info_data = None
        self.daily_data = None

    async def _ensure_resources(self):
        """確保 fetcher 和 db_manager 已初始化"""
        if self.fetcher is None:
            from .fetcher_manager import get_global_fetcher

            self.fetcher = await get_global_fetcher()
            # 從 fetcher 取得 db_manager
            if self.db_manager is None:
                self.db_manager = self.fetcher.db_manager

        if self.db_manager is None:
            from .fetcher_manager import get_db_manager

            self.db_manager = await get_db_manager()

    async def load_data(self, load_data: bool = True, force_reload: bool = False):
        """
        載入股票資料

        Args:
            load_data: 是否載入資料
            force_reload: 是否強制重新從 API 載入（忽略資料庫快取）
        """
        await self._ensure_resources()

        if not load_data:
            return

        # 檢查追蹤狀態
        tracker = (
            await self.db_manager.get_tracker(self.sid) if not force_reload else None
        )

        # 載入基本資訊
        if force_reload or not tracker or not tracker["info_loaded"]:
            # 從 API 取得並儲存到資料庫
            self.info_data = await self.fetcher.fetch_info(self.sid, save_to_db=True)
            # 更新追蹤狀態
            await self.db_manager.update_tracker(
                self.sid, info_loaded=True, info_updated_at=datetime.now()
            )
        else:
            # 從資料庫載入
            self.info_data = await self.load_info_from_db()

        # 載入每日資料
        if force_reload or not tracker or not tracker["daily_loaded"]:
            # 從 API 取得並儲存到資料庫
            outstanding_shares = (
                float(self.info_data["outstanding_shares"]) / 1000
                if self.info_data is not None and "outstanding_shares" in self.info_data
                else 1.0
            )
            self.daily_data = await self.fetcher.fetch_daily(
                self.sid, 490, outstanding_shares, save_to_db=True
            )

            if not self.daily_data.empty:
                # 使用工具函數轉換 Pandas Timestamp 為 Python datetime
                first_date = to_python_datetime(self.daily_data["date"].min())
                last_date = to_python_datetime(self.daily_data["date"].max())

                # 更新追蹤狀態
                await self.db_manager.update_tracker(
                    self.sid,
                    daily_loaded=True,
                    daily_first_date=first_date,
                    daily_last_date=last_date,
                    daily_count=len(self.daily_data),
                    daily_updated_at=datetime.now(),
                )
        else:
            # 檢查是否需要增量更新
            needs_update = await self.db_manager.check_needs_update(
                self.sid, days_threshold=1
            )
            if needs_update:
                # 增量更新：只取得最近的資料
                outstanding_shares = (
                    float(self.info_data["outstanding_shares"]) / 1000
                    if self.info_data is not None
                    and "outstanding_shares" in self.info_data
                    else 1.0
                )
                recent_data = await self.fetcher.fetch_daily(
                    self.sid, 30, outstanding_shares, save_to_db=True
                )
                # 更新追蹤狀態
                if not recent_data.empty:
                    last_date = to_python_datetime(recent_data["date"].max())

                    await self.db_manager.update_tracker(
                        self.sid,
                        daily_last_date=last_date,
                        daily_updated_at=datetime.now(),
                    )

            # 從資料庫載入
            self.daily_data = await self.load_daily_from_db()

        # 處理資料
        if self.daily_data is not None and len(self.daily_data) > 0:
            # 使用 copy() 避免 SettingWithCopyWarning
            self.daily_data = self.daily_data[::-1].copy()  # 反轉順序並複製
            self.calc_base()

            # 更新分析完成狀態
            await self.db_manager.update_tracker(
                self.sid, analysis_completed=True, analysis_updated_at=datetime.now()
            )

    async def load_info_from_db(self) -> pd.Series:
        """從資料庫載入股票基本資訊"""
        async with self.db_manager.get_session() as session:
            result = await session.execute(
                text("""SELECT * FROM stock_info WHERE stock_id = :stock_id"""),
                {"stock_id": self.sid},
            )
            row = result.first()
            if row:
                # 轉換為 pandas Series
                info_dict = {
                    "id": row.stock_id,
                    "capital": row.capital,
                    "outstanding_shares": row.outstanding_shares,
                    "PER": row.per,
                    "cash_dividend": row.cash_dividend,
                    "stock_dividend": row.stock_dividend,
                }

                # 載入 EPS 資料
                eps_result = await session.execute(
                    text(
                        """SELECT year, quarter, eps FROM stock_eps 
                           WHERE stock_id = :stock_id 
                           ORDER BY year DESC, quarter DESC"""
                    ),
                    {"stock_id": self.sid},
                )
                for eps_row in eps_result:
                    key = f"{eps_row.year}/{eps_row.quarter}Q"
                    info_dict[key] = eps_row.eps

                return pd.Series(info_dict)
            return pd.Series()

    async def load_daily_from_db(self) -> pd.DataFrame:
        """從資料庫載入每日資料"""
        async with self.db_manager.get_session() as session:
            # 載入價格資料（foreign 是保留字，需要引號）
            result = await session.execute(
                text(
                    """
                    SELECT d.*, 
                           i."foreign", i.investment_trust, i.dealer,
                           i.sum_holding_rate, i.foreign_holding_rate,
                           i.investment_trust_holding_rate, i.dealer_holding_rate,
                           m.major_investors, m.agent_diff, m.skp5, m.skp20,
                           mt.lending_balance, mt.borrowing_balance, mt.balance_limit
                    FROM stock_daily d
                    LEFT JOIN institutional_investors i ON d.stock_id = i.stock_id AND d.date = i.date
                    LEFT JOIN major_investors m ON d.stock_id = m.stock_id AND d.date = m.date
                    LEFT JOIN margin_trading mt ON d.stock_id = mt.stock_id AND d.date = mt.date
                    WHERE d.stock_id = :stock_id
                    ORDER BY d.date DESC
                """
                ),
                {"stock_id": self.sid},
            )

            # 轉換為 DataFrame
            data = []
            for row in result:
                data.append(
                    {
                        "date": row.date,
                        "volume": row.volume,
                        "open": row.open,
                        "high": row.high,
                        "low": row.low,
                        "close": row.close,
                        "foreign": row.foreign or 0,
                        "investment_trust": row.investment_trust or 0,
                        "dealer": row.dealer or 0,
                        "sum_holding_rate": row.sum_holding_rate or 0,
                        "foreign_holding_rate": row.foreign_holding_rate or 0,
                        "investment_trust_holding_rate": row.investment_trust_holding_rate
                        or 0,
                        "dealer_holding_rate": row.dealer_holding_rate or 0,
                        "major_investors": row.major_investors or 0,
                        "agent_diff": row.agent_diff or 0,
                        "skp5": row.skp5 or 0,
                        "skp20": row.skp20 or 0,
                        "lending_balance": row.lending_balance or 0,
                        "borrowing_balance": row.borrowing_balance or 0,
                        "balance_limit": row.balance_limit or 0,
                    }
                )

            return pd.DataFrame(data)

    async def get_all_stock_list(self):
        return await self.fetcher.get_all_stock_list()

    def fetch_info(self):
        return self.fetcher.fetch_info(self.sid)

    async def fetch_daily(self, num: int):
        outstanding_shares = (
            float(self.info_data["outstanding_shares"]) / 1000
            if self.info_data is not None and "outstanding_shares" in self.info_data
            else 1.0
        )
        return await self.fetcher.fetch_daily(self.sid, num, outstanding_shares)

    def calc_change(self, after, before):
        return round((after - before) / before * 100, 2)

    def calc_base(self):
        if self.info_data is None or len(self.close) == 0:
            return

        self.info_data["capital"] = round(
            self.close[-1] * float(self.info_data["outstanding_shares"]) / 100000000, 2
        )
        self.info_data["PER"] = (
            round(self.close[-1] / float(self.info_data["PER"]), 2)
            if self.info_data["PER"] is not None and float(self.info_data["PER"]) != 0
            else None
        )

        bollinger_upper, _, bollinger_lower = talib.BBANDS(self.close, 20)
        k9, d9 = talib.STOCH(self.high, self.low, self.close)
        macd, macdsignal, macdhist = talib.MACD(self.close)

        change = [0]
        for i in range(1, len(self.close)):
            change.append(self.calc_change(self.close[i], self.close[i - 1]))

        self.daily_data["bollinger_upper"] = bollinger_upper
        self.daily_data["bollinger_lower"] = bollinger_lower
        self.daily_data["change"] = change
        self.daily_data["ma5"] = talib.MA(self.close, timeperiod=5)
        self.daily_data["ma10"] = talib.MA(self.close, timeperiod=10)
        self.daily_data["ma20"] = talib.MA(self.close, timeperiod=20)
        self.daily_data["ma60"] = talib.MA(self.close, timeperiod=60)
        self.daily_data["k9"] = k9
        self.daily_data["d9"] = d9
        self.daily_data["macd"] = macd
        self.daily_data["macdsignal"] = macdsignal
        self.daily_data["macdhist"] = macdhist
        self.daily_data["adx"] = talib.ADX(
            self.high, self.low, self.close, timeperiod=14
        )
        self.daily_data["adxr"] = talib.ADXR(
            self.high, self.low, self.close, timeperiod=14
        )
        self.daily_data["plus_di"] = talib.PLUS_DI(
            self.high, self.low, self.close, timeperiod=14
        )
        self.daily_data["minus_di"] = talib.MINUS_DI(
            self.high, self.low, self.close, timeperiod=14
        )

        self.calc_line_diff()
        self.calc_trend()
        self.season_upper_and_lower()
        self.daily_data = self.daily_data.dropna(how="any")

    def calc_line_diff(self):
        three_line_diff = []
        four_line_diff = []
        for i in range(0, len(self.close)):
            sub = max(self.ma5[i], self.ma10[i], self.ma20[i]) - min(
                self.ma5[i], self.ma10[i], self.ma20[i]
            )
            avg = statistics.mean([self.ma5[i], self.ma10[i], self.ma20[i]])
            if avg == 0:
                return None

            three_line_diff.append(sub / avg)

            sub = max(self.ma5[i], self.ma10[i], self.ma20[i], self.ma60[i]) - min(
                self.ma5[i], self.ma10[i], self.ma20[i], self.ma60[i]
            )
            avg = statistics.mean(
                [self.ma5[i], self.ma10[i], self.ma20[i], self.ma60[i]]
            )
            if avg == 0:
                return None

            four_line_diff.append(sub / avg)

        self.daily_data["three_line_diff"] = three_line_diff
        self.daily_data["four_line_diff"] = four_line_diff

    def calc_trend(self):
        high = -sys.maxsize - 1
        low = sys.maxsize
        is_up = True
        day = 0
        wave = []
        for i in range(0, len(self.ma5)):
            # wave.append(0)
            if self.close[i] >= self.ma5[i]:
                wave.append(1)
                if is_up is False:
                    low = sys.maxsize
                    is_up = True
                    wave[day] = -2
                if self.high[i] >= high:
                    high = self.high[i]
                    day = i
            else:
                wave.append(-1)
                if is_up is True:
                    high = -sys.maxsize - 1
                    is_up = False
                    wave[day] = 2
                if self.low[i] <= low:
                    low = self.low[i]
                    day = i

        if wave[-1] == 1:
            wave[-1] = 2
        if wave[-1] == -1:
            wave[-1] = -2

        trend = []
        high_point = 0
        low_point = 0
        high = False
        low = False
        last_high_point = 0
        last_low_point = 0
        for i in range(0, len(wave)):
            trend.append(0)
            if wave[i] == 2:
                if self.close[i] > self.close[high_point]:
                    high = True
                    if low is True:
                        trend[low_point] = -3
                        last_low_point = low_point
                        low = False
                else:
                    low = True
                    if high is True:
                        trend[high_point] = 3
                        last_high_point = high_point
                        high = False

                high_point = i

            if wave[i] == -2:
                if self.close[i] < self.close[low_point]:
                    low = True
                    if high is True:
                        trend[high_point] = 3
                        last_high_point = high_point
                        high = False
                else:
                    high = True
                    if low is True:
                        trend[low_point] = -3
                        last_low_point = low_point
                        low = False

                low_point = i

        if high is True and self.close[high_point] > self.close[last_high_point]:
            trend[high_point] = 3
        if low is True and self.close[low_point] < self.close[last_low_point]:
            trend[low_point] = -3
        if trend[-1] == 0:
            for i in range(2, len(trend)):
                if trend[-i] != 0:
                    trend[-1] = trend[-i]
                    break

        for i in range(2, len(trend) - 1):
            if trend[-i] == 0:
                trend[-i] = trend[-i + 1]

        self.daily_data["wave"] = wave
        self.daily_data["trend"] = trend

    def season_upper_and_lower(self):
        self.season = self.daily_data[-60:].sort_values(by="close")

    @property
    def info(self):
        return self.info_data

    @property
    def date(self):
        if (
            self.daily_data is not None
            and not self.daily_data.empty
            and "date" in self.daily_data.columns
        ):
            return self.daily_data.date.values
        return []

    @property
    def volume(self):
        if (
            self.daily_data is not None
            and not self.daily_data.empty
            and "volume" in self.daily_data.columns
        ):
            return self.daily_data.volume.values
        return []

    @property
    def high(self):
        if (
            self.daily_data is not None
            and not self.daily_data.empty
            and "high" in self.daily_data.columns
        ):
            return self.daily_data.high.values
        return []

    @property
    def low(self):
        if (
            self.daily_data is not None
            and not self.daily_data.empty
            and "low" in self.daily_data.columns
        ):
            return self.daily_data.low.values
        return []

    @property
    def open(self):
        if (
            self.daily_data is not None
            and not self.daily_data.empty
            and "open" in self.daily_data.columns
        ):
            return self.daily_data.open.values
        return []

    @property
    def close(self):
        if (
            self.daily_data is not None
            and not self.daily_data.empty
            and "close" in self.daily_data.columns
        ):
            return self.daily_data.close.values
        return []

    @property
    def change(self):
        return self.daily_data.change.values if self.daily_data is not None else []

    @property
    def capital(self):
        return self.info_data["capital"] if self.info_data is not None else None

    @property
    def macd(self):
        return self.daily_data.macd.values if self.daily_data is not None else []

    @property
    def macdsignal(self):
        return self.daily_data.macdsignal.values if self.daily_data is not None else []

    @property
    def macdhist(self):
        return self.daily_data.macdhist.values if self.daily_data is not None else []

    @property
    def ma5(self):
        return self.daily_data.ma5.values if self.daily_data is not None else []

    @property
    def ma10(self):
        return self.daily_data.ma10.values if self.daily_data is not None else []

    @property
    def ma20(self):
        return self.daily_data.ma20.values if self.daily_data is not None else []

    @property
    def ma60(self):
        return self.daily_data.ma60.values if self.daily_data is not None else []

    @property
    def bollinger_upper(self):
        return (
            self.daily_data.bollinger_upper.values
            if self.daily_data is not None
            else []
        )

    @property
    def bollinger_lower(self):
        return (
            self.daily_data.bollinger_lower.values
            if self.daily_data is not None
            else []
        )

    @property
    def wave(self):
        return self.daily_data.wave.values if self.daily_data is not None else []

    @property
    def trend(self):
        return self.daily_data.trend.values if self.daily_data is not None else []

    @property
    def k9(self):
        return self.daily_data.k9.values if self.daily_data is not None else []

    @property
    def d9(self):
        return self.daily_data.d9.values if self.daily_data is not None else []

    @property
    def adx(self):
        return self.daily_data.adx.values if self.daily_data is not None else []

    @property
    def adr(self):
        return self.daily_data.adr.values if self.daily_data is not None else []

    @property
    def plus_di(self):
        return self.daily_data.plus_di.values if self.daily_data is not None else []

    @property
    def minus_di(self):
        return self.daily_data.minus_di.values if self.daily_data is not None else []

    @property
    def foreign(self):
        return self.daily_data.foreign.values if self.daily_data is not None else []

    @property
    def investment_trust(self):
        return (
            self.daily_data.investment_trust.values
            if self.daily_data is not None
            else []
        )

    @property
    def dealer(self):
        return self.daily_data.dealer.values if self.daily_data is not None else []

    @property
    def foreign_holding_rate(self):
        return (
            self.daily_data.foreign_holding_rate.values
            if self.daily_data is not None
            else []
        )

    @property
    def investment_trust_holding_rate(self):
        return (
            self.daily_data.investment_trust_holding_rate.values
            if self.daily_data is not None
            else []
        )

    @property
    def dealer_holding_rate(self):
        return (
            self.daily_data.dealer_holding_rate.values
            if self.daily_data is not None
            else []
        )

    @property
    def sum_holding_rate(self):
        return (
            self.daily_data.sum_holding_rate.values
            if self.daily_data is not None
            else []
        )

    @property
    def major_investors(self):
        return (
            self.daily_data.major_investors.values
            if self.daily_data is not None
            else []
        )

    @property
    def agent_diff(self):
        return self.daily_data.agent_diff.values if self.daily_data is not None else []

    @property
    def skp5(self):
        return self.daily_data.skp5.values if self.daily_data is not None else []

    @property
    def skp20(self):
        return self.daily_data.skp20.values if self.daily_data is not None else []

    @property
    def three_line_diff(self):
        return (
            self.daily_data.three_line_diff.values
            if self.daily_data is not None
            else []
        )

    @property
    def four_line_diff(self):
        return (
            self.daily_data.four_line_diff.values if self.daily_data is not None else []
        )

    @property
    def lending_balance(self):
        return (
            self.daily_data.lending_balance.values
            if self.daily_data is not None
            else []
        )

    @property
    def borrowing_balance(self):
        return (
            self.daily_data.borrowing_balance.values
            if self.daily_data is not None
            else []
        )

    @property
    def balance_limit(self):
        if self.daily_data is not None and len(self.daily_data) > 1:
            return self.daily_data.balance_limit.values[-2]
        return None

    @property
    def season_upper(self):
        return self.season.close.values[-1] if hasattr(self, "season") else None

    @property
    def season_lower(self):
        return self.season.close.values[0] if hasattr(self, "season") else None
