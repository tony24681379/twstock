# -*- coding: utf-8 -*-

import sys
import datetime
# import urllib.parse
import os
import statistics
import pandas as pd
from threading import Lock
import talib
from twstock.wantgoo import WantgooFetcher
from talib import MA_Type

try:
    from . import analytics
    from .codes import codes
except ImportError as e:
    if e.name == 'lxml':
        # Fix #69
        raise e
    import analytics
    from codes import codes


INFO_PATH = 'info/'
DAILY_PATH = 'daily/'

class Stock(analytics.Analytics):
    def __init__(self, sid: str, load_data: bool=True):
        self.sid = sid
        self.fetcher = WantgooFetcher()
        self.info_path = INFO_PATH + self.sid + ".csv"
        self.daily_path = DAILY_PATH + self.sid + ".csv"

        if load_data:
            if os.path.isfile(self.info_path):
                self.info_data = self.read_csv(self.info_path, True)
            else:
                self.info_data = self.fetch_info()
                self.to_csv(self.info_path, self.info_data)

            if os.path.isfile(self.daily_path):
                self.daily_data = self.read_csv(self.daily_path)
            else:
                self.daily_data = self.fetch_daily(490)
                self.to_csv(self.daily_path, self.daily_data)

        if len(self.close) == 0:
            return

        self.calc_base()

    def read_csv(self, path, is_squeeze: bool=False):
        if is_squeeze:
            return pd.read_csv(path, index_col=0, squeeze=is_squeeze, dtype='object')
        else:
            return pd.read_csv(path, index_col=0, parse_dates=True)

    def to_csv(self, path, data):
        data.to_csv(path)

    def get_all_stock_list(self):
        return self.fetcher.get_all_stock_list()

    def fetch_info(self):
        return self.fetcher.fetch_info(self.sid)

    def fetch_daily(self, num):
        return self.fetcher.fetch_daily(self.sid, num, float(self.info_data.outstanding_shares)/1000)

    def calc_change(self, after, before):
        return round((after - before)/before * 100, 2)

    def calc_base(self):
        self.info_data['capital'] = round(self.close[-1] * float(self.info_data.outstanding_shares) / 100000000, 2)
        self.info_data['PER'] = round(self.close[-1] / float(self.info_data['PER']), 2) if self.info_data['PER'] is not None else None

        bollinger_upper, _, bollinger_lower = talib.BBANDS(self.close, 20)
        k9, d9 = talib.STOCH(self.high, self.low, self.close)
        macd, macdsignal, macdhist = talib.MACD(self.close)

        change = [0]
        for i in range(1, len(self.close)):
            change.append(self.calc_change(self.close[i],self.close[i-1]))

        self.daily_data['bollinger_upper'] = bollinger_upper
        self.daily_data['bollinger_lower'] = bollinger_lower
        self.daily_data['change'] = change
        self.daily_data['ma5'] = talib.MA(self.close, timeperiod=5)
        self.daily_data['ma10'] = talib.MA(self.close, timeperiod=10)
        self.daily_data['ma20'] = talib.MA(self.close, timeperiod=20)
        self.daily_data['ma60'] = talib.MA(self.close, timeperiod=60)
        self.daily_data['k9'] = k9
        self.daily_data['d9'] = d9
        self.daily_data['macd'] = macd
        self.daily_data['macdsignal'] = macdsignal
        self.daily_data['macdhist'] = macdhist
        self.daily_data['adx'] = talib.ADX(self.high, self.low, self.close, timeperiod=14)
        self.daily_data['adxr'] = talib.ADXR(self.high, self.low, self.close, timeperiod=14)
        self.daily_data['plus_di'] = talib.PLUS_DI(self.high, self.low, self.close, timeperiod=14)
        self.daily_data['minus_di'] = talib.MINUS_DI(self.high, self.low, self.close, timeperiod=14)

        self.calc_line_diff()
        self.calc_trend()
        self.daily_data = self.daily_data.dropna(how='any')
    
    def calc_line_diff(self):
        three_line_diff = []
        four_line_diff = []
        for i in range(0, len(self.close)):
            sub = (max(self.ma5[i] , self.ma10[i], self.ma20[i]) - min(self.ma5[i] , self.ma10[i], self.ma20[i]))
            avg = statistics.mean([self.ma5[i], self.ma10[i], self.ma20[i]])
            if avg == 0:
                return None

            three_line_diff.append(sub/avg)

            sub = (max(self.ma5[i] , self.ma10[i], self.ma20[i], self.ma60[i]) - min(self.ma5[i] , self.ma10[i], self.ma20[i], self.ma60[i]))
            avg = statistics.mean([self.ma5[i], self.ma10[i], self.ma20[i], self.ma60[i]])
            if avg == 0:
                return None

            four_line_diff.append(sub/avg)

        self.daily_data['three_line_diff'] = three_line_diff
        self.daily_data['four_line_diff'] = four_line_diff

    def calc_trend(self):
        high = -sys.maxsize-1
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
                    high = -sys.maxsize-1
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
                trend[-i] = trend[-i+1]

        self.daily_data['wave'] = wave
        self.daily_data['trend'] = trend

    @property
    def info(self):
        return self.info_data

    @property
    def date(self):
        return self.daily_data.date.values

    @property
    def volume(self):
        return self.daily_data.volume.values

    @property
    def high(self):
        return self.daily_data.high.values

    @property
    def low(self):
        return self.daily_data.low.values

    @property
    def open(self):
        return self.daily_data.open.values

    @property
    def close(self):
        return self.daily_data.close.values

    @property
    def change(self):
        return self.daily_data.change.values

    @property
    def capital(self):
        return self.info_data['capital']

    @property
    def macd(self):
        return self.daily_data.macd.values

    @property
    def macdsignal(self):
        return self.daily_data.macdsignal.values

    @property
    def macdhist(self):
        return self.daily_data.macdhist.values

    @property
    def ma5(self):
        return self.daily_data.ma5.values

    @property
    def ma10(self):
        return self.daily_data.ma10.values

    @property
    def ma20(self):
        return self.daily_data.ma20.values

    @property
    def ma60(self):
        return self.daily_data.ma60.values

    @property
    def bollinger_upper(self):
        return self.daily_data.bollinger_upper.values

    @property
    def bollinger_lower(self):
        return self.daily_data.bollinger_lower.values

    @property
    def wave(self):
        return self.daily_data.wave.values

    @property
    def trend(self):
        return self.daily_data.trend.values

    @property
    def k9(self):
        return self.daily_data.k9.values

    @property
    def d9(self):
        return self.daily_data.d9.values

    @property
    def adx(self):
        return self.daily_data.adx.values

    @property
    def adr(self):
        return self.daily_data.adr.values
    
    @property
    def plus_di(self):
        return self.daily_data.plus_di.values
    
    @property
    def minus_di(self):
        return self.daily_data.minus_di.values

    @property
    def foreign(self):
        return self.daily_data.foreign.values

    @property
    def investment_trust(self):
        return self.daily_data.investment_trust.values

    @property
    def dealer(self):
        return self.daily_data.dealer.values

    @property
    def foreign_holding_rate(self):
        return self.daily_data.foreign_holding_rate.values

    @property
    def investment_trust_holding_rate(self):
        return self.daily_data.investment_trust_holding_rate.values

    @property
    def dealer_holding_rate(self):
        return self.daily_data.dealer_holding_rate.values

    @property
    def sum_holding_rate(self):
        return self.daily_data.sum_holding_rate.values

    @property
    def major_investors(self):
        return self.daily_data.major_investors.values

    @property
    def agent_diff(self):
        return self.daily_data.agent_diff.values

    @property
    def skp5(self):
        return self.daily_data.skp5.values

    @property
    def skp20(self):
        return self.daily_data.skp20.values

    @property
    def three_line_diff(self):
        return self.daily_data.three_line_diff.values

    @property
    def four_line_diff(self):
        return self.daily_data.four_line_diff.values

    @property
    def lending_balance(self):
        return self.daily_data.lending_balance.values

    @property
    def borrowing_balance(self):
        return self.daily_data.borrowing_balance.values

    @property
    def balance_limit(self):
        return self.daily_data.balance_limit.values[-2]