# -*- coding: utf-8 -*-

import sys
import datetime
import urllib.parse
import time
import os
from twstock.proxy import get_proxies
import pandas as pd
from threading import Lock
from collections import namedtuple
from twstock.proxy import get_proxies
import talib
from talib import MA_Type

try:
    from json.decoder import JSONDecodeError
except ImportError:
    JSONDecodeError = ValueError

import requests

try:
    from . import analytics
    from .codes import codes
except ImportError as e:
    if e.name == 'lxml':
        # Fix #69
        raise e
    import analytics
    from codes import codes


WANTGOO_BASE_URL = 'https://www.wantgoo.com/'
DATATUPLE = namedtuple('Data', ['date', 'volume', 'open', 'high', 'low', 'close'])

class BaseFetcher(object):
    def fetch(self, year, month, sid, retry):
        pass

    def _make_datatuple(self, data):
        pass

    def purify(self, original_data):
        pass

class WantgooFetcher(BaseFetcher):
    REPORT_URL = WANTGOO_BASE_URL
    HEADERS = {'User-Agent':'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/86.0.4240.183 Safari/537.36'}

    def fetch(self, sid: str, num: int, retry: int=5):
        params = {'before': int(time.mktime(datetime.datetime.now().timetuple()))*1000, 'top': num}
        for retry_i in range(retry):
            candlesticks_response = requests.get(
                self.REPORT_URL + 'investrue/' + sid + '/historical-daily-candlesticks',
                params = params,
                headers = self.HEADERS,
                proxies=get_proxies())

            institutional_investors_response = requests.get(
                self.REPORT_URL + 'stock/' + sid + '/institutional-investors/investment-trust/historical-net-buy-sell',
                headers = self.HEADERS,
                proxies=get_proxies())

            try:
                candlesticks = candlesticks_response.json()[::-1]
                institutional_investors = institutional_investors_response.json()[::-1]
            except JSONDecodeError:
                continue
            else:
                break
        else:
            # Fail in all retries
            print(sid + 'fail')
            candlesticks = []
            institutional_investors = []

        return self.purify(candlesticks, institutional_investors)

    def purify(self, candlesticks, institutional_investors):
        candlesticks_data = pd.DataFrame(candlesticks, columns=['volume', 'open', 'close', 'high', 'low'])
        institutional_investors_data = pd.DataFrame(institutional_investors, columns=['date', 'netBuySell']).rename(columns={"netBuySell": "institutional_investors"})
        candlesticks_data['date'] = [datetime.datetime.fromtimestamp(d['tradeDate']/1000) for d in candlesticks]
        institutional_investors_data['date'] = [datetime.datetime.fromtimestamp(d['date']/1000) for d in institutional_investors]
        institutional_investors_data['institutional_investors']

        data = pd.merge(candlesticks_data, institutional_investors_data, how='left', on=['date'])
        data['institutional_investors'] = data['institutional_investors'].fillna(0.0)

        return data[['date', 'volume', 'open', 'close', 'high', 'low', 'institutional_investors']]

    def getAllStockList(self, retry: int=5):
        for retry_i in range(retry):
            r = requests.get(self.REPORT_URL+'investrue/all-alive',
                headers = self.HEADERS,
                proxies=get_proxies()
                )
            try:
                data = r.json()
            except JSONDecodeError:
                print('error')
                continue
            else:
                break
        else:
            # Fail in all retries
            print('fail')
            data = []

        filtered = filter(lambda l: l['type'] in ['Stock'], data)
        sids = map(lambda l: l['id'], filtered)
        return list(sids)

class Stock(analytics.Analytics):
    def __init__(self, sid: str, load_data: bool=True):
        self.sid = sid
        self.fetcher = WantgooFetcher()
        self.path = "data/" + self.sid + ".csv"

        if load_data:
            if os.path.isfile(self.path):
                self.read_csv(self.path)
            else:
                self.fetch(490)
                self.to_csv(self.path)

        if len(self.close) == 0:
            return

        self.calc_base()

    def read_csv(self, path):
        self.data = pd.read_csv(path,index_col=0,parse_dates=True)

    def to_csv(self, path):
        self.data.to_csv(path)

    def getAllStockList(self):
        return self.fetcher.getAllStockList()

    def fetch(self, num):
        self.data = self.fetcher.fetch(self.sid, num)

    def calc_change(self, today, yesterday):
        return round((today-yesterday)/yesterday*100,2)

    def calc_base(self):
        bollinger_upper, _, bollinger_lower = talib.BBANDS(self.close, 20)
        k9, d9 = talib.STOCH(self.high, self.low, self.close)
        macd, macdsignal, macdhist = talib.MACD(self.close)

        change = [0]
        for i in range(1, len(self.close)):
            change.append(self.calc_change(self.close[i],self.close[i-1]))

        self.data['bollinger_upper'] = bollinger_upper
        self.data['bollinger_lower'] = bollinger_lower
        self.data['change'] = change
        self.data['ma5'] = talib.MA(self.close, timeperiod=5)
        self.data['ma10'] = talib.MA(self.close, timeperiod=10)
        self.data['ma20'] = talib.MA(self.close, timeperiod=20)
        self.data['ma60'] = talib.MA(self.close, timeperiod=60)
        self.data['k9'] = k9
        self.data['d9'] = d9
        self.data['macd'] = macd
        self.data['macdsignal'] = macdsignal
        self.data['macdhist'] = macdhist

        self.calc_trend()
        self.data = self.data.dropna(how='any')

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

        if wave[-1] == 1 and is_up is True:
            wave[-1] = 2
        if wave[-1] == -1 and is_up is False:
            wave[-1] = -2

        trend = []
        high_point = 0
        low_point = 0
        high = False
        low = False
        for i in range(0, len(wave)):
            trend.append(0)
            if wave[i] == 2:
                if self.high[i] > self.high[high_point]:
                    high = True
                    if low is True:
                        trend[low_point] = -3
                        low = False
                else:
                    low = True
                    if high is True:
                        trend[high_point] = 3
                        high = False

                high_point = i

            if wave[i] == -2:
                if self.low[i] < self.low[low_point]:
                    low = True
                    if high is True:
                        trend[high_point] = 3
                        high = False
                else:
                    high = True
                    if low is True:
                        trend[low_point] = -3
                        low = False

                low_point = i

        if high is True:
            trend[high_point] = 3
        if low is True:
            trend[low_point] = -3

        for i in range(1, len(trend)):
            if trend[i] == 0:
                trend[i] = trend[i-1]

        self.data['wave'] = wave
        self.data['trend'] = trend

    @property
    def date(self):
        return self.data.date.values

    @property
    def volume(self):
        return self.data.volume.values

    @property
    def high(self):
        return self.data.high.values

    @property
    def low(self):
        return self.data.low.values

    @property
    def open(self):
        return self.data.open.values

    @property
    def close(self):
        return self.data.close.values

    @property
    def change(self):
        return self.data.change.values

    @property
    def macd(self):
        return self.data.macd.values

    @property
    def macdsignal(self):
        return self.data.macdsignal.values

    @property
    def macdhist(self):
        return self.data.macdhist.values

    @property
    def ma5(self):
        return self.data.ma5.values

    @property
    def ma10(self):
        return self.data.ma10.values

    @property
    def ma20(self):
        return self.data.ma20.values

    @property
    def bollinger_upper(self):
        return self.data.bollinger_upper.values

    @property
    def bollinger_lower(self):
        return self.data.bollinger_lower.values

    @property
    def wave(self):
        return self.data.wave.values

    @property
    def trend(self):
        return self.data.trend.values

    @property
    def k9(self):
        return self.data.k9.values

    @property
    def d9(self):
        return self.data.d9.values
