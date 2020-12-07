import itertools
import time
import os
import statistics
import pandas as pd
import numpy as np
from datetime import date
from multiprocessing import Pool
from twstock import Stock
from twstock.stock import WantgooFetcher

INDEX = [
    'id',
    '收盤價', '漲跌幅', '成交量',
    '波段天數', '波段漲跌幅', '趨勢天數', '趨勢漲跌幅',
    '本日外本比', '本週外本比', '本月外本比', '外資佔比',
    '本日投本比', '本週投本比', '本月投本比', '投信佔比',
    '本日自本比', '本週自本比', '本月自本比', '自營商佔比',
    '三大法人佔比',
    '本日主力買賣超', '本週主力買賣超', '本月主力買賣超',
    '本日買賣家數差', '本週籌碼集中度', '本月籌碼集中度',
    '本日融資餘額比', '本週融資餘額比', '本月融資餘額比',
    '本日融券餘額比', '本週融券餘額比', '本月融券餘額比', 
    '本日券資比', '本週券資比', '本月券資比',
    '三線合一向上', '跳空向上', '長紅吞噬', 'KD向上', 'MACD>0', '布林通道上軌',
    '三線合一向下', '長黑吞噬', '跳空向下', '空頭',
    'URL'
]

ORGANIZATION = 'config/organization.csv'

class All():
    def __init__(self, initial_fetch: bool = True):
        self.fetcher = WantgooFetcher()

        # Init data
        if initial_fetch:
            self.getAllStockList()

    def getAllStockList(self):
        self.list = self.fetcher.getAllStockList()

    def getStockList(self):
        return self.list

    def getAllStockParall(self):
        startTime = time.time()
        cpuCount = os.cpu_count()

        pool = Pool(cpuCount*3)

        results = pool.map(self.getStock, map(lambda l: l['id'], self.list))

        self.data = pd.merge(pd.json_normalize(self.list)[['id', 'name', 'industry.shortName']], pd.read_csv(ORGANIZATION, dtype={'id': object, '集團': object}), how='left', on=['id'])
        self.data = pd.merge(self.data, pd.DataFrame(dict(results)).T, on=['id']).rename(columns={"id": "股票代碼", "name": "股票名稱", "industry.shortName": "產業"})
        endTime = time.time()
        print(endTime - startTime)
        self.data.to_excel(date.today().strftime("%Y%m%d") + '.xlsx', sheet_name='Sheet1', index=False)

    def sum_days(self, data, days):
        result = sum(data[days * -1:])
        return result if result != 0 else None

    def getStock(self, sid: int):
        stock = Stock(sid)

        print(stock.sid)

        if len(stock.close) < 60:
            return (stock.sid, pd.Series(index=INDEX))

        wave_days = stock.continuous_trend_days(stock.wave)
        trend_days = stock.continuous_trend_days(stock.trend)
        wave_days
        print(trend_days)

        check = [
            stock.sid,
            stock.close[-1], 
            stock.change[-1],
            stock.volume[-1],

            wave_days,
            stock.calc_change(stock.close[-1], stock.close[-1 * abs(wave_days)]),
            trend_days,
            stock.calc_change(stock.close[-1], stock.close[-1 * abs(trend_days)]),

            stock.foreign[-1] if stock.foreign[-1] != 0 else None,
            self.sum_days(stock.foreign ,5),
            self.sum_days(stock.foreign ,20),
            stock.foreign_holding_rate[-1],
            
            stock.investment_trust[-1] if stock.investment_trust[-1] != 0 else None,
            self.sum_days(stock.investment_trust ,5),
            self.sum_days(stock.investment_trust ,20),
            stock.investment_trust_holding_rate[-1],

            stock.dealer[-1] if stock.dealer[-1] != 0 else None,
            self.sum_days(stock.dealer ,5),
            self.sum_days(stock.dealer ,20),
            stock.dealer_holding_rate[-1],
            stock.sum_holding_rate[-1],

            stock.major_investors[-1] if stock.major_investors[-1] != 0 else None,
            self.sum_days(stock.major_investors ,5),
            self.sum_days(stock.major_investors ,20),
            stock.agent_diff[-1],
            stock.skp5[-1],
            stock.skp20[-1],
        ]

        if stock.balance_limit != 0:
            # to avoid 0
            lending_balance_today = stock.lending_balance[-1] if stock.lending_balance[-1] != 0 else stock.lending_balance[-2]
            borrowing_balance_today = stock.borrowing_balance[-1] if stock.borrowing_balance[-1] != 0 else stock.borrowing_balance[-2]

            check = check + [
                round(lending_balance_today / stock.balance_limit * 100, 2),
                round(stock.lending_balance[-5] / stock.balance_limit * 100, 2),
                round(stock.lending_balance[-20] / stock.balance_limit * 100, 2),

                round(borrowing_balance_today / stock.balance_limit * 100, 2),
                round(stock.borrowing_balance[-5] / stock.balance_limit * 100, 2),
                round(stock.borrowing_balance[-20] / stock.balance_limit * 100, 2),

                round(borrowing_balance_today / lending_balance_today * 100, 2),
                round(stock.borrowing_balance[-5] / stock.lending_balance[-5] * 100, 2),
                round(stock.borrowing_balance[-20] / stock.lending_balance[-20] * 100, 2)
            ]
        else:
            check = check + [None , None, None, None, None, None, None, None, None]

        if stock.wave[-1] > 0:
            check = check + [
                stock.up_three_line(),
                stock.up_jump_line(),
                stock.long_up(),
                stock.up_kd(),
                stock.up_macd(),
                stock.up_bollinger(),
                None,
                None,
                None,
                None
            ]
        else:
            check = check + [
                None,
                None,
                None,
                None,
                None,
                None,
                stock.down_three_line(),
                stock.long_down(),
                stock.down_jump_line(),
                stock.short()
            ]

        check = check + ['https://histock.tw/stock/tv/tvchart.aspx?no='+str(sid)]
        
        return (stock.sid, pd.Series(check, index=INDEX))

def init(l):
    global lock
    lock = l
