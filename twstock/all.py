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
    '收盤價', '漲跌幅',
    '本日外資買賣超', '本週外資買賣超', '本月外資買賣超',
    '本日投信買賣超', '本週投信買賣超', '本月投信買賣超',
    '本日主力買賣超', '本週主力買賣超', '本月主力買賣超',
    '本日買賣家數差', '本週籌碼集中度', '本月籌碼集中度',
    '三線合一', '跳空向上', '長紅吞噬', 'KD向上', 'MACD>0', '布林通道上軌', '長黑吞噬',
    '跳空向下', '空頭',
    'URL'
]

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

        pool = Pool(cpuCount)

        results = pool.map(self.getStock, map(lambda l: l['id'], self.list))
        self.data = pd.merge(pd.DataFrame(self.list, columns=['id', 'name']), pd.DataFrame(dict(results)).T, on=['id']).rename(columns={"id": "股票代碼", "name": "股票名稱"})
        endTime = time.time()
        print(endTime - startTime)
        self.data.to_excel(date.today().strftime("%Y%m%d") + '.xlsx', sheet_name='Sheet1', index=False)

    def sum_days(self, data, days):
        result = sum(data[days * -1:])
        return result/1000 if result != 0 else None

    def getStock(self, sid: int):
        stock = Stock(sid)

        print(stock.sid)

        if len(stock.close) < 60 or statistics.mean(stock.volume[:-10]) < 500:
            return (stock.sid, pd.Series(index=INDEX))

        check = [
            stock.sid,
            stock.close[-1], 
            stock.change[-1],
            stock.foreign[-1]/1000 if stock.foreign[-1] != 0 else None,
            self.sum_days(stock.foreign ,5),
            self.sum_days(stock.foreign ,20),
            stock.investment_trust[-1]/1000 if stock.investment_trust[-1] != 0 else None,
            self.sum_days(stock.investment_trust ,5),
            self.sum_days(stock.investment_trust ,20),
            stock.major_investors[-1]/1000 if stock.major_investors[-1] != 0 else None,
            self.sum_days(stock.major_investors ,5),
            self.sum_days(stock.major_investors ,20),
            stock.agent_diff[-1],
            stock.skp5[-1],
            stock.skp20[-1]
        ]

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
                stock.long_down(),
                stock.down_jump_line(),
                stock.short()
            ]

        check = check + ['https://histock.tw/stock/tv/tvchart.aspx?no='+str(sid)]
        
        return (stock.sid, pd.Series(check, index=INDEX))

def init(l):
    global lock
    lock = l
