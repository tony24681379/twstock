import itertools
import time
import os
import pandas as pd
import numpy as np
from datetime import date
from multiprocessing import Pool
from twstock import Stock
from twstock.stock import WantgooFetcher

INDEX = ['收盤價', '投信買賣超', '三線合一', '跳空向上', '長紅吞噬', 'KD向上', 'MACD>0', '布林通道上軌', '長黑吞噬', '跳空向下', 'URL']

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

        results = pool.map(self.getStock, self.list)
        self.data = pd.DataFrame(dict(results)).T
        endTime = time.time()
        print(endTime - startTime)
        self.data.to_csv(date.today().strftime("%Y%m%d") + '.csv')

    def getStock(self, sid: int):
        stock = Stock(sid)

        print(stock.sid)

        if len(stock.close) == 0 or stock.data.volume.mean() < 500:
            check = [
                None,
                None,
                None,
                None,
                None,
                None,
                None,
                None,
                None,
                None,
                None
            ]
            return (stock.sid, pd.Series(check, index=INDEX))

        check = [stock.close[-1], stock.institutional_investors[-1] if stock.institutional_investors[-1] > 0 else None]

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
            ]

        check = check + ['https://histock.tw/stock/tv/tvchart.aspx?no='+str(sid)]
        
        return (stock.sid, pd.Series(check, index=INDEX))

def init(l):
    global lock
    lock = l
