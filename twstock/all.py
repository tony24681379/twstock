import itertools
import time
import os
from multiprocessing import Pool
from twstock import Stock
from twstock.stock import WantgooFetcher

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

        results = list(itertools.chain.from_iterable(results))
        results = sorted(results, key=lambda s: s[0])
        endTime = time.time()
        print(endTime - startTime)

        total = {}

        for (sid, date, typ) in results:
            if sid in total:
                total[sid].add((date, typ))
            else:
                total[sid] = set((date, typ))

        for sid in total:
            print(sid, total[sid])
            print('https://histock.tw/stock/tv/tvchart.aspx?no='+str(sid))

    def getStock(self, sid: int):
        stock = Stock(sid)

        print(stock.sid)

        if len(stock.close) == 0 or stock.data.volume.mean() < 500:
            return []

        result = []
        up_check = []
        down_check = []

        if stock.wave[-1] > 0:
            up_check = [
                stock.up_three_line(),
                stock.up_jump_line(),
                stock.long_up(),
                stock.up_kd(),
                stock.up_macd(),
                stock.up_bollinger()
                # stock.continuous_days()
            ]

        if stock.wave[-1] < 0:
            down_check = [
                stock.long_down(),
                stock.down_jump_line()
                # stock.continuous_days()
            ]

        for index, v in enumerate(up_check):
            if v:
                result.append(v)

        for index, v in enumerate(down_check):
            if v:
                result.append(v)

        return result


def init(l):
    global lock
    lock = l
