# -*- coding: utf-8 -*-

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
    '收盤價', '漲跌幅', '成交量', '資本額',
    '波段天數', '波段漲跌幅', '趨勢天數', '趨勢漲跌幅',
    'K', 'D', 'ADX'
]
SKILL_INDEX = [
    '本日外本比', '本週外本比', '本月外本比', '外資佔比',
    '本日投本比', '本週投本比', '本月投本比', '投信佔比',
    '本日自本比', '本週自本比', '本月自本比', '自營商佔比',
    '三大法人佔比',
    '本日主力買賣超', '本週主力買賣超', '本月主力買賣超',
    '本日買賣家數差', '本週籌碼集中度', '本月籌碼集中度',
    '本日融資餘額比', '本週融資餘額比', '本月融資餘額比',
    '本日融券餘額比', '本週融券餘額比', '本月融券餘額比', 
    '本日券資比', '本週券資比', '本月券資比',
    '三線合一向上', '四線合一向上', '跳空向上', '長紅吞噬', '5 20黃金交叉', 'KD向上', 'MACD>0', '布林通道上軌', 'DMI向上', '多頭排列', '季線以上',
    '三線合一向下', '四線合一向下', '跳空向下', '長黑吞噬', '5 20死亡交叉', '空頭排列', '季線以下'
]

INDEX_COLUMN = {'id': '股票代碼'}
NAME_COLUMN = {'name': '股票名稱', 'industry.name': '上市/櫃', 'industry.shortName': '產業'}
INFO_COLUMN = {'outstanding_shares': '發行股數'}

ORGANIZATION = 'config/organization.csv'

class All():
    def __init__(self, initial_fetch: bool = True):
        self.fetcher = WantgooFetcher()

        # Init data
        if initial_fetch:
            self.get_all_stock_list()

    def get_all_stock_list(self):
        self.list = self.fetcher.get_all_stock_list()

    def get_all_stock_parall(self):
        startTime = time.time()
        cpuCount = os.cpu_count()

        pool = Pool(cpuCount*3)

        results = pool.map(self.get_stock, map(lambda l: l['id'], self.list))
        skill_list = {}
        info_list = {}
        for (id, skill, info) in iter(results):
            skill_list[id] = skill
            info_list[id] = info
        
        self.stock_list = pd.json_normalize(self.list)[['id', 'name', 'industry.name', 'industry.shortName']].rename(columns={'industry.name': 'market'})
        # self.stock_list['market'][]
        # stock_list.assign(market=stock_list.market.astype(str)[-2:])
        self.stock_list = pd.merge(self.stock_list, pd.read_csv(ORGANIZATION, dtype={'id': object, '集團': object}), how='left', on=['id']).rename(columns=NAME_COLUMN)
        skill_list = pd.merge(self.stock_list, pd.DataFrame(dict(skill_list)).T, on=['id'])
        info_list = (pd.merge(skill_list.iloc[:, :12], pd.DataFrame(dict(info_list)).T, on=['id'])
            .rename(columns=INFO_COLUMN))

        del info_list['capital']

        endTime = time.time()
        print(endTime - startTime)
        with pd.ExcelWriter(date.today().strftime('%Y%m%d') + '.xlsx') as writer:
            skill_list.rename(columns=INDEX_COLUMN).to_excel(writer, sheet_name='技術籌碼', index=False)
            info_list.rename(columns=INDEX_COLUMN).to_excel(writer, sheet_name='基本面', index=False)

    def sum_days(self, data, days):
        result = sum(data[days * -1:])
        return result if result != 0 else None

    def get_stock(self, sid: int):
        stock = Stock(sid)

        print(stock.sid)

        if len(stock.close) < 60:
            return (stock.sid, pd.Series(index=INDEX + SKILL_INDEX), stock.info)

        wave_days = stock.continuous_trend_days(stock.wave)
        trend_days = stock.continuous_trend_days(stock.trend)

        check = [
            stock.sid,
            stock.close[-1], 
            stock.change[-1],
            stock.volume[-1],
            stock.info.capital,

            wave_days,
            stock.calc_change(stock.close[-1], stock.close[-1 * (abs(wave_days) + 1)]),
            trend_days,
            stock.calc_change(stock.close[-1], stock.close[-1 * (abs(trend_days) + 1)]),
            stock.k9[-1],
            stock.d9[-1],
            stock.adx[-1],

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

        check = check + [
            stock.up_three_line(),
            stock.up_four_line(),
            stock.up_jump_line(),
            stock.long_up(),
            stock.up_cross_ma5_ma20(),
            stock.up_kd(),
            stock.up_macd(),
            stock.up_bollinger(),
            stock.up_dmi(),
            stock.long(),
            stock.up_session(),
            stock.down_three_line(),
            stock.down_four_line(),
            stock.down_jump_line(),
            stock.long_down(),
            stock.down_cross_ma5_ma20(),
            stock.short(),
            stock.down_session()
        ]

        return (stock.sid, pd.Series(check, index=INDEX + SKILL_INDEX), stock.info)

def init(l):
    global lock
    lock = l
