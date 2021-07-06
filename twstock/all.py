# -*- coding: utf-8 -*-

import time
import os
import pandas as pd
import numpy as np
from datetime import date
from multiprocessing import Pool
from twstock import Stock
from twstock.stock import WantgooFetcher

INDEX = [
    'id',
    '最高價', '最大漲幅', '最低價', '最大跌幅', '收盤價', '漲跌幅', '成交量', '資本額',
    '波段天數', '波段漲跌幅', '趨勢天數', '趨勢漲跌幅',
    '波段高點', '高點差距', '波段低點', '低點差距',
    'K', 'D', 'ADX'
]
SKILL_INDEX = [
    '本日外本比', '本週外本比', '本月外本比', '外資10日買天數', '外資10日賣天數', '外資佔比',
    '本日投本比', '本週投本比', '本月投本比', '投信10日買天數', '投信10日賣天數', '投信佔比',
    '本日自本比', '本週自本比', '本月自本比', '自營10日買天數', '自營10日賣天數', '自營商佔比',
    '三大法人佔比',
    '本日主力買賣超', '本週主力買賣超', '本月主力買賣超', '主力10日買天數', '主力10日賣天數',
    '本日買賣家數差', '本週籌碼集中度', '本月籌碼集中度',
    '本日融資餘額比', '本週融資餘額比', '本月融資餘額比',
    '本日融券餘額比', '本週融券餘額比', '本月融券餘額比', 
    '本日券資比', '本週券資比', '本月券資比',
    '近期新高', '近期新低',
    '三線合一向上', '四線合一向上', '跳空向上', '長紅吞噬', '5 20黃金交叉', 'KD向上', 'MACD>0', '布林通道上軌', 'DMI向上', '多頭排列', '季線以上',
    '三線合一向下', '四線合一向下', '跳空向下', '長黑吞噬', '5 20死亡交叉', '空頭排列', '季線以下'
]

INDEX_COLUMN = {'id': '股票代碼'}
NAME_COLUMN = {'name': '股票名稱', 'industries_name': '上市/櫃', 'industries_shortName': '產業'}
INFO_COLUMN = {'outstanding_shares': '發行股數', 'cash_dividend': '現金股利', 'stock_dividend': '股票股利'}

ORGANIZATION = 'config/organization.csv'

class All():
    def __init__(self, initial_fetch: bool = True):
        self.fetcher = WantgooFetcher()

        # Init data
        if initial_fetch:
            self.get_all_stock_list()

    def get_all_stock_list(self):
        self.list = self.fetcher.get_all_stock_list()
        self.stock_list = pd.json_normalize(self.list, record_path='industries', record_prefix='industries_', meta=['id', 'name', 'type'])[['id', 'name', 'industries_name', 'industries_shortName']]
        self.stock_list = self.stock_list.assign(industries_name=self.stock_list.industries_name.str[:2])
        self.stock_list = pd.merge(self.stock_list, pd.read_csv(ORGANIZATION, dtype={'id': object, '集團': object}), how='left', on=['id']).rename(columns=NAME_COLUMN)

    def get_all_stock_parall(self):
        startTime = time.time()
        cpuCount = os.cpu_count()

        pool = Pool(cpuCount*3)

        results = pool.map(self.get_stock, map(lambda l: l['id'], self.list))
        name_list = {}
        skill_list = {}
        info_list = {}
        daily_list = {}
        category_list = self.stock_list.groupby("產業")

        for (id, skill, info, daily) in iter(results):
            skill_list[id] = skill
            info_list[id] = info
            daily_list[id] = daily
        
        for l in self.list:
            name_list[l['id']] = l
        
        skill_list_excel = pd.merge(self.stock_list, pd.DataFrame(dict(skill_list)).T, on=['id'])
        info_list_excel = (pd.merge(skill_list_excel.iloc[:, :12], pd.DataFrame(dict(info_list)).T, on=['id'])
            .rename(columns=INFO_COLUMN))
        extreme_list_excel = skill_list_excel[(skill_list_excel['最大漲幅'] > 9) | (skill_list_excel['最大跌幅'] < -9)]
        del info_list_excel['capital']

        endTime = time.time()
        print(endTime - startTime)

        with pd.ExcelWriter(date.today().strftime('%Y%m%d') + '.xlsx') as writer:
            skill_list_excel.rename(columns=INDEX_COLUMN).to_excel(writer, sheet_name='技術籌碼', index=False)
            info_list_excel.rename(columns=INDEX_COLUMN).to_excel(writer, sheet_name='基本面', index=False)
            extreme_list_excel.rename(columns=INDEX_COLUMN).to_excel(writer, sheet_name='極端漲跌', index=False)

        with pd.ExcelWriter(date.today().strftime('%Y%m%d') + '-graph.xlsx') as writer:
            workbook  = writer.book
            worksheet = workbook.add_worksheet('目錄')

            worksheet.write_formula('A1', '=HYPERLINK("#技術籌碼!A1", "技術籌碼")')
            worksheet.write_formula('A2', '=HYPERLINK("#基本面!A1", "基本面")')
            worksheet.write_formula('A3', '=HYPERLINK("#極端漲跌!A1", "極端漲跌")')
            i = 4
            for category_name, category in category_list:
                worksheet.write_formula('A'+str(i), '=HYPERLINK("#' + category_name + '!A1", "'+ category_name + '")')
                i += 1

            skill_list_excel.rename(columns=INDEX_COLUMN).to_excel(writer, sheet_name='技術籌碼', index=False)
            info_list_excel.rename(columns=INDEX_COLUMN).to_excel(writer, sheet_name='基本面', index=False)
            extreme_list_excel.rename(columns=INDEX_COLUMN).to_excel(writer, sheet_name='極端漲跌', index=False)

            for category_name, category in category_list:
                category.to_excel(writer, category_name, index=False)

            for id, daily in daily_list.items():
                daily.rename(columns=INDEX_COLUMN).to_excel(writer, id, index=False)

            for category_name, category in category_list:
                worksheet = writer.sheets[category_name]
                chart = workbook.add_chart({'type': 'line'})
                for _, c in category.iterrows():
                    chart.add_series({
                        'name': c['id'] + ' ' + c['股票名稱'],
                        'categories': '='+c['id']+'!$A$2:$A$61',
                        'values': '='+c['id']+'!$B$2:$B$61'
                    })

                chart.set_x_axis({'name': 'Index', 'position_axis': 'on_tick'})
                chart.set_x_axis({'name': '日期'})
                chart.set_size({'width': 800, 'height': 600})

                worksheet.insert_chart('F3', chart)
                worksheet.write_formula('G1', '=HYPERLINK("#目錄!A1", "回目錄")')

    def sum_days(self, data, days):
        result = sum(data[days * -1:])
        return result if result != 0 else None

    def get_stock(self, sid: int):
        stock = Stock(sid)

        print(stock.sid)

        if len(stock.close) < 60:
            return (stock.sid, pd.Series(index=INDEX + SKILL_INDEX), stock.info, pd.DataFrame({}))

        wave_days = stock.continuous_trend_days(stock.wave)
        trend_days = stock.continuous_trend_days(stock.trend)

        check = [
            stock.sid,
            stock.high[-1],
            stock.calc_change(stock.high[-1], stock.close[-2]),
            stock.low[-1],
            stock.calc_change(stock.low[-1], stock.close[-2]),
            stock.close[-1], 
            stock.change[-1],
            stock.volume[-1],
            stock.info.capital,

            wave_days,
            stock.calc_change(stock.close[-1], stock.close[-1 * (abs(wave_days) + 1)]),
            trend_days,
            stock.calc_change(stock.close[-1], stock.close[-1 * (abs(trend_days) + 1)]),
            stock.season_upper,
            stock.calc_change(stock.close[-1], stock.season_upper),
            stock.season_lower,
            stock.calc_change(stock.close[-1], stock.season_lower),
            stock.k9[-1],
            stock.d9[-1],
            stock.adx[-1],

            stock.foreign[-1] if stock.foreign[-1] != 0 else None,
            self.sum_days(stock.foreign ,5),
            self.sum_days(stock.foreign ,20),
            stock.buy_10days(stock.foreign),
            stock.sell_10days(stock.foreign),
            stock.foreign_holding_rate[-1],
            
            stock.investment_trust[-1] if stock.investment_trust[-1] != 0 else None,
            self.sum_days(stock.investment_trust ,5),
            self.sum_days(stock.investment_trust ,20),
            stock.buy_10days(stock.investment_trust),
            stock.sell_10days(stock.investment_trust),
            stock.investment_trust_holding_rate[-1],

            stock.dealer[-1] if stock.dealer[-1] != 0 else None,
            self.sum_days(stock.dealer ,5),
            self.sum_days(stock.dealer ,20),
            stock.buy_10days(stock.dealer),
            stock.sell_10days(stock.dealer),
            stock.dealer_holding_rate[-1],
            stock.sum_holding_rate[-1],

            stock.major_investors[-1] if stock.major_investors[-1] != 0 else None,
            self.sum_days(stock.major_investors ,5),
            self.sum_days(stock.major_investors ,20),
            stock.buy_10days(stock.major_investors),
            stock.sell_10days(stock.major_investors),
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

                round(borrowing_balance_today / lending_balance_today * 100, 2) if lending_balance_today != 0 else 0,
                round(stock.borrowing_balance[-5] / stock.lending_balance[-5] * 100, 2) if stock.lending_balance[-5] != 0 else 0,
                round(stock.borrowing_balance[-20] / stock.lending_balance[-20] * 100, 2) if stock.lending_balance[-20] != 0 else 0
            ]
        else:
            check = check + [None , None, None, None, None, None, None, None, None]

        check = check + [
            stock.is_upper(),
            stock.is_lower(),
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

        daily = pd.DataFrame(
            {
                'date': stock.date[-60:],
                'close': stock.close[-60:],
                'volume': stock.volume[-60:],
            })

        return (stock.sid, pd.Series(check, index=INDEX + SKILL_INDEX), stock.info, daily)

def init(l):
    global lock
    lock = l
