# -*- coding: utf-8 -*-

import sys
import statistics
import talib
from talib import MA_Type
import numpy as np
from collections import namedtuple

conter_tuple = namedtuple('counter',('index', 'is_up'))

class Analytics(object):
    length = 30

    def continuous(self, data):
        diff = [1 if data[-i] >= data[-i - 1] else -1 for i in range(1, len(data))]
        cont = 0
        for v in diff:
            if v == diff[0]:
                cont += 1
            else:
                break
        return cont * diff[0]

    def pivot_point(self):
        self.is_up = True
        conter = []

        diff = []
        for i in range(1, len(self.close) - 2):
            if self.pivots[i] == 1:
                    self.is_up = False
                    conter.append(conter_tuple(i, self.is_up))
                    if len(conter) > 1:
                        diff.append(abs(self.close[conter[-1].index] - self.close[conter[-2].index]))
            elif self.pivots[i] == -1:
                    self.is_up = True
                    conter.append(conter_tuple(i, self.is_up))
                    if len(conter) > 1:
                        diff.append(abs(self.close[conter[-1].index] - self.close[conter[-2].index]))
                    if len(diff) > 3:
                        if diff[-1] > diff[-2]/2 and diff[-1] > 10:
                            print(self.date[i], "N")
            # else:


        # print(conter)

        # for i in range(0, len(conter) - 1):
        #     diff.append(abs(self.close[conter[i+1].index] - self.close[conter[i].index]))
        #     print(self.date[conter[i].index], self.date[conter[i+1].index], conter[i].is_up ,diff[i])

        #     if i > 3 and conter[i].is_up is True:
        #         if diff[i] > diff[i-1]/2 and diff[i] > 10:
        #             print(self.date[conter[i+1].index], "N")
        #         print(diff[i], diff[i-1], diff[i-2], diff[i-3])

    def moving_average(self, data, days):
        result = []
        data = data[:]
        for _ in range(len(data) - days + 1):
            result.append(round(sum(data[-days:]) / days, 2))
            data.pop()
        return result[::-1]

    def ma_bias_ratio(self, day1, day2):
        """Calculate moving average bias ratio"""
        data1 = self.moving_average(self.close, day1)
        data2 = self.moving_average(self.close, day2)
        result = [data1[-i] - data2[-i] for i in range(1, min(len(data1), len(data2)) + 1)]

        return result[::-1]

    def ma_bias_ratio_pivot(self, data, sample_size=5, position=False):
        """Calculate pivot point"""
        sample = data[-sample_size:]

        if position is True:
            check_value = max(sample)
            pre_check_value = max(sample) > 0
        elif position is False:
            check_value = min(sample)
            pre_check_value = max(sample) < 0

        return ((sample_size - sample.index(check_value) < 4 and
                 sample.index(check_value) != sample_size - 1 and pre_check_value),
                sample_size - sample.index(check_value) - 1,
                check_value)

    def up_three_line(self):
        for i in range(2, self.length):
            # print(self.sid, self.date[-i+1], self.change[-i+1], self.ma5[-i], self.ma10[-i], self.ma20[-i], self.three_line_diff[-i])
            if self.volume[-i+1] > 1000:
                if self.change[-i+1] > 1.5 and self.close[-i+1] > self.open[-i+1] and self.close[-i+1] > self.close[-i]:
                    if self.close[-i+1] < 20:
                        if self.three_line_diff[-i] <= 0.03:
                            print(self.sid, self.date[-i+1], self.change[-i+1], self.ma5[-i], self.ma10[-i], self.ma20[-i], self.three_line_diff[-i])
                            return self.date[-i+1]
                    else:
                        if self.three_line_diff[-i] <= 0.02:
                            print(self.sid, self.date[-i+1], self.change[-i+1], self.ma5[-i], self.ma10[-i], self.ma20[-i], self.three_line_diff[-i])
                            return self.date[-i+1]

    def down_three_line(self):
        for i in range(2, self.length):
            # print(self.sid, self.date[-i+1], self.change[-i+1], self.ma5[-i], self.ma10[-i], self.ma20[-i], self.three_line_diff[-i])
            if self.change[-i+1] < -1.5 and self.close[-i+1] < self.open[-i+1] and self.close[-i+1] < self.close[-i]:
                if self.close[-i+1] < 20:
                    if self.three_line_diff[-i] <= 0.02:
                        print(self.sid, self.date[-i+1], self.change[-i+1], self.ma5[-i], self.ma10[-i], self.ma20[-i], self.three_line_diff[-i])
                        return self.date[-i+1]
                else:
                    if self.three_line_diff[-i] <= 0.01:
                        print(self.sid, self.date[-i+1], self.change[-i+1], self.ma5[-i], self.ma10[-i], self.ma20[-i], self.three_line_diff[-i])
                        return self.date[-i+1]

    def up_jump_line(self):
        for i in range(2, self.length):
            if self.volume[-i+1] > 300 and self.volume[-i+1] > self.volume[-i] * 2:
                if self.change[-i+1] > 3 and self.close[-i+1] > self.open[-i+1]:
                    if self.low[-i+1] > self.high[-i]:
                        return self.date[-i+1]

    def down_jump_line(self):
        for i in range(2, self.length):
                if self.change[-i+1] < -3 and self.close[-i+1] < self.open[-i+1]:
                    if self.high[-i+1] < self.low[-i]:
                        return self.date[-i+1]

    def up_macd(self):
        for i in range(2, self.length):
            if self.macdsignal[-i+1] > 0 and self.macd[-i+1] > 0:
                if self.change[-i+1] > 0 and self.close[-i+1] > self.open[-i+1]:
                    if self.macdsignal[-i+1] > self.macdsignal[-i] and self.macd[-i+1] > self.macd[-i]:
                        if self.macdsignal[-i+1] > 0 and self.macdsignal[-i] < 0:
                            return self.date[-i+1]

    def up_kd(self):
        for i in range(2, self.length):
            if self.k9[-i+1] < 20 and self.d9[-i+1] < 20:
                if self.change[-i+1] > 0:
                    if self.k9[-i+1] > self.k9[-i]:
                        if self.k9[-i+1] > self.d9[-i+1] and self.k9[-i] < self.d9[-i]:
                            return self.date[-i+1]

    def up_bollinger(self):
        if len(self.bollinger_upper) > 10:
            for i in range(2, self.length):
                bollinger_dif = self.bollinger_upper[-i]/self.bollinger_lower[-i] - 1
                if bollinger_dif < 0.07:
                    if self.volume[-i+1] > 300 and self.volume[-i+1] > self.volume[-i] * 2:
                        if self.close[-i+1] > self.open[-i+1]:
                            if self.close[-i+1] > self.bollinger_upper[-i+1]:
                                print(self.sid, self.date[-i+1], self.change[-i+1], self.bollinger_upper[-i+1], 'up_bollinger')
                                return self.date[-i+1]

    def long_up(self):
        for i in range(2, self.length):
            if self.change[-i+1] > 3:
                if self.close[-i+1] > self.open[-i+1] and self.close[-i] < self.open[-i]:
                    if self.close[-i] > self.open[-i+1] and self.open[-i] < self.close[-i+1]:
                        return self.date[-i+1]

    def long_down(self):
        for i in range(2, self.length):
            if self.change[-i+1] < -3:
                if self.close[-i+1] < self.open[-i+1] and self.close[-i] > self.open[-i]:
                    if self.close[-i] < self.open[-i+1] and self.open[-i] > self.close[-i+1]:
                        return self.date[-i+1]

    def long(self):
        for i in range(2, self.length):
            if (self.ma20[-i] <= self.ma10[-i] and self.ma10[-i] <= self.ma5[-i]):
                return self.date[-i+1]

    def short(self):
        for i in range(2, self.length):
            if (self.ma20[-i] >= self.ma10[-i] and self.ma10[-i] >= self.ma5[-i]):
                return self.date[-i+1]

    def up_session(self):
        for i in range(2, self.length):
            if self.close[-i] > self.ma60[-i]:
                return self.date[-i+1]

    def down_session(self):
        for i in range(2, self.length):
            if self.close[-i] < self.ma60[-i]:
                return self.date[-i+1]

    def up_cross_ma5_ma20(self):
        for i in range(2, self.length):
            if self.ma20[-i] < self.ma20[-i+1] and self.ma5[-i] < self.ma5[-i+1]:
                if self.ma5[-i] < self.ma20[-i] and self.ma5[-i+1] > self.ma20[-i+1]:
                    return self.date[-i+1]

    def down_cross_ma5_ma20(self):
        for i in range(2, self.length):
            if self.ma20[-i] > self.ma20[-i+1] and self.ma5[-i] > self.ma5[-i+1]:
                if self.ma5[-i] > self.ma20[-i] and self.ma5[-i+1] < self.ma20[-i+1]:
                    return self.date[-i+1]

    def up_dmi(self):
        for i in range(2, self.length):
            if self.plus_di[-i+1] > self.minus_di[-i+1]:
                if self.adx[-i+1] - self.adx[-i] > 2:
                    if self.plus_di[-i+1] - self.plus_di[-i] > 3:
                        return self.date[-i+1]
    
    def down_dmi(self):
        for i in range(2, self.length):
            if self.plus_di[-i+1] < self.minus_di[-i+1]:
                if self.adx[-i+1] - self.adx[-i] > 2:
                    if self.minus_di[-i+1] - self.minus_di[-i] > 3:
                        return self.date[-i+1]
    
    def continuous_trend_days(self, data):
        diff = [1 if data[-i] > 0 else -1 for i in range(1, len(data))]
        cont = 0
        for v in diff:
            if v == diff[0]:
                cont += 1
            else:
                break
        return cont * diff[0]

class BestFourPoint(object):
    BEST_BUY_WHY = ['量大收紅', '量縮價不跌', '三日均價由下往上', '三日均價大於六日均價']
    BEST_SELL_WHY = ['量大收黑', '量縮價跌', '三日均價由上往下', '三日均價小於六日均價']

    def __init__(self, stock):
        self.stock = stock

    def bias_ratio(self, position=False):
        return self.stock.ma_bias_ratio_pivot(
            self.stock.ma_bias_ratio(3, 6),
            position=position)

    def plus_bias_ratio(self):
        return self.bias_ratio(True)

    def mins_bias_ratio(self):
        return self.bias_ratio(False)

    def best_buy_1(self):
        return (self.stock.volume[-1] > self.stock.volume[-2] and
                self.stock.close[-1] > self.stock.open[-1])

    def best_buy_2(self):
        return (self.stock.volume[-1] < self.stock.volume[-2] and
                self.stock.close[-1] > self.stock.open[-2])

    def best_buy_3(self):
        return self.stock.continuous(self.stock.moving_average(self.stock.close, 3)) == 1

    def best_buy_4(self):
        return (self.stock.moving_average(self.stock.close, 3)[-1] >
                self.stock.moving_average(self.stock.close, 6)[-1])

    def best_sell_1(self):
        return (self.stock.volume[-1] > self.stock.volume[-2] and
                self.stock.close[-1] < self.stock.open[-1])

    def best_sell_2(self):
        return (self.stock.volume[-1] < self.stock.volume[-2] and
                self.stock.close[-1] < self.stock.open[-2])

    def best_sell_3(self):
        return self.stock.continuous(self.stock.moving_average(self.stock.close, 3)) == -1

    def best_sell_4(self):
        return (self.stock.moving_average(self.stock.close, 3)[-1] <
                self.stock.moving_average(self.stock.close, 6)[-1])

    def best_four_point_to_buy(self):
        result = []
        check = [self.best_buy_1(), self.best_buy_2(),
                 self.best_buy_3(), self.best_buy_4()]
        if self.mins_bias_ratio() and any(check):
            for index, v in enumerate(check):
                if v:
                    result.append(self.BEST_BUY_WHY[index])
        else:
            return False
        return ', '.join(result)

    def best_four_point_to_sell(self):
        result = []
        check = [self.best_sell_1(), self.best_sell_2(),
                 self.best_sell_3(), self.best_sell_4()]
        if self.plus_bias_ratio() and any(check):
            for index, v in enumerate(check):
                if v:
                    result.append(self.BEST_SELL_WHY[index])
        else:
            return False
        return ', '.join(result)

    def best_four_point(self):
        buy = self.best_four_point_to_buy()
        sell = self.best_four_point_to_sell()
        if buy:
            return (True, buy)
        elif sell:
            return (False, sell)

        return None
