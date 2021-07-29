#!/usr/bin/env python
# -*- encoding: utf-8 -*-
'''
@File    :   DoubleMA.py
@Time    :   2021/05/23
@Author  :   levonwoo
@Version :   0.2
@Contact :   
@License :   (C)Copyright 2020-2021
@Desc    :   双均线策略示例
'''

# here put the import lib
import numpy as np
import time
import matplotlib.pyplot as plt
import talib
from tqdm import tqdm

from QuadQuanta.core.strategy import BaseStrategy


class DoubleMA(BaseStrategy):
    def __init__(self,
                 code=None,
                 start_date=None,
                 end_date=None,
                 frequency='day'):
        super(DoubleMA, self).__init__(code, start_date, end_date, frequency)
        self.init()
        self.account = self.acc

    def init(self):
        self.short_period = 5
        self.long_period = 10
        self.period = self.long_period + 1

    def on_bar(self, bars):

        code = bars[-1]['code']
        # 当日收盘价
        price = bars[-1]['close']
        # 使用talib库计算
        short_avg = talib.SMA(bars['close'], self.short_period)
        long_avg = talib.SMA(bars['close'], self.long_period)
        # 短均线下穿长均线
        if long_avg[-2] < short_avg[-2] and long_avg[-1] >= short_avg[-1]:
            # 如果在在持仓中,全部卖出
            if code in self.account.positions.keys():
                volume_long_history = self.account.get_position(
                    code).volume_long_history
                if volume_long_history > 0:
                    order = self.account.send_order(code,
                                                    volume_long_history,
                                                    price,
                                                    'sell',
                                                    order_time=str(
                                                        bars['date'][-1]))
                    self.account.make_deal(order)

        # 短均线上穿长均线，买入
        if short_avg[-2] < long_avg[-2] and short_avg[-1] >= long_avg[-1]:
            volume_long_history = self.account.get_position(
                code).volume_long_history
            # 无持仓,则下单
            if volume_long_history == 0:
                # 仓位按照总资产的四分之一设置
                order_volume = 100 * (self.account.total_assets /
                                      (4 * price) // 100)
                order = self.account.send_order(code,
                                                order_volume,
                                                price,
                                                'buy',
                                                order_time=str(
                                                    bars[1]['datetime']))
                self.account.make_deal(order)

    def backtest(self):
        self.total_assert = []
        #
        for i in tqdm(range(self.period, len(self.trading_date) - 1)):

            # 当前交易日

            date = self.trading_date[i]
            period_date = self.trading_date[i- self.period]
            # 取时间周期内当日所有股票数据
            period_data = self.day_data[self.day_data['date'] <= date]
            period_data = period_data[period_data['date'] >= period_date]

            # 取日期内有数据的股票列表
            code_list = np.unique(period_data['code'])

            for code in code_list:
                try:
                    # 取单个股票时间序列
                    bars = period_data[period_data['code'] == code]
                    self.on_bar(bars)
                except Exception as e:
                    continue
            # 每日收盘结算
            self.account.settle()
            # 查看每日资产和收益率
            print(
                f"date:{date} asserts: {self.account.total_assets}, profit_ratio: {self.account.profit_ratio}"
            )
            self.total_assert.append(self.account.total_assets)
        # 回测结束总资产和收益率
        print(
            f"total_assets: {self.account.total_assets}, total_profit_ratio: {self.account.profit_ratio}"
        )
        plt.plot(self.total_assert)
        plt.show()


if __name__ == '__main__':
    stragety = DoubleMA(code=['000001','000002','600000','600004'],start_date='2020-01-01',
                        end_date='2021-01-20',
                        frequency='day')

    stragety.backtest()
