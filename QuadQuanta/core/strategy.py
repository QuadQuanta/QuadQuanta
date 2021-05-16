#!/usr/bin/env python
# -*- encoding: utf-8 -*-
'''
@File    :   strategy.py
@Time    :   2021/05/14
@Author  :   levonwoo
@Version :   0.2
@Contact :   
@License :   (C)Copyright 2020-2021
@Desc    :   None
'''

# here put the import lib
import numpy as np

from QuadQuanta.data import query_clickhouse


class BaseStrategy():
    """
    策略基类
    """
    def __init__(self,
                 code=None,
                 start_date=None,
                 end_date=None,
                 frequency='day'):
        self.start_date = start_date
        self.end_date = end_date
        self.frequency = frequency
        # 初始化时加载日线数据
        self.day_data = query_clickhouse(code, start_date, end_date, 'day')
        self.subscribe_code = np.unique(self.day_data['code']).tolist()
        self.trading_date = np.sort(np.unique(self.day_data['date']))
        self.trading_datetime = np.sort(np.unique(self.day_data['datetime']))

    def init(self):
        """
        策略初始化
        """
        raise NotImplementedError

    def on_bar(self, bar):
        """
        
        """
        raise NotImplementedError

    def on_tick(self, tick):
        raise NotImplementedError

    def syn_backtest(self):
        for date in self.trading_date:
            if self.frequency in ['d', 'day']:
                self.subscribe_data = self.day_data[self.day_data['date'] ==
                                                    date]
            else:
                self.subscribe_data = query_clickhouse(self.subscribe_code,
                                                       str(date), str(date),
                                                       self.frequency)
            for sigle_bar in self.subscribe_data:
                self.on_bar(sigle_bar)

    # TODO
    async def asyn_backtest(self):
        """
        异步回测
        """

        import time

        for date in self.trading_date:
            _start = time.time()

            self.subscribe_data = query_clickhouse(self.subscribe_code,
                                                   str(date), str(date),
                                                   self.frequency)


if __name__ == '__main__':
    strategy = BaseStrategy(start_date='2014-01-01',
                            end_date='2015-01-10',
                            frequency='min')
    strategy.syn_backtest()
