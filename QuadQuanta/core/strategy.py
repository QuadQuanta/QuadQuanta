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
from QuadQuanta.data.clickhouse_api import query_clickhouse
from tqdm import tqdm


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
        self.init()

    def init(self):
        """
        策略初始化函数, 初始化回测账户,手续费
        """
        raise NotImplementedError

    def on_bar(self, bars):
        """
        
        """
        raise NotImplementedError

    def on_tick(self, tick):
        raise NotImplementedError

    def syn_backtest(self):
        """
        日线回测逻辑

        """
        for i in tqdm(range(0, len(self.trading_date))):
            date = self.trading_date[i]
            self.today_data = self.day_data[self.day_data['date'] == date]
            self.on_bar(self.today_data)
    # TODO
    async def asyn_backtest(self):
        """
        异步回测
        """
        raise NotImplementedError


if __name__ == '__main__':
    strategy = BaseStrategy(start_date='2014-01-01',
                            end_date='2014-01-10',
                            frequency='min')
    strategy.syn_backtest()
