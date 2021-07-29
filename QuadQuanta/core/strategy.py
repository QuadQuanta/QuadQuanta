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
from QuadQuanta.portfolio.account import Account
import numpy as np
from QuadQuanta.data.get_data import get_bars
from tqdm import tqdm


class BaseStrategy():
    """
    策略基类
    """
    def __init__(self,
                 code=None,
                 start_date=None,
                 end_date=None,
                 frequency='daily',
                 username='quadquanta',
                 passwd='quadquanta',
                 model='backtest',
                 init_cash=100000,
                 account_id=None,
                 mongo_db='QuadQuanta',
                 mongo_col='account',
                 solid=False):
        self.start_date = start_date
        self.end_date = end_date
        self.frequency = frequency
        # 初始化时加载日线数据
        self.day_data = get_bars(code, start_date, end_date, 'daily')
        self.subscribe_code = np.unique(self.day_data['code']).tolist()
        self.trading_date = np.sort(np.unique(self.day_data['date']))
        self.trading_datetime = np.sort(np.unique(self.day_data['datetime']))
        self.init()
        self.acc = Account(
            username,
            passwd,
            model,
            init_cash,
            account_id,
            mongo_db,
            mongo_col,
            solid,
        )

    def init(self):
        """
        策略初始化函数, 初始化回测账户,手续费
        """
        raise NotImplementedError

    def on_bar(self, bars):
        """
        策略函数
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


if __name__ == '__main__':
    strategy = BaseStrategy(code='000001',start_date='2014-01-01',
                            end_date='2014-01-10',
                            frequency='day')
    strategy.syn_backtest()
