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

from QuadQuanta.data import query_clickhouse


class Strategy():
    """
    策略基类
    """
    def __init__(self,
                 code=None,
                 start_date=None,
                 end_date=None,
                 frequency='day'):
        all_day_data = query_clickhouse(code, start_date, end_date, 'day')

    def init(self):
        """
        策略初始化
        """
        pass

    def on_bar(self, bar):
        """
        
        """
        raise NotImplementedError

    def on_tick(self, tick):
        raise NotImplementedError

    def run_backtest(self):
        """
        运行回测
        """
        pass


if __name__ == '__main__':
    strategy = Strategy(start_date='2021-04-01',
                        end_date='2021-04-02',
                        frequency='d')
