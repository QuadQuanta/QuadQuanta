#!/usr/bin/env python
# -*- encoding: utf-8 -*-
'''
@File    :   position.py
@Time    :   2021/05/11
@Author  :   levonwoo
@Version :   0.1
@Contact :   
@License :   (C)Copyright 2020-2021
@Desc    :   股票持仓模型
'''

# here put the import lib
import uuid


class Position():
    def __init__(self,
                 code='000001',
                 volume_long_today=0,
                 volume_long_history=0,
                 position_cost=0,
                 volume_long_frozen=0,
                 positon_id=None):
        """
        持仓模型初始化
        """
        self.code = code
        self.position_id = str(
            uuid.uuid4()) if positon_id is None else positon_id  # 生成唯一持仓id

        self.volume_long_today = volume_long_today
        self.volume_long_history = volume_long_history
        self.volume_short_today = 0
        self.volume_short_history = 0
        self.position_cost = position_cost
        # 卖出冻结
        self.volume_short_frozen = 0
        self.frozen_cash = 0
        self.last_price = 0  # 持仓最新价格
        # 开仓总成本
        self.open_cost = position_cost

        self.datetime = ""

        # 持仓天数
        self.hold_days = 0

    def __repr__(self) -> str:
        return 'Positon: {} volume: {} avaliable:{} cost_price:{} mark_value:{} float_profit:{}'.format(
            self.code, self.volume_long, self.volume_long_history,
            self.cost_price, self.market_value, self.float_profit)

    @property
    def volume_long(self):
        """
        实际持仓
        """
        return self.volume_long_today + self.volume_long_history + self.volume_short_frozen

    @property
    def cost_price(self):
        """
        持仓成本价

        Returns
        -------
        [type]
            [description]
        """
        try:
            return round(self.position_cost / self.volume_long, 2)
        except ZeroDivisionError:
            return 0

    @property
    def float_profit(self):
        """
        浮动盈亏金额
        """
        return self.volume_long * self.last_price - self.position_cost

    @property
    def profit_ratio(self):
        """
        收益率

        Returns
        -------
        [type]
            [description]
        """
        return round(100 * (self.float_profit / self.open_cost), 2)

    @property
    def market_value(self):
        """
        市值
        """
        return self.volume_long * self.last_price

    def on_price_change(self, price):
        """
        更新价格

        Parameters
        ----------
        price : [type]
            [description]
        """
        self.last_price = price

    def settle(self):
        """
        收盘后结算
        """
        self.volume_long_history += self.volume_long_today
        self.volume_short_history += self.volume_short_today
        self.volume_long_today = 0
        self.volume_short_today = 0
        # 持仓天数
        if self.volume_long > 0:
            self.hold_days += 1
        else:
            self.hold_days = 0

    def update_pos(self, price, update_time):
        """
        股票更新仓位

        Parameters
        ----------
        price : [type]
            [description]
        update_time : [type]
            [description]
        """
        self.on_price_change(price)
        self.datetime = update_time
        # temp_cost = int(volume) * price

    @property
    def static_message(self):
        return {
            'code': self.code,
            'position_id': self.position_id,
            'last_updatetime': str(self.datetime),
            # 持仓字段
            'volume_long_today': int(self.volume_long_today),
            'volume_long_his': int(self.volume_long_history),
            'volume_long': int(self.volume_long),
            'cost_price': self.cost_price,
            'float_profit': self.float_profit,
            'profit_ratio': self.profit_ratio
        }
